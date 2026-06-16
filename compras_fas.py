"""
Compras de la exportación e industria en el mercado FAS argentino.

Agrega el eslabón que falta en la cadena analítica del dashboard:

    DECLARADO (DJVE) - COMPRADO (FAS) = FALTA COMPRAR -> presión compradora
    DECLARADO (DJVE) - EMBARCADO (line-up) = FALTA EMBARCAR -> presión logística

Cadena completa:
  1. DECLARADO   : venta externa comprometida (DJVE, Ley 21.453).
  2. COMPRADO    : grano comprado al productor en el mercado FAS interno.
                   Se registra en SIO-Granos y el MAGyP publica el acumulado
                   semanal por grano/campaña/sector (exportación/industria).
  3. EMBARCADO   : buques con ETB en la ventana (line-up ISA). Ya modelado en
                   `cobertura.py`.

Lecturas clave:
  - falta_comprar > 0 (declarado >> comprado): la expo está CORTA, debe salir
    a comprar grano al FAS -> presión compradora -> basis/FAS firme -> ALCISTA.
  - falta_comprar < 0 (comprado >> declarado): expo LARGA/cubierta, puede bajar
    el bid -> sesgo BAJISTA.
  - falta_embarcar > 0: presión logística/deadline físico (deadfreight riesgo).

Fuente de datos — compras MAGyP SIO-Granos:
  - Dataset CKAN: datos.magyp.gob.ar
    ID presunto: "compras-de-granos" o "compras-y-existencias-de-granos"
    (verificar contra CKAN api: datos.magyp.gob.ar/api/3/action/package_list)
  - El MAGyP publica semanalmente la planilla "Comercialización de Granos"
    (también reproducida por BCR en el Informativo Semanal) con columnas:
      grano, campaña, semana, comprado_exportacion_tn, comprado_industria_tn,
      total_comprado_tn, precio_promedio_usd_ton (no siempre disponible),
      porcentaje_cosecha_comercializada.
  - Frecuencia: semanal (cada miércoles/jueves).
  - Nota: los endpoints magyp.gob.ar devuelven 403 desde IPs cloud/sandbox.
    En producción local esto funciona sin restricciones.

Este modulo es PURO: recibe DataFrames, devuelve DataFrames. Sin red al importar.
"""
from __future__ import annotations

import logging
from datetime import date

import pandas as pd
import requests

import campanas
import config
from cobertura import _filtrar_djve_por_ventana, _filtrar_lineup_por_ventana, _ratio

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Mapeo grano (nomenclatura MAGyP en planilla de compras) → codigo_interno
# ---------------------------------------------------------------------------
# La planilla de comercialización usa nombres distintos a la DJVE y al line-up.
# Ej: "Girasol" en compras = "SFSEED" en el line-up.

GRANO_COMPRAS_A_CODIGO: dict[str, str] = {
    # Soja (el complejo se publica agregado como "Soja" en compras)
    "SOJA":               "SBS",
    "POROTO DE SOJA":     "SBS",
    # Maíz
    "MAIZ":               "MAIZE",
    "MAÍZ":               "MAIZE",
    # Trigo
    "TRIGO":              "WHEAT",
    "TRIGO PAN":          "WHEAT",
    # Cebada
    "CEBADA":             "BARLEY",
    "CEBADA FORRAJERA":   "BARLEY",
    "CEBADA CERVECERA":   "MALT",
    # Sorgo
    "SORGO":              "SORGHUM",
    # Girasol
    "GIRASOL":            "SFSEED",
    # Derivados (a veces aparecen en compras de industria)
    "HARINA DE SOJA":     "SBM",
    "ACEITE DE SOJA":     "SBO",
    "HARINA DE GIRASOL":  "SFMP",
    "ACEITE DE GIRASOL":  "SFO",
}

# Columnas mínimas que debe tener el DataFrame de compras para ser usable.
_COLUMNAS_REQUERIDAS = {"fecha", "codigo_interno", "sector", "toneladas"}

# URL base CKAN — resolver programáticamente o usar fallback.
_URL_CKAN_COMPRAS = (
    "https://datos.magyp.gob.ar/api/3/action/package_show"
    "?id=compras-de-granos"
)
# Fallback hardcodeado (verificar periódicamente si cambia el resource_id).
# NOTA: no se pudo verificar esta URL desde el entorno sandbox (403).
# Formato esperado: CSV con columnas grano, campaña, semana_inicio, sector,
# toneladas, porcentaje_cosecha.
_URL_CSV_FALLBACK_COMPRAS = (
    "https://datos.magyp.gob.ar/dataset/"
    "compras-de-granos/resource/compras-granos-latest.csv"
)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (LineUpDashboard/1.0; agro trading research; "
        "contact=personal use)"
    ),
}

# Sectores que identifican compras de la exportación vs industria.
SECTOR_EXPORTACION = "EXPORTACION"
SECTOR_INDUSTRIA = "INDUSTRIA"

# Señales de presión (coherentes con cobertura.py).
RATIO_COMPRA_CORTO = 0.80       # < 0.8: expo corta, presión compradora
RATIO_COMPRA_LARGO = 1.25       # > 1.25: expo larga, sesgo bajista
DECLARADO_MINIMO_TN = 5_000.0   # mínimo para emitir señal (evita ruido)

# Un "Panamax típico" ~65.000 tn — calibra intensidad de las señales.
PANAMAX_TN = 65_000.0


# ---------------------------------------------------------------------------
# 1. Descarga y parser defensivo
# ---------------------------------------------------------------------------

def _mapear_grano(nombre: str | None) -> str | None:
    """Mapea nombre crudo de grano (planilla MAGyP) al codigo_interno."""
    if not nombre:
        return None
    upper = str(nombre).strip().upper()
    if upper in GRANO_COMPRAS_A_CODIGO:
        return GRANO_COMPRAS_A_CODIGO[upper]
    for key, codigo in GRANO_COMPRAS_A_CODIGO.items():
        if key in upper:
            return codigo
    return None


def _resolver_url_csv(timeout: int = 15) -> str:
    """Intenta resolver la URL del CSV más reciente vía CKAN; fallback si falla."""
    try:
        resp = requests.get(_URL_CKAN_COMPRAS, headers=_HEADERS, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
        for resource in data.get("result", {}).get("resources", []):
            fmt = (resource.get("format") or "").upper()
            url = resource.get("url") or ""
            if fmt == "CSV" and url:
                return url
    except Exception as exc:
        logger.debug("CKAN compras no disponible: %s — usando fallback.", exc)
    return _URL_CSV_FALLBACK_COMPRAS


def descargar_compras(timeout: int = 60) -> pd.DataFrame:
    """
    Descarga las compras de exportación e industria (planilla SIO-Granos MAGyP).

    Intenta resolver la URL actual vía CKAN; si falla usa la URL fallback.
    Parser defensivo: si la descarga falla o el esquema no coincide, devuelve
    DataFrame vacío con las columnas correctas — nunca lanza excepción.

    Returns:
        DataFrame normalizado con columnas:
          fecha (date), grano_raw (str), codigo_interno (str|None),
          campana (str), sector (str — EXPORTACION/INDUSTRIA),
          toneladas (float).
        Y si están disponibles:
          toneladas_a_fijar (float), precio_promedio_usd (float),
          porcentaje_cosecha (float).
        DataFrame vacío (mismas columnas) si la descarga o el parseo fallan.
    """
    _columnas_vacias = pd.DataFrame(columns=[
        "fecha", "grano_raw", "codigo_interno", "campana",
        "sector", "toneladas",
    ])

    url = _resolver_url_csv(timeout=min(15, timeout))
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=timeout)
        resp.raise_for_status()
    except requests.exceptions.RequestException as exc:
        logger.warning("No se pudo descargar compras MAGyP: %s", exc)
        return _columnas_vacias

    try:
        df = pd.read_csv(pd.io.common.BytesIO(resp.content), encoding="utf-8",
                         on_bad_lines="skip")
    except Exception:
        try:
            df = pd.read_csv(pd.io.common.BytesIO(resp.content),
                             encoding="latin-1", on_bad_lines="skip")
        except Exception as exc:
            logger.warning("No se pudo parsear CSV de compras: %s", exc)
            return _columnas_vacias

    # Normalizar nombres de columna a snake_case.
    df.columns = [c.lower().strip().replace(" ", "_") for c in df.columns]

    # Intentar detectar las columnas clave con tolerancia a variantes de nombre.
    col_map: dict[str, str] = {}
    for col in df.columns:
        if any(k in col for k in ("grano", "cultivo", "producto")):
            col_map["grano_raw"] = col
        elif any(k in col for k in ("sector",)):
            col_map["sector"] = col
        elif any(k in col for k in ("campa", "zafra")):
            col_map["campana"] = col
        elif any(k in col for k in ("semana", "fecha", "periodo")):
            col_map["fecha"] = col
        elif col in ("tn", "toneladas", "comprado_tn", "total_tn"):
            col_map["toneladas"] = col

    if not {"grano_raw", "toneladas"}.issubset(col_map):
        logger.warning(
            "CSV de compras no tiene las columnas esperadas. "
            "Columnas encontradas: %s", list(df.columns)
        )
        return _columnas_vacias

    df = df.rename(columns={v: k for k, v in col_map.items()})

    # Normalizar tipos.
    df["toneladas"] = pd.to_numeric(df["toneladas"], errors="coerce").fillna(0)
    df["grano_raw"] = df["grano_raw"].astype(str).str.strip().str.upper()

    if "fecha" in df.columns:
        df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce").dt.date
    else:
        df["fecha"] = None

    if "sector" in df.columns:
        df["sector"] = df["sector"].astype(str).str.strip().str.upper()
    else:
        df["sector"] = SECTOR_EXPORTACION  # asumir exportación si no viene

    if "campana" not in df.columns:
        df["campana"] = None

    # Mapear a codigo_interno.
    df["codigo_interno"] = df["grano_raw"].apply(_mapear_grano)

    # Columnas opcionales.
    for col_opt in ("toneladas_a_fijar", "precio_promedio_usd", "porcentaje_cosecha"):
        candidates = [c for c in df.columns if col_opt.split("_")[0] in c]
        if candidates:
            df[col_opt] = pd.to_numeric(df[candidates[0]], errors="coerce")

    return df


# ---------------------------------------------------------------------------
# 2. Agregaciones sobre el DataFrame de compras
# ---------------------------------------------------------------------------

def compras_acumuladas_campana(
    df_compras: pd.DataFrame,
    codigo_interno: str,
    campana: str,
    sector: str | None = None,
) -> float:
    """
    Total comprado (tn) para un código de producto y campaña.

    Args:
        sector: "EXPORTACION", "INDUSTRIA" o None (ambos).
    Returns:
        Toneladas acumuladas. 0.0 si el DataFrame está vacío o sin datos.
    """
    if df_compras.empty or "codigo_interno" not in df_compras.columns:
        return 0.0
    mask = (df_compras["codigo_interno"] == codigo_interno)
    if "campana" in df_compras.columns and campana:
        mask &= (df_compras["campana"] == campana)
    if sector and "sector" in df_compras.columns:
        mask &= (df_compras["sector"] == sector.upper())
    sub = df_compras[mask]
    return float(sub["toneladas"].sum()) if not sub.empty else 0.0


def porcentaje_cosecha_comercializado(
    df_compras: pd.DataFrame,
    codigo_interno: str,
    campana: str,
) -> float | None:
    """
    Último % de cosecha comercializado publicado para el producto/campaña.
    Devuelve None si la columna no existe o no hay datos.
    """
    if df_compras.empty or "porcentaje_cosecha" not in df_compras.columns:
        return None
    mask = (df_compras["codigo_interno"] == codigo_interno)
    if "campana" in df_compras.columns and campana:
        mask &= (df_compras["campana"] == campana)
    sub = df_compras[mask].dropna(subset=["porcentaje_cosecha"])
    if sub.empty:
        return None
    # Si hay fecha, tomamos el más reciente.
    if "fecha" in sub.columns and sub["fecha"].notna().any():
        sub = sub.sort_values("fecha", ascending=False)
    return float(sub["porcentaje_cosecha"].iloc[0])


# ---------------------------------------------------------------------------
# 3. Posición exportadora completa: declarado vs comprado vs embarcado
# ---------------------------------------------------------------------------

def posicion_exportadora(
    df_djve: pd.DataFrame,
    df_compras: pd.DataFrame,
    df_lineup: pd.DataFrame,
    fecha_ref: date,
    horizonte_dias: int = 60,
) -> pd.DataFrame:
    """
    Posición completa de la exportación por producto para el horizonte dado.

    Columnas del resultado:
      codigo_interno, producto_display,
      declarado_tn    — DJVE con ventana de embarque en el horizonte,
      comprado_tn     — compras acumuladas de la campaña en curso (exportación),
      embarcado_tn    — line-up con ETB en el horizonte,
      falta_comprar_tn = declarado_tn − comprado_tn,
      falta_embarcar_tn = declarado_tn − embarcado_tn,
      ratio_compra    = comprado_tn / declarado_tn,
      ratio_embarque  = embarcado_tn / declarado_tn,
      campana_actual  — campaña vigente para cada producto.

    Degradación: si df_compras viene vacío, las columnas de compra quedan en
    NaN y el resto (declarado, embarcado) se calcula normalmente.
    Si df_lineup viene vacío, columnas de embarque en 0.
    Si df_djve viene vacío, devuelve DataFrame vacío.

    Args:
        df_djve:    DataFrame de fob_djve (con columna codigo_interno).
        df_compras: DataFrame de descargar_compras(). Puede estar vacío.
        df_lineup:  DataFrame de db.query_exports_prioritarios(). Puede estar vacío.
        fecha_ref:  Fecha de referencia (hoy).
        horizonte_dias: Días hacia adelante para filtrar DJVE y line-up.
    """
    _cols_vacias = [
        "codigo_interno", "producto_display",
        "declarado_tn", "comprado_tn", "embarcado_tn",
        "falta_comprar_tn", "falta_embarcar_tn",
        "ratio_compra", "ratio_embarque", "campana_actual",
    ]
    if df_djve.empty:
        return pd.DataFrame(columns=_cols_vacias)

    # --- DECLARADO: DJVE filtrada por ventana de horizonte ---
    djve_h = _filtrar_djve_por_ventana(df_djve, fecha_ref, horizonte_dias)
    if djve_h.empty:
        decl = pd.DataFrame(columns=["codigo_interno", "declarado_tn"])
    else:
        decl = (
            djve_h.groupby("codigo_interno")["toneladas"]
            .sum()
            .reset_index()
            .rename(columns={"toneladas": "declarado_tn"})
        )

    # --- EMBARCADO: line-up filtrado por ETB en horizonte ---
    lineup_h = _filtrar_lineup_por_ventana(df_lineup, fecha_ref, horizonte_dias)
    if lineup_h.empty:
        emb = pd.DataFrame(columns=["codigo_interno", "embarcado_tn"])
    else:
        tmp = lineup_h.copy()
        tmp["quantity"] = pd.to_numeric(tmp["quantity"], errors="coerce").fillna(0)
        emb = (
            tmp.groupby("cargo")["quantity"]
            .sum()
            .reset_index()
            .rename(columns={"cargo": "codigo_interno", "quantity": "embarcado_tn"})
        )

    # --- Merge declarado + embarcado ---
    df = decl.merge(emb, on="codigo_interno", how="outer")
    for col in ("declarado_tn", "embarcado_tn"):
        df[col] = df[col].fillna(0.0)

    df["falta_embarcar_tn"] = df["declarado_tn"] - df["embarcado_tn"]
    df["ratio_embarque"] = df.apply(
        lambda r: _ratio(r["embarcado_tn"], r["declarado_tn"]), axis=1
    )

    # --- COMPRADO: compras de la exportación por campaña ---
    compras_disponibles = (
        not df_compras.empty
        and "codigo_interno" in df_compras.columns
        and "toneladas" in df_compras.columns
    )

    def _comprado(codigo: str) -> float | None:
        if not compras_disponibles:
            return None
        camp = campanas.campana_de(codigo, fecha_ref)
        return compras_acumuladas_campana(df_compras, codigo, camp,
                                          sector=SECTOR_EXPORTACION)

    df["comprado_tn"] = df["codigo_interno"].apply(_comprado)
    df["falta_comprar_tn"] = df.apply(
        lambda r: r["declarado_tn"] - r["comprado_tn"]
        if r["comprado_tn"] is not None else None,
        axis=1,
    )
    df["ratio_compra"] = df.apply(
        lambda r: _ratio(r["comprado_tn"], r["declarado_tn"])
        if r["comprado_tn"] is not None else float("nan"),
        axis=1,
    )

    # --- Enriquecer con display y campaña ---
    _ci = df["codigo_interno"].astype(str)
    df["producto_display"] = _ci.map(config.PRODUCTO_DISPLAY).fillna(_ci)
    df["campana_actual"] = df["codigo_interno"].apply(
        lambda c: campanas.campana_de(c, fecha_ref)
    )

    return df[_cols_vacias].sort_values("declarado_tn", ascending=False).reset_index(drop=True)


# ---------------------------------------------------------------------------
# 4. Señales de presión compradora / vendedora
# ---------------------------------------------------------------------------

def senales_presion(posicion: pd.DataFrame) -> pd.DataFrame:
    """
    Traduce la posición exportadora en señales accionables.

    Reglas:
      - ratio_compra < 0.80 y declarado significativo → PRESION COMPRADORA
        (expo corta, debe salir a comprar al FAS → alcista basis/FAS).
        Intensidad 1-5 según magnitud del faltante en múltiplos de Panamax.
      - ratio_compra > 1.25 → PRESION VENDEDORA
        (expo larga/cubierta, puede bajar el bid).
      - Sin datos de compras (ratio_compra = NaN) pero ratio_embarque < 0.70
        → CORTO_EMBARQUE (proxy de posición corta cuando no hay dato FAS).
      - Si no hay condición → NEUTRAL.

    Coherente en estructura con `cobertura.senales_trading`.

    Returns:
        DataFrame: codigo_interno, producto_display, senal, intensidad (1-5),
        racional.
    """
    filas = []
    for _, row in posicion.iterrows():
        codigo = row["codigo_interno"]
        display = row["producto_display"]
        declarado = row.get("declarado_tn") or 0.0
        comprado = row.get("comprado_tn")
        falta_comprar = row.get("falta_comprar_tn")
        ratio_c = row.get("ratio_compra")
        ratio_e = row.get("ratio_embarque")
        falta_emb = row.get("falta_embarcar_tn") or 0.0

        senal = "NEUTRAL"
        intensidad = 0
        racional = "Posición equilibrada."

        tiene_ratio_compra = (
            comprado is not None
            and not (isinstance(ratio_c, float) and (ratio_c != ratio_c))  # NaN check
        )

        if tiene_ratio_compra and declarado >= DECLARADO_MINIMO_TN:
            if ratio_c < RATIO_COMPRA_CORTO:
                senal = "PRESION COMPRADORA"
                panamax_faltantes = max(1, round(falta_comprar / PANAMAX_TN))
                intensidad = min(5, max(1, panamax_faltantes))
                racional = (
                    f"Expo declaró {declarado/1e6:.2f} Mt pero compró "
                    f"{comprado/1e6:.2f} Mt ({ratio_c:.0%}). "
                    f"Faltan ~{falta_comprar/1e3:.0f} kt ≈ {panamax_faltantes} "
                    f"Panamax. Debe comprar al FAS → bid firme."
                )
            elif ratio_c > RATIO_COMPRA_LARGO:
                senal = "PRESION VENDEDORA"
                intensidad = min(5, max(1, round((ratio_c - 1) * 5)))
                racional = (
                    f"Expo compró {comprado/1e6:.2f} Mt vs {declarado/1e6:.2f} Mt "
                    f"declaradas (ratio {ratio_c:.2f}). Posición larga: "
                    f"puede bajar el bid sin riesgo de incumplimiento."
                )
        elif not tiene_ratio_compra and declarado >= DECLARADO_MINIMO_TN:
            # Sin datos de compras: usar ratio de embarque como proxy.
            if (isinstance(ratio_e, float) and not (ratio_e != ratio_e)
                    and ratio_e < 0.70):
                senal = "CORTO_EMBARQUE"
                panamax = max(1, round(falta_emb / PANAMAX_TN))
                intensidad = min(5, max(1, panamax))
                racional = (
                    f"Sin dato de compras FAS. Ratio embarque {ratio_e:.0%}: "
                    f"faltan ~{falta_emb/1e3:.0f} kt en line-up vs DJVE. "
                    f"Proxy de posición corta — confirmar con compras BCR."
                )

        if senal != "NEUTRAL":
            filas.append({
                "codigo_interno": codigo,
                "producto_display": display,
                "senal": senal,
                "intensidad": intensidad,
                "racional": racional,
            })

    if not filas:
        return pd.DataFrame(
            columns=["codigo_interno", "producto_display",
                     "senal", "intensidad", "racional"]
        )
    return (
        pd.DataFrame(filas)
        .sort_values(["intensidad", "senal"], ascending=[False, True])
        .reset_index(drop=True)
    )


# ---------------------------------------------------------------------------
# 5. Self-test (solo como script directo)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    from utils import setup_logging
    setup_logging(__name__)

    print("=== COMPRAS FAS — SELF-TEST ===")
    print("Intentando descargar compras MAGyP...")
    df = descargar_compras(timeout=20)
    if df.empty:
        print("  Descarga falló (403 esperado desde sandbox/cloud).")
        print("  El módulo necesita correrse desde una IP sin restricciones.")
        print("  Estructura esperada del CSV:")
        print("    grano, campaña, semana_inicio, sector, toneladas, %_cosecha")
    else:
        print(f"  Descarga OK: {df.shape[0]} filas, {df.shape[1]} columnas")
        print(f"  Columnas: {list(df.columns)}")
        print(f"  Granos: {df['grano_raw'].value_counts().head(5).to_dict()}")
        print(f"  Sectores: {df['sector'].value_counts().to_dict()}")

    print("\nPosicion exportadora (DataFrames vacíos — modo degradado):")
    from datetime import date as _date
    pos = posicion_exportadora(
        pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
        fecha_ref=_date.today(),
    )
    print(f"  Resultado vacío esperado: {pos.empty}")

    print("\nSelf-test OK.")
    sys.exit(0)
