"""
Estimaciones agricolas oficiales (MAGyP + links a BCBA/BCR).

Provee contexto macro de cuanto se sembro/cosecho/produjo historicamente
por cultivo en Argentina. El trader lo usa para dimensionar el line-up:
"si la campana 2024/25 de maiz produjo 52 M tn y cayo a 48 M, el line-up
se va a ver mas flojo en los proximos meses".

Fuente primaria: MAGyP - dataset "Estimaciones Agricolas"
    https://datos.magyp.gob.ar/dataset/estimaciones-agricolas

Resource ID estable del CSV (lo resuelve CKAN):
    https://datos.magyp.gob.ar/api/3/action/package_show?id=estimaciones-agricolas

Granularidad: departamento x campana, desde 1969/1970 hasta la ultima
campana cerrada (hoy: 2024/2025). No incluye estimaciones de campana en
curso; para eso se consulta manualmente el PAS (BCBA) o el Relevamiento
Semanal (BCR) cuyos links se devuelven en este modulo.

Fuentes secundarias (scraping bloqueado por WAF, solo links manuales):
- BCBA PAS (semanal):  https://www.bolsadecereales.com/estimaciones-agricolas
- BCR GEA (semanal):   https://www.bcr.com.ar/es/mercados/investigacion-y-desarrollo/informativo-semanal
- USDA PSD Online:     https://apps.fas.usda.gov/psdonline/app/index.html
"""
from __future__ import annotations

import io
from typing import Any

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# URLs y constantes
# ---------------------------------------------------------------------------

# CKAN API: devuelve JSON con los resources del dataset. El campo
# result.resources[0].url tiene el CSV con el sufijo de fecha mas reciente.
URL_CKAN = (
    "https://datos.magyp.gob.ar/api/3/action/package_show"
    "?id=estimaciones-agricolas"
)

# Fallback si CKAN falla: URL directa del CSV al momento de la codificacion
# (marzo 2026). El resource_id 95d066e6-... es estable, solo cambia el
# sufijo de fecha cada ~6 meses.
URL_CSV_FALLBACK = (
    "https://datos.magyp.gob.ar/dataset/"
    "9e1e77ba-267e-4eaa-a59f-3296e86b5f36/resource/"
    "95d066e6-8a0f-4a80-b59d-6f28f88eacd5/download/"
    "estimaciones-agricolas-2026-03.csv"
)

LINK_BCBA_PAS = "https://www.bolsadecereales.com/estimaciones-agricolas"
LINK_BCR_GEA = (
    "https://www.bcr.com.ar/es/mercados/investigacion-y-desarrollo/"
    "informativo-semanal"
)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (LineUpDashboard/1.0; agro trading research; "
        "contact=personal use)"
    ),
}

# ---------------------------------------------------------------------------
# Mapeo de cultivos MAGyP a codigos internos del line-up
# ---------------------------------------------------------------------------
# MAGyP usa minusculas con acentos. Los agrupamos para mostrar totales
# razonables. "soja 1ra" + "soja 2da" = "soja total" (pero MAGyP ya lo
# reporta agregado en "soja total").

CULTIVO_MAGYP_A_CODIGO: dict[str, str] = {
    "ma\u00edz":             "MAIZE",       # maiz
    "sorgo":                 "SORGHUM",
    "soja total":            "SBS",
    "trigo total":           "WHEAT",
    "cebada total":          "BARLEY",      # post-2015 (incluye cervecera+forrajera)
    "cebada cervecera":      "MALT",        # solo hasta 2015; legacy
    "girasol":               "SFSEED",
}

# Cultivos que NO sumamos al agregado para evitar doble conteo.
# MAGyP reporta "soja total" = 1ra + 2da; "trigo total" incluye candeal;
# "cebada total" = cervecera + forrajera (post-2015).
_CULTIVOS_EXCLUIR = {
    "soja 1ra", "soja 2da", "trigo candeal", "cebada forrajera",
}


# ---------------------------------------------------------------------------
# Resolucion de URL actual
# ---------------------------------------------------------------------------

def _resolver_url_csv(timeout: int = 30) -> str:
    """
    Consulta CKAN para obtener la URL del CSV mas reciente.
    Si CKAN falla, devuelve la URL fallback hardcodeada.

    Esto evita hardcodear la fecha en el codigo cuando MAGyP publique
    un nuevo snapshot (pasa cada ~6 meses).
    """
    try:
        resp = requests.get(URL_CKAN, headers=_HEADERS, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
        resources = data.get("result", {}).get("resources", [])
        for r in resources:
            fmt = (r.get("format") or "").upper()
            url = r.get("url") or ""
            if fmt == "CSV" and "estimaciones-agricolas" in url.lower():
                return url
    except Exception:
        pass
    return URL_CSV_FALLBACK


# ---------------------------------------------------------------------------
# Descarga y parseo
# ---------------------------------------------------------------------------

def descargar_estimaciones_magyp(timeout: int = 120) -> pd.DataFrame:
    """
    Descarga el CSV completo de estimaciones del MAGyP y devuelve un
    DataFrame normalizado.

    El archivo pesa ~15 MB (160k filas, 40 cultivos, desde 1969).

    Returns:
        DataFrame con columnas:
            cultivo, anio, campania, provincia, departamento,
            superficie_sembrada_ha, superficie_cosechada_ha,
            produccion_tm, rendimiento_kgxha, codigo_interno.

        DataFrame vacio si la descarga fallo.
    """
    url = _resolver_url_csv(timeout=min(30, timeout))
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=timeout)
        resp.raise_for_status()
    except requests.exceptions.RequestException:
        return pd.DataFrame()

    try:
        df = pd.read_csv(
            io.BytesIO(resp.content),
            encoding="utf-8",
            dtype={"provincia_id": str, "departamento_id": str},
        )
    except Exception:
        return pd.DataFrame()

    # Verificar columnas esperadas.
    requeridas = {
        "cultivo", "campania", "superficie_sembrada_ha",
        "superficie_cosechada_ha", "produccion_tm", "rendimiento_kgxha",
    }
    if not requeridas.issubset(df.columns):
        return pd.DataFrame()

    # Normalizar tipos numericos.
    for col in ("superficie_sembrada_ha", "superficie_cosechada_ha",
                "produccion_tm", "rendimiento_kgxha"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Agregar codigo interno para cruce con line-up.
    # Filtrar cultivos no relevantes y evitar doble conteo de soja.
    df = df[~df["cultivo"].isin(_CULTIVOS_EXCLUIR)].copy()
    df["codigo_interno"] = df["cultivo"].map(CULTIVO_MAGYP_A_CODIGO)

    return df


# ---------------------------------------------------------------------------
# Agregaciones usadas por el dashboard
# ---------------------------------------------------------------------------

def totales_nacionales_por_campania(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega por cultivo + campania a nivel nacional (suma provincias).
    Solo incluye cultivos mapeados al line-up.

    Returns:
        DataFrame: codigo_interno, cultivo, campania, sembrada_ha,
        cosechada_ha, produccion_tm, rinde_kgxha (recalculado sobre el
        total nacional).
    """
    if df.empty:
        return pd.DataFrame()

    sub = df[df["codigo_interno"].notna()].copy()
    if sub.empty:
        return pd.DataFrame()

    agg = (
        sub.groupby(["codigo_interno", "cultivo", "campania"])
        .agg(
            sembrada_ha=("superficie_sembrada_ha", "sum"),
            cosechada_ha=("superficie_cosechada_ha", "sum"),
            produccion_tm=("produccion_tm", "sum"),
        )
        .reset_index()
    )

    # Recalcular rinde nacional (kg/ha) = produccion_tm * 1000 / cosechada_ha.
    agg["rinde_kgxha"] = (
        agg["produccion_tm"] * 1000 / agg["cosechada_ha"].replace(0, 1)
    ).round(0).astype(int)

    # Ordenar campania mas reciente primero.
    agg = agg.sort_values(["codigo_interno", "campania"], ascending=[True, False])
    return agg.reset_index(drop=True)


def ultima_campania_por_cultivo(
    df_totales: pd.DataFrame,
    codigo: str,
    n: int = 5,
) -> pd.DataFrame:
    """
    Devuelve las ultimas N campanias cerradas para un cultivo.
    Util para mostrar tendencia reciente en el dashboard.
    """
    if df_totales.empty:
        return pd.DataFrame()

    sub = df_totales[df_totales["codigo_interno"] == codigo].copy()
    if sub.empty:
        return pd.DataFrame()

    # sub ya viene ordenado por campania desc.
    return sub.head(n).reset_index(drop=True)


def variacion_vs_campania_anterior(df_ult: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega columna pct_vs_anterior calculando la variacion de produccion
    respecto a la campania previa. Util para mostrar "vs anterior" en el
    dashboard (mas alta o mas baja).
    """
    if df_ult.empty:
        return df_ult

    # Asumo que viene ordenado desc por campania (mas reciente arriba).
    df = df_ult.copy().sort_values("campania", ascending=True).reset_index(drop=True)
    df["produccion_anterior"] = df["produccion_tm"].shift(1)
    df["pct_vs_anterior"] = (
        (df["produccion_tm"] - df["produccion_anterior"])
        / df["produccion_anterior"].replace(0, 1) * 100
    ).round(1)
    return df.sort_values("campania", ascending=False).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Links manuales a reportes externos (no scrapeables)
# ---------------------------------------------------------------------------

def links_reportes_semanales() -> dict[str, dict[str, str]]:
    """
    Devuelve los links a los reportes semanales que no podemos scrapear
    (BCBA bloquea con Cloudflare, BCR publica solo PDF).

    Uso en dashboard: mostrar cards clickeables que abren en nueva pestana.
    """
    return {
        "BCBA PAS": {
            "nombre": "Panorama Agricola Semanal (BCBA)",
            "url": LINK_BCBA_PAS,
            "frecuencia": "Semanal (jueves)",
            "cubre": "Estimaciones en curso, avance siembra/cosecha, estado cultivos",
        },
        "BCR GEA": {
            "nombre": "Informativo Semanal (BCR/GEA)",
            "url": LINK_BCR_GEA,
            "frecuencia": "Semanal (viernes)",
            "cubre": "Estimaciones de produccion, precios FAS, retenciones",
        },
    }


# ---------------------------------------------------------------------------
# Test manual
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    print("Resolviendo URL CSV via CKAN...")
    url = _resolver_url_csv()
    print(f"  URL: {url}\n")

    print("Descargando CSV estimaciones MAGyP (puede tardar 30s)...")
    df = descargar_estimaciones_magyp()
    print(f"  Filas totales: {len(df):,}")
    if df.empty:
        print("  ERROR: DataFrame vacio")
        sys.exit(1)

    print(f"  Columnas: {list(df.columns)}\n")

    print("Cultivos mapeados al line-up:")
    mapeados = df[df["codigo_interno"].notna()]
    for cultivo in sorted(mapeados["cultivo"].unique()):
        codigo = CULTIVO_MAGYP_A_CODIGO.get(cultivo, "?")
        print(f"  {cultivo:25} -> {codigo}")
    print()

    print("Totales nacionales por campania:")
    totales = totales_nacionales_por_campania(df)
    print(f"  Filas agregadas: {len(totales)}\n")

    print("Ultimas 5 campanias por cultivo (produccion_tm):")
    for codigo in ["MAIZE", "SBS", "WHEAT", "BARLEY", "SORGHUM", "SFSEED"]:
        print(f"\n  {codigo}:")
        ult = ultima_campania_por_cultivo(totales, codigo, n=5)
        ult = variacion_vs_campania_anterior(ult)
        for _, row in ult.iterrows():
            prod_mt = row["produccion_tm"] / 1_000_000
            pct = row.get("pct_vs_anterior")
            pct_str = f"{pct:+.1f}%" if pd.notna(pct) else "   -  "
            print(
                f"    {row['campania']:10} {prod_mt:6.2f} Mt "
                f"(rinde {int(row['rinde_kgxha']):5,} kg/ha) {pct_str}"
            )

    print("\nLinks manuales a reportes semanales:")
    for k, v in links_reportes_semanales().items():
        print(f"  {k}: {v['url']}")
