"""
Descarga y parseo de DJVE (Declaraciones Juradas de Ventas al Exterior) del MAGyP.

Las DJVE son el registro oficial de ventas de granos/oleaginosas/subproductos
argentinos al exterior (Ley 21.453). Cada exportador presenta una DJVE por
venta declarando producto, toneladas, ventana de embarque y razon social.

Complementa el line-up portuario: mientras el line-up muestra "buques
arribando a cargar", las DJVE muestran "ventas ya comprometidas". Un pico
en DJVE anticipa actividad portuaria en las proximas semanas.

Fuente oficial:
    https://www.magyp.gob.ar/sitio/areas/ss_mercados_agropecuarios/djve/

Formato: XLSX estatico, URL fija por ano. Se actualiza en dias habiles
(archivo "actual_aprobadas" pisa dentro del dia; el acumulado crece).

Funcion principal:
    descargar_djve_acumuladas(anio) -> DataFrame con columnas normalizadas:
        nro_djve, fecha_registro, fecha_presentacion, producto, toneladas,
        fecha_inicio_embarque, fecha_fin_embarque, opcion, razon_social.

NOTA: el XLSX no tiene columna de destino ni valor FOB USD. Solo TN.
Si en el futuro el MAGyP agrega esos campos, extender _ESQUEMA_COLUMNAS.
"""
from __future__ import annotations

import io
import logging
import time
from datetime import date, datetime
from typing import Any

import pandas as pd
import requests

# Reintentos para mitigar 403/timeouts puntuales de MAGyP. Si MAGyP cae,
# preferimos devolver DataFrame vacio (downstream lo tolera) en lugar de
# cortar el update diario.
_MAX_RETRIES = 2
_RETRY_BACKOFF = 3  # segundos entre reintentos

_logger = logging.getLogger(__name__)

# URL base (solo cambia el ano al final del nombre).
BASE_URL_DJVE = (
    "https://www.magyp.gob.ar/sitio/areas/ss_mercados_agropecuarios/djve/"
    "_archivos/_archivos_djve/DJVE-Granos-Acumuladas-Aprobadas-{anio}.xlsx"
)

# URL del archivo "actual_aprobadas" (delta del dia, ~15 KB).
# Util para refresh intra-day sin bajar el acumulado completo.
URL_DJVE_ACTUAL = (
    "https://www.magyp.gob.ar/sitio/areas/ss_mercados_agropecuarios/djve/"
    "_archivos/_archivos_djve/DJVE-actual_aprobadas.xlsx"
)

# Headers requeridos (MAGyP a veces devuelve 403 sin User-Agent).
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (LineUpDashboard/1.0; agro trading research; "
        "contact=personal use)"
    ),
}

# Mapeo de columnas del XLSX a nombres snake_case usables en pandas.
_ESQUEMA_COLUMNAS: dict[str, str] = {
    "Nº DJVE SIM": "nro_djve",
    "FECHA DE REGISTRO": "fecha_registro",
    "FECHA DE\nPRESENTACIÓN": "fecha_presentacion",
    "PRODUCTO": "producto",
    "TN": "toneladas",
    "FECHA  DE\nINICIO PER.": "fecha_inicio_embarque",
    "FECHA DE\nFIN PER.": "fecha_fin_embarque",
    "OPCION": "opcion",
    "RAZON SOCIAL": "razon_social",
}


# ---------------------------------------------------------------------------
# Mapeo de productos DJVE (string libre) a codigos internos del line-up
# ---------------------------------------------------------------------------
# El line-up usa codigos tipo "MAIZE" / "SBS" / "SBM". Las DJVE usan nombres
# libres en espanol ("MAIZ", "SOJA", "HARINA DE SOJA"). Este mapa los junta
# para poder cruzar ambos datasets en el dashboard.

PRODUCTO_DJVE_A_CODIGO: dict[str, str] = {
    # Maiz y sorgo
    "MAIZ":                       "MAIZE",
    "SORGO":                      "SORGHUM",
    # Complejo soja
    "SOJA":                       "SBS",
    "POROTO DE SOJA":             "SBS",
    "HARINA DE SOJA":             "SBM",
    "PELLETS DE SOJA":            "SBM",
    "SUBPRODUCTOS DE SOJA":       "SBM",  # mayormente harina + cascaras
    "ACEITE DE SOJA":             "SBO",
    # Trigo y derivados
    "TRIGO":                      "WHEAT",
    "TRIGO PAN":                  "WHEAT",
    "HARINA DE TRIGO":            "WBP",
    # Cebada y malta
    "CEBADA":                     "BARLEY",
    "CEBADA FORRAJERA":           "BARLEY",
    "CEBADA CERVECERA":           "MALT",
    "MALTA":                      "MALT",
    # Girasol
    "GIRASOL":                    "SFSEED",
    "GIRASOL CONFITERIA":         "SFSEED",
    "ACEITE DE GIRASOL":          "SFO",
    "HARINA DE GIRASOL":          "SFMP",
    "PELLETS DE GIRASOL":         "SFMP",
    "SUBPRODUCTOS DE GIRASOL":    "SFMP",
}


def _producto_a_codigo(nombre_djve: str | None) -> str | None:
    """Mapea el nombre DJVE crudo al codigo del line-up. None si no esta mapeado."""
    if not nombre_djve:
        return None
    upper = nombre_djve.upper().strip()
    # Match exacto primero.
    if upper in PRODUCTO_DJVE_A_CODIGO:
        return PRODUCTO_DJVE_A_CODIGO[upper]
    # Match por substring (cubre variantes de redaccion).
    for nombre_std, codigo in PRODUCTO_DJVE_A_CODIGO.items():
        if nombre_std in upper:
            return codigo
    return None


# ---------------------------------------------------------------------------
# Descarga y parseo
# ---------------------------------------------------------------------------

def descargar_djve_acumuladas(
    anio: int | None = None,
    timeout: int = 60,
) -> pd.DataFrame:
    """
    Descarga el acumulado DJVE aprobadas del ano especificado y devuelve
    un DataFrame con columnas normalizadas.

    Args:
        anio: ano a descargar. Default: ano actual.
        timeout: segundos de timeout para la request HTTP.

    Returns:
        DataFrame con columnas: nro_djve, fecha_registro, fecha_presentacion,
        producto, toneladas, fecha_inicio_embarque, fecha_fin_embarque,
        opcion, razon_social, codigo_interno (producto mapeado al line-up).

        DataFrame vacio si la descarga fallo o el archivo tiene formato
        inesperado.
    """
    if anio is None:
        anio = date.today().year

    url = BASE_URL_DJVE.format(anio=anio)
    contenido = _bajar_xlsx_con_retry(url, timeout=timeout, descripcion=f"DJVE {anio}")
    if contenido is None:
        return pd.DataFrame()
    return _parsear_xlsx(contenido)


def descargar_djve_actual(timeout: int = 30) -> pd.DataFrame:
    """
    Descarga el XLSX "actual_aprobadas" (ultimas DJVE aprobadas en el dia).
    Mas rapido que el acumulado; util para refresh intra-day.
    """
    contenido = _bajar_xlsx_con_retry(URL_DJVE_ACTUAL, timeout=timeout,
                                      descripcion="DJVE actual")
    if contenido is None:
        return pd.DataFrame()
    return _parsear_xlsx(contenido)


def _bajar_xlsx_con_retry(url: str, timeout: int, descripcion: str) -> bytes | None:
    """
    Descarga un XLSX de MAGyP con reintentos. Devuelve los bytes o None si
    fallo definitivamente. MAGyP a veces devuelve 403 transitorio o se queda
    colgado; un reintento simple lo soluciona en la mayoria de los casos.
    """
    ultimo_error: Exception | None = None
    for intento in range(1, _MAX_RETRIES + 2):
        try:
            resp = requests.get(url, headers=_HEADERS, timeout=timeout)
            resp.raise_for_status()
            # Validar Content-Type: si MAGyP devuelve HTML de error con codigo 200
            # (pasa con algunos proxies), rechazamos antes de intentar parsear.
            ct = resp.headers.get("Content-Type", "")
            tipos_validos = ("spreadsheetml", "octet-stream", "excel", "binary")
            if not any(t in ct for t in tipos_validos):
                _logger.warning(
                    "Content-Type inesperado de %s: '%s'. Intentando parsear igual.",
                    descripcion, ct,
                )
            return resp.content
        except requests.exceptions.RequestException as exc:
            ultimo_error = exc
            if intento <= _MAX_RETRIES:
                _logger.warning(
                    "Error bajando %s (intento %d/%d): %s. Reintento en %ds...",
                    descripcion, intento, _MAX_RETRIES + 1, exc, _RETRY_BACKOFF,
                )
                time.sleep(_RETRY_BACKOFF)
            else:
                _logger.error("Fallo definitivo bajando %s: %s", descripcion, exc)
    return None


def _parsear_xlsx(contenido: bytes) -> pd.DataFrame:
    """
    Parsea el contenido binario de un XLSX DJVE al esquema normalizado.
    Si la estructura cambio (nombres de columnas distintos), devuelve DataFrame
    vacio en vez de pinchar.
    """
    # Defensa XXE: rechazamos XLSX con DOCTYPE o ENTITY (vector clasico de
    # XML External Entity attack). MAGyP es fuente confiable y openpyxl
    # moderno no expande entidades por default, pero defendemos en
    # profundidad por si MAGyP es comprometido o cambian la implementacion.
    if b"<!DOCTYPE" in contenido or b"<!ENTITY" in contenido:
        _logger.error("XLSX rechazado: contiene DOCTYPE/ENTITY (posible XXE).")
        return pd.DataFrame()
    try:
        df = pd.read_excel(io.BytesIO(contenido), engine="openpyxl")
    except Exception:
        return pd.DataFrame()

    # Renombrar a snake_case.
    df = df.rename(columns=_ESQUEMA_COLUMNAS)

    # Verificar que al menos las columnas criticas estan.
    requeridas = {"producto", "toneladas", "fecha_registro", "razon_social"}
    if not requeridas.issubset(df.columns):
        return pd.DataFrame()

    # Normalizar tipos.
    for col in ("fecha_registro", "fecha_presentacion",
                "fecha_inicio_embarque", "fecha_fin_embarque"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    df["toneladas"] = pd.to_numeric(df["toneladas"], errors="coerce").fillna(0)
    df["producto"] = df["producto"].astype(str).str.strip().str.upper()
    df["razon_social"] = df["razon_social"].astype(str).str.strip()

    # Agregar codigo interno para cruce con line-up.
    df["codigo_interno"] = df["producto"].apply(_producto_a_codigo)

    return df


# ---------------------------------------------------------------------------
# Agregaciones usadas por el dashboard
# ---------------------------------------------------------------------------

def djve_por_producto_recientes(
    df: pd.DataFrame,
    dias: int = 30,
    hasta: date | None = None,
) -> pd.DataFrame:
    """
    Agrega DJVE por producto (codigo_interno) en los ultimos N dias.

    Filtra por fecha_registro. Solo incluye productos mapeados al line-up.

    Returns:
        DataFrame: codigo_interno, toneladas, n_djve (cantidad de declaraciones),
        razon_social_top (exportador con mas toneladas).
    """
    if df.empty:
        return pd.DataFrame()

    if hasta is None:
        hasta = date.today()
    desde = hasta - pd.Timedelta(days=dias)

    sub = df[(df["fecha_registro"] >= desde) & (df["fecha_registro"] <= hasta)].copy()
    sub = sub[sub["codigo_interno"].notna()]

    if sub.empty:
        return pd.DataFrame()

    # Agregado principal.
    agg = (
        sub.groupby("codigo_interno")
        .agg(
            toneladas=("toneladas", "sum"),
            n_djve=("nro_djve", "count"),
        )
        .reset_index()
    )

    # Top exportador por producto.
    top_exportador = (
        sub.groupby(["codigo_interno", "razon_social"])["toneladas"].sum()
        .reset_index()
        .sort_values(["codigo_interno", "toneladas"], ascending=[True, False])
        .groupby("codigo_interno")
        .first()
        .reset_index()
        .rename(columns={"razon_social": "razon_social_top",
                         "toneladas": "toneladas_top"})[
            ["codigo_interno", "razon_social_top"]
        ]
    )

    return agg.merge(top_exportador, on="codigo_interno", how="left")


def djve_diarias(
    df: pd.DataFrame,
    codigo_interno: str | None = None,
) -> pd.DataFrame:
    """
    Serie diaria de toneladas declaradas (DJVE por fecha_registro).

    Si codigo_interno es None, agrega todos los productos.
    """
    if df.empty:
        return pd.DataFrame()

    sub = df.copy()
    if codigo_interno is not None:
        sub = sub[sub["codigo_interno"] == codigo_interno]

    if sub.empty:
        return pd.DataFrame()

    return (
        sub.groupby("fecha_registro")["toneladas"].sum()
        .reset_index()
        .sort_values("fecha_registro")
    )


if __name__ == "__main__":
    import sys
    sys.stdout.reconfigure(encoding="utf-8")
    print("Descargando DJVE ano actual...")
    df = descargar_djve_acumuladas()
    print(f"Filas totales: {len(df):,}")
    if not df.empty:
        print(f"Columnas: {list(df.columns)}")
        print(f"\nRango fechas: {df['fecha_registro'].min()} -> {df['fecha_registro'].max()}")
        print(f"Toneladas totales: {int(df['toneladas'].sum()):,}")
        print(f"\nProductos mas frecuentes (crudo):")
        print(df["producto"].value_counts().head(10).to_string())
        print(f"\nMapeados al line-up:")
        mapeados = df[df["codigo_interno"].notna()]
        print(f"  {len(mapeados):,} filas / {int(mapeados['toneladas'].sum()):,} tons")
        print(f"\nAgregado ultimos 30 dias por producto:")
        print(djve_por_producto_recientes(df, dias=30).to_string(index=False))
