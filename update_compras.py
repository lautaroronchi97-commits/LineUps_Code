"""
Update diario de COMPRAS: descarga la comercializacion de granos del MAGyP
(compras de la exportacion e industria al productor) y la upsertea en la tabla
`compras` de Supabase.

Por que existe
--------------
El componente "farmer selling" del indice de calor de la pestana MESA mide el
ritmo de ventas del productor (avance de cosecha comercializado vs campanas
previas). El MAGyP publica esto semanalmente. Antes el dashboard lo descargaba
EN VIVO al renderizar, lo que: (a) sumaba ~20s por sesion y (b) desde IPs cloud
da 403. Persistir el dato en una tabla resuelve ambos: el runner de GitHub
Actions (IP no bloqueada) lo baja una vez al dia y el dashboard solo lee la DB.

Flujo:
1. Descarga las compras con compras_fas.descargar_compras (CKAN MAGyP).
2. Normaliza y filtra filas usables (con codigo_interno, sector, fecha).
3. Upsert idempotente en `compras` por (campana, codigo_interno, sector, fecha).

La tabla se crea una vez con compras.sql.

Uso local:
    python update_compras.py

Lo corre el workflow daily_update.yml junto a update_today.py y update_djve.py.
"""
from __future__ import annotations

import sys
from datetime import date

import pandas as pd

import campanas
import compras_fas
from db import upsert_compras
from utils import setup_logging

logger = setup_logging(__name__)

# Columnas que viajan a la tabla `compras`.
_COLUMNAS_DB = [
    "fecha", "grano_raw", "codigo_interno", "campana", "sector",
    "toneladas", "toneladas_a_fijar", "precio_promedio_usd",
    "porcentaje_cosecha",
]


def _df_a_filas(df: pd.DataFrame) -> list[dict]:
    """
    Convierte el DataFrame de compras_fas al formato que espera la tabla.

    - Descarta filas sin codigo_interno (grano no mapeado), sin sector o sin
      fecha (no se pueden upsertear: faltaria parte de la unique constraint).
    - Rellena la campana cuando viene vacia (la deriva del codigo + fecha).
    - Convierte fechas a ISO y NaN -> None (Supabase no acepta NaN en JSON).
    """
    if df.empty:
        return []

    df = df.copy()

    # Filtrar lo que no se puede ubicar.
    if "codigo_interno" not in df.columns:
        return []
    df = df[df["codigo_interno"].notna()]
    if "fecha" in df.columns:
        df = df[df["fecha"].notna()]
    else:
        return []
    if "sector" not in df.columns:
        df["sector"] = compras_fas.SECTOR_EXPORTACION
    df = df[df["sector"].notna()]
    if df.empty:
        return []

    # Derivar campana faltante desde codigo + fecha.
    def _campana(row):
        c = row.get("campana")
        if c and not (isinstance(c, float) and pd.isna(c)):
            return c
        f = row.get("fecha")
        if isinstance(f, date):
            return campanas.campana_de(row["codigo_interno"], f)
        return None

    df["campana"] = df.apply(_campana, axis=1)
    df = df[df["campana"].notna()]
    if df.empty:
        return []

    # Asegurar que existan todas las columnas opcionales.
    for col in _COLUMNAS_DB:
        if col not in df.columns:
            df[col] = None

    # Fechas date -> ISO str.
    df["fecha"] = df["fecha"].apply(
        lambda d: d.isoformat() if isinstance(d, date) else None
    )
    df["toneladas"] = pd.to_numeric(df["toneladas"], errors="coerce").fillna(0).astype(float)

    df = df[_COLUMNAS_DB]
    # astype(object) primero: sobre columnas float, where(..., None) reconvierte
    # None a NaN. Con dtype object el None se mantiene (Supabase no acepta NaN).
    df = df.astype(object).where(pd.notna(df), None)
    return df.to_dict(orient="records")


def actualizar() -> tuple[int, int]:
    """
    Descarga + upsertea las compras MAGyP.

    Returns:
        (filas_descargadas, filas_upserted)
    """
    logger.info("Compras MAGyP: descargando comercializacion...")
    df = compras_fas.descargar_compras(timeout=60)
    if df.empty:
        logger.warning("Compras: descarga vacia o fallida (¿403 desde esta IP?).")
        return 0, 0

    logger.info("Compras: %d filas descargadas. Upsertando...", len(df))
    filas = _df_a_filas(df)
    if not filas:
        logger.warning("Compras: no hay filas validas para upsertear "
                       "(sin codigo/sector/fecha utilizable).")
        return len(df), 0

    upsert_compras(filas)
    logger.info("Compras: %d filas upserted.", len(filas))
    return len(df), len(filas)


def main() -> int:
    logger.info("Update compras MAGyP: arrancando.")
    try:
        descargadas, upserted = actualizar()
    except Exception as exc:  # noqa: BLE001
        logger.error("FALLO update compras: %s", exc)
        return 1

    logger.info("Update compras terminado: %d descargadas, %d upserted.",
                descargadas, upserted)
    # Sin datos (probable 403) no es un error duro: el indice degrada solo.
    return 0


if __name__ == "__main__":
    sys.exit(main())
