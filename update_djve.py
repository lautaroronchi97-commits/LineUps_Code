"""
Update diario de DJVE: descarga el XLSX acumulado del MAGyP y lo upsertea
en la tabla `djve` de Supabase.

Las DJVE (Declaraciones Juradas de Ventas al Exterior, Ley 21.453) las
publica el MAGyP en un XLSX acumulado por ano. Cada dia se actualiza
agregando las DJVE recien aprobadas. Este script:

1. Descarga el XLSX del ano corriente.
2. Lo parsea con fob_djve.descargar_djve_acumuladas.
3. Hace upsert en la tabla `djve` (idempotente: si una DJVE ya existia, se
   actualiza; si es nueva, se inserta).

El dashboard lee la tabla `djve` directamente. Esto evita descargar el XLSX
en cada apertura de la pestana Productos (era ~30s por usuario por sesion).

Uso local:
    python update_djve.py            # ano actual
    python update_djve.py --anio 2025  # ano especifico
    python update_djve.py --anios 2024,2025,2026  # varios anos

Lo corre el Programador de Tareas de Windows todos los dias junto al
update_today.py (line-up ISA). Ver scheduled_update.bat.
"""
from __future__ import annotations

import argparse
import sys
from datetime import date

import pandas as pd

import fob_djve
from db import upsert_djve
from utils import setup_logging

logger = setup_logging(__name__)


def _df_a_filas(df: pd.DataFrame, anio: int) -> list[dict]:
    """
    Convierte el DataFrame de fob_djve al formato que espera la tabla.

    - Agrega la columna `anio` (parte del unique constraint).
    - Convierte fechas a string ISO (Supabase los acepta asi).
    - Filtra filas sin nro_djve (no se pueden upsertear sin clave).
    """
    if df.empty:
        return []

    df = df.copy()
    df["anio"] = anio

    # Convertir fechas date -> ISO str (Supabase rest API).
    for col in ("fecha_registro", "fecha_presentacion",
                "fecha_inicio_embarque", "fecha_fin_embarque"):
        if col in df.columns:
            df[col] = df[col].apply(
                lambda d: d.isoformat() if isinstance(d, date) else None
            )

    # toneladas ya viene numerico; pasamos a float por seguridad JSON.
    if "toneladas" in df.columns:
        df["toneladas"] = pd.to_numeric(df["toneladas"], errors="coerce").fillna(0).astype(float)

    # nro_djve a string (a veces viene como int).
    if "nro_djve" in df.columns:
        df["nro_djve"] = df["nro_djve"].astype(str).str.strip()
        df = df[df["nro_djve"].notna() & (df["nro_djve"] != "")]
        df = df[df["nro_djve"] != "nan"]

    columnas_db = [
        "anio", "nro_djve", "fecha_registro", "fecha_presentacion",
        "producto", "toneladas", "fecha_inicio_embarque",
        "fecha_fin_embarque", "opcion", "razon_social", "codigo_interno",
    ]
    df = df[[c for c in columnas_db if c in df.columns]]

    # NaN -> None (Supabase no acepta NaN en JSON).
    df = df.where(pd.notna(df), None)

    return df.to_dict(orient="records")


def actualizar_anio(anio: int) -> tuple[int, int]:
    """
    Descarga + upsertea las DJVE de un ano.

    Returns:
        (filas_descargadas, filas_upserted)
    """
    logger.info("DJVE %d: descargando XLSX del MAGyP...", anio)
    df = fob_djve.descargar_djve_acumuladas(anio)
    if df.empty:
        logger.warning("DJVE %d: no se pudo descargar o el XLSX vino vacio.", anio)
        return 0, 0

    logger.info("DJVE %d: %d filas descargadas. Upsertando...", anio, len(df))
    filas = _df_a_filas(df, anio)
    if not filas:
        logger.warning("DJVE %d: no hay filas validas para upsertear "
                       "(nro_djve nulo en todas).", anio)
        return len(df), 0

    upsert_djve(filas)
    logger.info("DJVE %d: %d filas upserted.", anio, len(filas))
    return len(df), len(filas)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--anio", type=int, default=None,
        help="Ano a actualizar. Default: ano actual.",
    )
    parser.add_argument(
        "--anios", type=str, default=None,
        help="Lista de anos separados por coma. Ej: 2024,2025,2026. "
             "Si se pasa, ignora --anio.",
    )
    args = parser.parse_args()

    if args.anios:
        anios = [int(a.strip()) for a in args.anios.split(",") if a.strip()]
    elif args.anio:
        anios = [args.anio]
    else:
        anios = [date.today().year]

    logger.info("Update DJVE: voy a actualizar %d ano(s): %s",
                len(anios), ", ".join(str(a) for a in anios))

    total_descargadas = 0
    total_upserted = 0
    fallidos = 0

    for anio in anios:
        try:
            desc, ups = actualizar_anio(anio)
            total_descargadas += desc
            total_upserted += ups
            if desc == 0:
                fallidos += 1
        except Exception as exc:  # noqa: BLE001
            fallidos += 1
            logger.error("FALLO ano %d: %s", anio, exc)

    logger.info("Update DJVE terminado: %d descargadas, %d upserted, %d anos fallidos.",
                total_descargadas, total_upserted, fallidos)

    return 1 if fallidos == len(anios) else 0


if __name__ == "__main__":
    sys.exit(main())
