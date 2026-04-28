"""
Backfill historico del line-up.

Recorre un rango de fechas, scrapea cada una y hace upsert a Supabase.
Es resumable: al arrancar consulta que fechas ya estan cargadas y las saltea.

Uso:
    # Full historico desde 2020 hasta hoy (tarda ~1h20min con delay=2)
    python backfill.py

    # Rango especifico
    python backfill.py --from-date 2024-01-01 --to-date 2024-03-31

    # Ajustar delay (default 2 segundos entre requests)
    python backfill.py --delay 3

    # Forzar re-scrape (ignorar fechas ya cargadas)
    python backfill.py --no-skip
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import date, datetime, timedelta

from tqdm import tqdm

from config import DEFAULT_DELAY_SECONDS
from db import get_fechas_ya_cargadas, upsert_lineup
from scraper import scrape_lineup
from utils import setup_logging

logger = setup_logging(__name__)


# Default: arrancamos desde el 1 de enero de 2020 (~6 anos de histrico).
FECHA_DEFAULT_DESDE = date(2020, 1, 1)

# Si fallan estas fechas consecutivas, algo anda mal (no solo un dia sin data).
# Cortamos para evitar martillar el server.
MAX_FALLOS_CONSECUTIVOS = 5


def _parse_fecha(s: str) -> date:
    if s.lower() == "today":
        return date.today()
    return datetime.strptime(s, "%Y-%m-%d").date()


def generar_fechas(desde: date, hasta: date) -> list[date]:
    """Genera la lista de fechas dia a dia entre `desde` y `hasta` inclusive."""
    if desde > hasta:
        raise ValueError(f"--from-date ({desde}) es posterior a --to-date ({hasta})")
    dias = (hasta - desde).days + 1
    return [desde + timedelta(days=i) for i in range(dias)]


def backfill(desde: date, hasta: date, delay: float, saltar_existentes: bool) -> None:
    """Itera dia por dia, scrapea y upsertea. Loguea progreso con tqdm."""
    todas_las_fechas = generar_fechas(desde, hasta)

    if saltar_existentes:
        ya_cargadas = get_fechas_ya_cargadas()
        pendientes = [f for f in todas_las_fechas if f not in ya_cargadas]
        logger.info(
            "Rango %s - %s: %d fechas en total, %d ya cargadas, %d pendientes.",
            desde, hasta, len(todas_las_fechas), len(ya_cargadas & set(todas_las_fechas)),
            len(pendientes),
        )
    else:
        pendientes = todas_las_fechas
        logger.info("Rango %s - %s: %d fechas (sin saltear existentes).",
                    desde, hasta, len(pendientes))

    if not pendientes:
        logger.info("No hay fechas pendientes. Fin.")
        return

    total_filas_insertadas = 0
    total_fechas_con_data = 0
    fallos_consecutivos = 0

    with tqdm(total=len(pendientes), desc="Backfill", unit="dia") as pbar:
        for fecha in pendientes:
            try:
                filas = scrape_lineup(fecha)
                if filas:
                    upsert_lineup(filas)
                    total_filas_insertadas += len(filas)
                    total_fechas_con_data += 1
                else:
                    logger.info("Fecha %s: 0 filas (fin de semana o feriado).", fecha)
                fallos_consecutivos = 0
            except Exception as exc:  # noqa: BLE001
                fallos_consecutivos += 1
                logger.error("Fecha %s fallo: %s", fecha, exc)
                if fallos_consecutivos >= MAX_FALLOS_CONSECUTIVOS:
                    logger.critical(
                        "Abortando: %d fechas consecutivas fallidas. "
                        "Revisa conectividad o si ISA cambio la pagina.",
                        fallos_consecutivos,
                    )
                    break

            pbar.update(1)
            pbar.set_postfix(filas=total_filas_insertadas, con_data=total_fechas_con_data)
            time.sleep(delay)

    logger.info(
        "Backfill terminado: %d fechas procesadas, %d fechas con data, %d filas upserted total.",
        len(pendientes), total_fechas_con_data, total_filas_insertadas,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--from-date", type=_parse_fecha, default=FECHA_DEFAULT_DESDE,
        help=f"Fecha desde (YYYY-MM-DD). Default: {FECHA_DEFAULT_DESDE.isoformat()}",
    )
    parser.add_argument(
        "--to-date", type=_parse_fecha, default=date.today(),
        help="Fecha hasta (YYYY-MM-DD o 'today'). Default: hoy.",
    )
    parser.add_argument(
        "--delay", type=float, default=DEFAULT_DELAY_SECONDS,
        help=f"Segundos entre requests. Minimo: 0.5. Default: {DEFAULT_DELAY_SECONDS}",
    )
    parser.add_argument(
        "--no-skip", action="store_true",
        help="Re-scrapea fechas que ya tienen data en la DB.",
    )
    args = parser.parse_args()

    # Validar delay minimo para no banear la IP del runner.
    if args.delay < 0.5:
        parser.error(f"--delay minimo es 0.5s para no saturar el servidor (recibido: {args.delay})")

    backfill(
        desde=args.from_date,
        hasta=args.to_date,
        delay=args.delay,
        saltar_existentes=not args.no_skip,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
