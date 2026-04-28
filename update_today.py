"""
Update diario: re-scrapea hoy y los ultimos 3 dias para capturar correcciones
que ISA haya aplicado a los datos historicos (cambios de ETA/ETB, correccion
de quantities, etc.).

Lo corre GitHub Actions todos los dias a las 13:00 UTC (10:00 ART).

Uso:
    python update_today.py                 # hoy + 3 dias hacia atras
    python update_today.py --dias 7        # hoy + 7 dias hacia atras
    python update_today.py --solo-hoy      # solo hoy
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import date, timedelta

from config import DEFAULT_DELAY_SECONDS
from db import upsert_lineup
from scraper import scrape_lineup
from utils import setup_logging

logger = setup_logging(__name__)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dias", type=int, default=3,
                        help="Cuantos dias hacia atras re-scrapear (ademas de hoy). Default: 3. Max: 30.")
    parser.add_argument("--solo-hoy", action="store_true",
                        help="Solo scrapea hoy (ignora --dias).")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY_SECONDS,
                        help=f"Segundos entre requests. Minimo: 0.5. Default: {DEFAULT_DELAY_SECONDS}")
    args = parser.parse_args()

    # Validar bounds para evitar baneos de IP o saturacion de cuotas.
    if not 0 <= args.dias <= 30:
        parser.error(f"--dias debe estar entre 0 y 30 (recibido: {args.dias})")
    if args.delay < 0.5:
        parser.error(f"--delay minimo es 0.5s para no saturar el servidor (recibido: {args.delay})")

    hoy = date.today()
    if args.solo_hoy:
        fechas = [hoy]
    else:
        fechas = [hoy - timedelta(days=i) for i in range(args.dias + 1)]
        # De mas vieja a mas nueva, asi el hoy queda al final y es lo ultimo upserted.
        fechas.sort()

    logger.info("Update diario: voy a re-scrapear %d fecha(s): %s",
                len(fechas), ", ".join(f.isoformat() for f in fechas))

    total_filas = 0
    total_fallas = 0

    for fecha in fechas:
        try:
            filas = scrape_lineup(fecha)
            if filas:
                upsert_lineup(filas)
                total_filas += len(filas)
                logger.info("OK %s: %d filas upserted.", fecha, len(filas))
            else:
                logger.info("OK %s: 0 filas.", fecha)
        except Exception as exc:  # noqa: BLE001
            total_fallas += 1
            logger.error("FALLO %s: %s", fecha, exc)

        time.sleep(args.delay)

    logger.info("Update terminado: %d filas total, %d fechas fallidas.",
                total_filas, total_fallas)

    # Exit code: una falla aislada (network blip, dia sin datos) no rompe el
    # cron, pero si fallo la mayoria de fechas hay un problema sistematico
    # (auth, scraper roto, ISA caido) y queremos que GitHub Actions lo
    # marque como failed para que la notificacion llegue.
    if total_fallas > 0:
        logger.warning("FALLO PARCIAL: %d de %d fechas fallaron.",
                       total_fallas, len(fechas))
    if total_fallas > len(fechas) // 2:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
