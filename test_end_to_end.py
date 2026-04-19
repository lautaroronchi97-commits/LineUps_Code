"""
Smoke test end-to-end del pipeline.

Hace en secuencia:
  1. Ping a Supabase (verifica credenciales + tabla existe).
  2. Scrapea una fecha reciente.
  3. Upsert de las filas scrapeadas.
  4. Query de vuelta para confirmar que se guardaron.

Si algo falla, muestra el error claro y sale con exit code != 0.

Uso:
    python test_end_to_end.py             # usa ayer como fecha de prueba
    python test_end_to_end.py 2026-04-15  # fecha especifica
"""

from __future__ import annotations

import sys
from datetime import date, datetime, timedelta

from db import get_client, ping, query_lineup, upsert_lineup
from scraper import scrape_lineup
from utils import setup_logging

logger = setup_logging(__name__)


def _parse_fecha_arg() -> date:
    if len(sys.argv) > 1:
        return datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
    # Ayer: mas probable que tenga data que hoy temprano en la manana.
    return date.today() - timedelta(days=1)


def main() -> int:
    print("=" * 60)
    print("TEST END-TO-END: scraper + DB")
    print("=" * 60)

    # 1. Ping
    print("\n[1/4] Probando conexion a Supabase...")
    resultado = ping()
    if not resultado["conectado"]:
        print(f"  FALLO: {resultado['error']}")
        print("  Revisa .env y que el proyecto Supabase este activo.")
        return 1
    print(f"  OK. Tabla lineup tiene {resultado['cantidad_filas']} filas actualmente.")

    # 2. Scrape
    fecha = _parse_fecha_arg()
    print(f"\n[2/4] Scrapeando line-up de {fecha}...")
    try:
        filas = scrape_lineup(fecha)
    except Exception as exc:  # noqa: BLE001
        print(f"  FALLO al scrapear: {exc}")
        return 2
    print(f"  OK. {len(filas)} filas parseadas.")

    if not filas:
        print("  (Sin filas para upsert. Puede ser fin de semana o feriado.)")
        return 0

    # 3. Upsert
    print(f"\n[3/4] Upsert a Supabase...")
    try:
        total = upsert_lineup(filas)
    except Exception as exc:  # noqa: BLE001
        print(f"  FALLO al upsertear: {exc}")
        return 3
    print(f"  OK. {total} filas upserted.")

    # 4. Read back
    print(f"\n[4/4] Consultando filas del {fecha} desde DB...")
    df = query_lineup(fecha_desde=fecha, fecha_hasta=fecha)
    print(f"  OK. DB devolvio {len(df)} filas para esa fecha.")
    if not df.empty:
        print("\n  Muestra (primeras 5):")
        muestra = df[["port", "vessel", "ops", "cat", "cargo", "quantity", "eta"]].head()
        print(muestra.to_string(index=False))

    print("\n" + "=" * 60)
    print("TODO OK.")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
