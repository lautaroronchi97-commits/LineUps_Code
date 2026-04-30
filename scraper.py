"""
Scraper del Line Up de ISA Agents.

Baja el HTML para una fecha dada y lo parsea a una lista de diccionarios
listos para insertar en la tabla `lineup` de Supabase.

Uso como script (prueba rapida sin tocar la DB):
    python scraper.py                # scrapea hoy
    python scraper.py 2026-04-15     # scrapea una fecha especifica
"""

from __future__ import annotations

import sys
import time
from datetime import date, datetime, timedelta
from typing import Any

import requests
from bs4 import BeautifulSoup

from config import (
    BASE_URL,
    DB_COLUMNS,
    EXPECTED_HEADERS,
    MAX_RETRIES,
    REQUEST_TIMEOUT,
    RETRY_BACKOFF_SECONDS,
    USER_AGENT,
)
from utils import (
    ajustar_anio_por_rollover,
    es_agro,
    parse_fecha_corta,
    parse_quantity,
    parse_text_cell,
    setup_logging,
)

logger = setup_logging(__name__)


# ---------------------------------------------------------------------------
# Fetch HTML
# ---------------------------------------------------------------------------

def fetch_lineup_html(fecha: date) -> str:
    """
    Descarga el HTML crudo del Line Up para una fecha especifica.

    Construye la URL con los parametros que espera el form de ISA:
        ?lang=es&select_day=DD&select_month=MM&select_year=YYYY&mode=Search

    Reintenta hasta MAX_RETRIES veces con backoff si hay error de red o el
    server devuelve algo raro. Levanta requests.RequestException si falla
    definitivamente.
    """
    params = {
        "lang": "es",
        "select_day": f"{fecha.day:02d}",
        "select_month": f"{fecha.month:02d}",
        "select_year": f"{fecha.year:04d}",
        "mode": "Search",
    }
    headers = {"User-Agent": USER_AGENT}

    ultimo_error: Exception | None = None
    for intento in range(1, MAX_RETRIES + 2):  # primer intento + reintentos
        try:
            resp = requests.get(
                BASE_URL,
                params=params,
                headers=headers,
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            # Validar Content-Type: si ISA devuelve una pagina de bloqueo o login
            # wall con 200-OK, lo detectamos antes de intentar parsear la tabla.
            ct = resp.headers.get("Content-Type", "")
            if "text/html" not in ct:
                logger.warning(
                    "Content-Type inesperado de ISA para %s: '%s'. Intentando parsear igual.",
                    fecha, ct,
                )
            # La pagina viene en iso-8859-1 segun el meta. requests a veces
            # adivina mal la codificacion; se la fijamos explicita.
            resp.encoding = "iso-8859-1"
            return resp.text
        except requests.RequestException as exc:
            ultimo_error = exc
            if intento <= MAX_RETRIES:
                logger.warning(
                    "Error al bajar %s (intento %d/%d): %s. Reintento en %ds...",
                    fecha,
                    intento,
                    MAX_RETRIES + 1,
                    exc,
                    RETRY_BACKOFF_SECONDS,
                )
                time.sleep(RETRY_BACKOFF_SECONDS)
            else:
                logger.error("Fallo definitivo al bajar %s: %s", fecha, exc)

    raise requests.RequestException(
        f"No se pudo bajar el line up de {fecha} despues de {MAX_RETRIES + 1} intentos"
    ) from ultimo_error


# ---------------------------------------------------------------------------
# Parse HTML
# ---------------------------------------------------------------------------

def _validar_headers(thead, fecha_consulta: date) -> None:
    """Chequea que las columnas del <thead> sean las esperadas.

    Si ISA reordena o renombra columnas, el parser silenciosamente mezclaria
    datos. Preferimos fallar con un mensaje claro.
    """
    if thead is None:
        raise ValueError(
            f"No se encontro <thead> en la tabla del {fecha_consulta}. "
            "La pagina puede haber cambiado de estructura."
        )
    headers_actuales = [th.get_text(strip=True) for th in thead.find_all("th")]
    if headers_actuales != EXPECTED_HEADERS:
        raise ValueError(
            f"Los headers de la tabla cambiaron en {fecha_consulta}.\n"
            f"  Esperados: {EXPECTED_HEADERS}\n"
            f"  Encontrados: {headers_actuales}"
        )


def parse_lineup_table(html: str, fecha_consulta: date) -> list[dict[str, Any]]:
    """
    Parsea el HTML crudo y devuelve una lista de dicts, uno por fila de la tabla.

    Cada dict tiene keys que matchean las columnas de la tabla `lineup` en
    Supabase, mas `fecha_consulta` y `es_agro`. Las fechas (eta/etb/ets) ya
    vienen como datetime.date con el ano ajustado por rollover.
    """
    soup = BeautifulSoup(html, "lxml")

    # Sanity check #1: el titulo "Line Up - April 15th" deberia matchear la fecha.
    titulo = soup.find("h2", class_="title")
    if titulo:
        texto_titulo = titulo.get_text(strip=True)
        fecha_esperada_en_titulo = fecha_consulta.strftime("%B")  # "April"
        if fecha_esperada_en_titulo not in texto_titulo:
            logger.warning(
                "Titulo de la pagina (%r) no incluye el mes de la fecha consultada %s. "
                "Puede que el server haya devuelto otro dia (feriado?).",
                texto_titulo,
                fecha_consulta,
            )

    tabla = soup.find("table", id="line-up-data")
    if tabla is None:
        raise ValueError(
            f"No se encontro <table id='line-up-data'> en {fecha_consulta}. "
            "La pagina puede haber cambiado."
        )

    _validar_headers(tabla.find("thead"), fecha_consulta)

    tbody = tabla.find("tbody")
    if tbody is None:
        logger.info("Fecha %s: la tabla existe pero no tiene <tbody> (0 filas).", fecha_consulta)
        return []

    filas_raw = tbody.find_all("tr")
    filas: list[dict[str, Any]] = []

    for idx, tr in enumerate(filas_raw, start=1):
        tds = tr.find_all("td")
        if len(tds) != len(EXPECTED_HEADERS):
            logger.warning(
                "Fila %d de %s tiene %d celdas (esperaba %d). La salteo.",
                idx,
                fecha_consulta,
                len(tds),
                len(EXPECTED_HEADERS),
            )
            continue

        # Extraemos texto de cada celda en el orden de EXPECTED_HEADERS.
        textos = [td.get_text() for td in tds]

        # Las 3 columnas de fecha al final requieren parseo especial.
        eta_raw, etb_raw, ets_raw = textos[10], textos[11], textos[12]

        fila: dict[str, Any] = {
            "fecha_consulta": fecha_consulta.isoformat(),
            "port": parse_text_cell(textos[0]),
            "berth": parse_text_cell(textos[1]),
            "vessel": parse_text_cell(textos[2]),
            "ops": parse_text_cell(textos[3]),
            "cat": parse_text_cell(textos[4]),
            "cargo": parse_text_cell(textos[5]),
            "quantity": parse_quantity(textos[6]),
            "dest_orig": parse_text_cell(textos[7]),
            "area": parse_text_cell(textos[8]),
            "shipper": parse_text_cell(textos[9]),
            "eta": _fecha_ajustada(eta_raw, fecha_consulta),
            "etb": _fecha_ajustada(etb_raw, fecha_consulta),
            "ets": _fecha_ajustada(ets_raw, fecha_consulta),
            "remarks": parse_text_cell(textos[13]),
        }

        # port y vessel son NOT NULL en la DB. Si vienen vacios, la fila esta
        # rota y no tiene sentido insertarla.
        if fila["port"] is None or fila["vessel"] is None:
            logger.warning(
                "Fila %d de %s sin port/vessel. La salteo (datos incompletos).",
                idx,
                fecha_consulta,
            )
            continue

        fila["es_agro"] = es_agro(fila["cat"])
        filas.append(fila)

    # La fuente a veces publica filas literalmente identicas (mismo buque, mismo
    # berth, misma carga, misma cantidad, mismo destino). Si las mandamos asi al
    # upsert, Postgres falla con "ON CONFLICT DO UPDATE command cannot affect
    # row a second time" porque dos filas del mismo batch apuntan a la misma key.
    # Dedup por valor completo preservando el orden de aparicion.
    # Las keys del dict siempre vienen en el mismo orden (Python 3.7+),
    # asi que sorted() es redundante. Tuple directo es ~2x mas rapido.
    vistas: set[tuple] = set()
    dedup: list[dict[str, Any]] = []
    _dedup_keys = ("fecha_consulta", "port", "berth", "vessel", "ops", "cat",
                   "cargo", "quantity", "dest_orig", "area", "shipper",
                   "eta", "etb", "ets", "remarks")
    for fila in filas:
        clave = tuple(fila.get(k) for k in _dedup_keys)
        if clave in vistas:
            logger.warning(
                "Fecha %s: fila duplicada exacta detectada (%s / %s / %s). Descarto la copia.",
                fecha_consulta,
                fila.get("port"),
                fila.get("vessel"),
                fila.get("cargo"),
            )
            continue
        vistas.add(clave)
        dedup.append(fila)

    logger.info(
        "Fecha %s: parseadas %d filas (%d tras dedup).",
        fecha_consulta,
        len(filas),
        len(dedup),
    )
    return dedup


def _fecha_ajustada(raw: str | None, fecha_consulta: date) -> str | None:
    """Parsea una celda 'DD-mmm' a ISO date string, con rollover de ano.

    Devuelve el string ISO (ej '2026-04-14') porque Supabase lo consume asi
    por JSON. Si no se puede parsear, devuelve None (queda NULL en la DB).
    """
    parseada = parse_fecha_corta(raw, fecha_consulta.year)
    ajustada = ajustar_anio_por_rollover(parseada, fecha_consulta)
    return ajustada.isoformat() if ajustada else None


# ---------------------------------------------------------------------------
# API publica de alto nivel
# ---------------------------------------------------------------------------

def scrape_lineup(fecha: date) -> list[dict[str, Any]]:
    """
    Fetch + parse en un solo llamado. Devuelve filas listas para upsert.
    """
    html = fetch_lineup_html(fecha)
    return parse_lineup_table(html, fecha)


# ---------------------------------------------------------------------------
# CLI de prueba (no toca la DB)
# ---------------------------------------------------------------------------

def _main() -> None:
    """Uso: python scraper.py [YYYY-MM-DD]"""
    if len(sys.argv) > 1:
        fecha = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
    else:
        fecha = date.today()

    logger.info("Scrapeando %s ...", fecha)
    filas = scrape_lineup(fecha)

    if not filas:
        print(f"No hay filas para {fecha} (o el server devolvio tabla vacia).")
        return

    print(f"\n{len(filas)} filas parseadas. Primeras 5:\n")
    for fila in filas[:5]:
        print(f"  {fila['port']:15s} | {fila['vessel']:25s} | {fila['ops']:6s} | "
              f"{fila['cat']:12s} | {fila['cargo']:10s} | qty={fila['quantity']} | "
              f"eta={fila['eta']} | agro={fila['es_agro']}")

    # Resumen por categoria agro.
    agro_count = sum(1 for f in filas if f["es_agro"])
    print(f"\nAgro: {agro_count} / {len(filas)} filas")


if __name__ == "__main__":
    _main()
