"""
Funciones utilitarias puras (sin estado) usadas por scraper, db y dashboard.

Mantener aca solo funciones que:
  - Reciben inputs simples y devuelven outputs simples.
  - No tocan red, DB ni archivos.
  - Se pueden testear de forma aislada.
"""

from __future__ import annotations

import logging
import re
from datetime import date
from pathlib import Path

from config import AGRO_CATEGORIES, MESES_ES


# ---------------------------------------------------------------------------
# Parseo de celdas de la tabla
# ---------------------------------------------------------------------------

def parse_text_cell(raw: str | None) -> str | None:
    """
    Normaliza el contenido de una celda <td> de texto.

    - Strip de espacios y &nbsp; (ya resueltos por BeautifulSoup a '\\xa0').
    - Si queda vacio o es solo "-", devuelve None (para que termine como NULL
      en la base, no como string vacio).
    """
    if raw is None:
        return None
    # \xa0 es el caracter unicode de &nbsp;. Lo tratamos como espacio.
    limpio = raw.replace("\xa0", " ").strip()
    if limpio == "" or limpio == "-":
        return None
    return limpio


def parse_quantity(raw: str | None) -> int | None:
    """
    Convierte la columna Quantity a entero.

    Acepta formatos como "46000", "46,000", "46.000". Si esta vacio o no
    se puede parsear, devuelve None (preferimos perder una fila que meter
    data mala).
    """
    texto = parse_text_cell(raw)
    if texto is None:
        return None
    # Sacamos separadores comunes de miles (coma, punto, espacio).
    solo_digitos = re.sub(r"[,.\s]", "", texto)
    if not solo_digitos.isdigit():
        return None
    return int(solo_digitos)


def parse_fecha_corta(raw: str | None, anio_consulta: int) -> date | None:
    """
    Parsea fechas como "14-abr" / "5-ene" a datetime.date usando el ano de la
    fecha de consulta como referencia.

    Heuristica de rollover de ano:
    - Si consulto en diciembre y aparece "5-ene", asumo que ese enero es del
      ano siguiente (el buque llega despues del cambio de ano).
    - Si consulto en enero y aparece "28-dic", asumo que ese diciembre es del
      ano anterior (es un ETA atrasado del ano que paso).

    Devuelve None si la celda esta vacia o mal formada.
    """
    texto = parse_text_cell(raw)
    if texto is None:
        return None

    # Formato esperado: "DD-mmm" con DD de 1 a 2 digitos y mmm de 3 letras.
    match = re.match(r"^(\d{1,2})[-/](\w{3})$", texto.lower())
    if not match:
        return None

    dia_str, mes_abrev = match.groups()
    mes = MESES_ES.get(mes_abrev)
    if mes is None:
        return None

    try:
        dia = int(dia_str)
    except ValueError:
        return None

    # Heuristica de rollover: si la diferencia entre el mes parseado y el mes
    # de consulta es mayor a 6, cruzamos de ano.
    mes_consulta = date(anio_consulta, 1, 1).month  # placeholder, se ajusta abajo
    # (la funcion no conoce el mes real de consulta; usamos solo el ano)
    # Por eso el rollover real lo hacemos en el scraper, que SI conoce la
    # fecha_consulta completa. Aca devolvemos la fecha tal cual con anio_consulta.

    try:
        return date(anio_consulta, mes, dia)
    except ValueError:
        # Fecha invalida (ej: 31-feb). Preferimos None que pinchar.
        return None


def ajustar_anio_por_rollover(
    fecha_parseada: date | None,
    fecha_consulta: date,
) -> date | None:
    """
    Ajusta el ano de una fecha corta (ETA/ETB/ETS) cuando cruza el cambio de ano.

    Ejemplos:
      fecha_consulta=2024-12-28, fecha_parseada=2024-01-05  -> 2025-01-05
      fecha_consulta=2025-01-03, fecha_parseada=2025-12-28  -> 2024-12-28
      fecha_consulta=2024-04-15, fecha_parseada=2024-04-18  -> 2024-04-18 (igual)

    Heuristica: si la distancia entre fechas es > 6 meses, cruzamos de ano.
    """
    if fecha_parseada is None:
        return None

    diff_meses = (fecha_parseada.year - fecha_consulta.year) * 12 + (
        fecha_parseada.month - fecha_consulta.month
    )

    if diff_meses > 6:
        # La fecha parseada cae muy despues: probablemente del ano anterior.
        try:
            return fecha_parseada.replace(year=fecha_parseada.year - 1)
        except ValueError:
            return fecha_parseada
    if diff_meses < -6:
        # La fecha parseada cae muy antes: probablemente del ano siguiente.
        try:
            return fecha_parseada.replace(year=fecha_parseada.year + 1)
        except ValueError:
            return fecha_parseada
    return fecha_parseada


# ---------------------------------------------------------------------------
# Clasificacion
# ---------------------------------------------------------------------------

def es_agro(categoria: str | None) -> bool:
    """
    True si la categoria esta en el set AGRO_CATEGORIES (GRAINS, BY PRODUCTS, VEGOIL).

    Ojo: fertilizantes NO son "agro puro" para este flag, pero los guardamos
    igual en la DB y los filtramos en el dashboard por separado.
    """
    if categoria is None:
        return False
    return categoria.strip().upper() in AGRO_CATEGORIES


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging(nombre: str = "lineup", nivel: int = logging.INFO) -> logging.Logger:
    """
    Configura un logger que escribe a consola y a logs/scraper.log.

    El RotatingFileHandler se registra UNA sola vez en el root logger para
    que todos los loggers nombrados propaguen al mismo handler de archivo.
    En Windows, multiples RotatingFileHandlers apuntando al mismo archivo
    causan PermissionError al intentar rotar (el OS bloquea el rename).

    Uso:
        logger = setup_logging(__name__)
        logger.info("Arrancando backfill...")
    """
    from logging.handlers import RotatingFileHandler

    formato = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # ---- Root logger: consola + archivo (solo configurar una vez) ----
    root = logging.getLogger()
    if not root.handlers:
        root.setLevel(logging.DEBUG)  # el nivel efectivo lo controla cada logger

        consola = logging.StreamHandler()
        consola.setFormatter(formato)
        root.addHandler(consola)

        # Un solo RotatingFileHandler compartido por todos los modulos.
        # 5 MB x 3 archivos = maximo 15 MB en disco.
        logs_dir = Path(__file__).parent / "logs"
        logs_dir.mkdir(exist_ok=True)
        archivo = RotatingFileHandler(
            logs_dir / "scraper.log",
            maxBytes=5 * 1024 * 1024,
            backupCount=3,
            encoding="utf-8",
        )
        archivo.setFormatter(formato)
        root.addHandler(archivo)

    # ---- Logger nombrado: solo establece el nivel, propaga al root ----
    logger = logging.getLogger(nombre)
    logger.setLevel(nivel)
    # propagate=True es el default: los mensajes suben al root y llegan
    # al handler de archivo sin que este logger necesite su propio handler.
    return logger
