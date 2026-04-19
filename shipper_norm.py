"""
Normalizacion de shippers (exportadores) a nombres canonicos.

El line-up bruto tiene ~282 variantes ortograficas de nombres de shippers
(razones sociales, filiales regionales, typos). Para el analisis por shipper
necesitamos pocas entidades estables: ~10 jugadores principales + "OTROS".

Decisiones clave:
- VITERRA + BUNGE + OMHSA (Oleaginosa Moreno Hnos) se fusionan en "VITERRA-BUNGE"
  (fusion real de 2023).
- MOLINOS AGRO y MOLINOS RIO DE LA PLATA son la misma empresa -> "MOLINOS".
- MALTERIA QUILMES y QUILMES son lo mismo -> "QUILMES".
- Las filiales PY/UY se fusionan con su casa matriz, PERO guardamos un flag
  `origen_alt` ("PY" o "UY") para poder aislar el flujo Paraguay/Uruguay en
  el dashboard (indicador de soja originada en Paraguay pero exportada por
  puertos argentinos).

Funcion publica:
- canonicalizar_shipper(raw) -> (canonico, origen_alt)
- aplicar_a_dataframe(df) -> agrega columnas shipper_canon y origen_alt
"""
from __future__ import annotations

import re
from typing import Any

import pandas as pd

# ---------------------------------------------------------------------------
# Patrones de mapeo (orden importa: primero los mas especificos)
# ---------------------------------------------------------------------------
# Cada entrada es (nombre_canonico, lista_de_regex). El primer match gana.
# Los regex corren contra el nombre crudo convertido a MAYUSCULAS.

CANONICAL_MAP: list[tuple[str, list[str]]] = [
    # Viterra + Bunge + OMHSA (Oleaginosa Moreno Hnos - nombre historico de Viterra)
    ("VITERRA-BUNGE", [
        r"\bVITERRA\b",
        r"\bBUNGE\b",
        r"\bOMHSA\b",
        r"OLEAGINOSA\s+MORENO",
        r"ORGANIZACION\s+MORENO",
        r"MORENO\s+HNOS",
        r"MORENO\s+HERMANOS",
    ]),
    ("CARGILL", [
        r"\bCARGILL\b",
    ]),
    ("COFCO", [
        r"\bCOFCO\b",
        r"NIDERA",  # Nidera fue absorbida por COFCO en 2017
    ]),
    ("LDC", [
        r"\bLDC\b",
        r"LOUIS\s+DREYFUS",
        r"\bDREYFUS\b",
    ]),
    ("ADM", [
        r"\bADM\b",
        r"ARCHER\s+DANIELS",
        r"TOEPFER",  # ADM compro Toepfer
    ]),
    ("AGD", [
        r"\bAGD\b",
        r"ACEITERA\s+GENERAL\s+DEHEZA",
        r"\bDEHEZA\b",
    ]),
    ("ACA", [
        r"\bACA\b",
        r"ASOC\.?\s+COOPERATIVAS",
        r"ASOCIACION\s+DE\s+COOPERATIVAS",
    ]),
    ("MOLINOS", [
        r"\bMOLINOS\b",  # cubre "MOLINOS AGRO" y "MOLINOS RIO DE LA PLATA"
    ]),
    ("QUILMES", [
        r"\bQUILMES\b",
        r"MALTERIA\s+QUILMES",
        r"\bMALTERIA\b",  # en contexto agro = Quilmes casi siempre
    ]),
    # Players agro-industriales de segunda linea pero relevantes
    ("GLENCORE", [
        r"\bGLENCORE\b",
    ]),
    ("OLAM", [
        r"\bOLAM\b",
    ]),
    ("PROMASA", [
        r"\bPROMASA\b",
    ]),
]

# Lista ordenada de shippers "top" para rankings en el dashboard.
SHIPPERS_TOP = [
    "VITERRA-BUNGE",
    "CARGILL",
    "COFCO",
    "LDC",
    "ADM",
    "AGD",
    "ACA",
    "MOLINOS",
    "QUILMES",
    "GLENCORE",
]

# Patron para detectar filiales regionales (Paraguay/Uruguay).
# Se corre ANTES de buscar la casa matriz. El flag se guarda aparte.
_PAT_PARAGUAY = re.compile(r"\bPY\b|\bPARAGUAY\b|\bPGY\b")
_PAT_URUGUAY = re.compile(r"\bUY\b|\bURUGUAY\b")


# ---------------------------------------------------------------------------
# Funcion principal
# ---------------------------------------------------------------------------

def canonicalizar_shipper(raw: str | None) -> tuple[str, str | None]:
    """
    Convierte un nombre bruto de shipper en (canonico, origen_alt).

    Args:
        raw: nombre bruto (ej "LDC ARGENTINA S.A." o "LDC PY").

    Returns:
        (canonico, origen_alt) donde:
          - canonico es uno de SHIPPERS_TOP o "OTROS"
          - origen_alt es "PY", "UY" o None

    Ejemplos:
        "VITERRA ARGENTINA S.A."       -> ("VITERRA-BUNGE", None)
        "OLEAGINOSA MORENO HNOS S.A."  -> ("VITERRA-BUNGE", None)
        "OMHSA"                        -> ("VITERRA-BUNGE", None)
        "LDC PY"                       -> ("LDC", "PY")
        "CARGILL UY"                   -> ("CARGILL", "UY")
        "ALGUNA EMPRESA CHICA S.A."    -> ("OTROS", None)
    """
    if not raw or not isinstance(raw, str):
        return ("OTROS", None)

    upper = raw.upper().strip()
    if not upper:
        return ("OTROS", None)

    # Detectar origen alternativo (PY/UY) ANTES del merge con casa matriz.
    origen_alt: str | None = None
    if _PAT_PARAGUAY.search(upper):
        origen_alt = "PY"
    elif _PAT_URUGUAY.search(upper):
        origen_alt = "UY"

    # Buscar la casa matriz canonica.
    for canonical, patterns in CANONICAL_MAP:
        for pat in patterns:
            if re.search(pat, upper):
                return (canonical, origen_alt)

    # No match -> cubo "OTROS" pero conservamos el flag (raro pero posible).
    return ("OTROS", origen_alt)


def aplicar_a_dataframe(
    df: pd.DataFrame,
    col_in: str = "shipper",
    col_canon: str = "shipper_canon",
    col_origen: str = "origen_alt",
) -> pd.DataFrame:
    """
    Agrega (o actualiza) las columnas normalizadas a un DataFrame.

    El DataFrame se modifica in-place y tambien se devuelve por conveniencia
    (permite usarlo en chains tipo `df = aplicar_a_dataframe(df)`).
    """
    if df.empty or col_in not in df.columns:
        df[col_canon] = None
        df[col_origen] = None
        return df

    pares = df[col_in].apply(canonicalizar_shipper)
    df[col_canon] = pares.apply(lambda p: p[0])
    df[col_origen] = pares.apply(lambda p: p[1])
    return df


# ---------------------------------------------------------------------------
# Test rapido (correr `python shipper_norm.py` para verificar)
# ---------------------------------------------------------------------------

def _self_test() -> None:
    casos: list[tuple[str, str, str | None]] = [
        ("VITERRA ARGENTINA S.A.", "VITERRA-BUNGE", None),
        ("BUNGE ARGENTINA S.A.", "VITERRA-BUNGE", None),
        ("OLEAGINOSA MORENO HNOS S.A.", "VITERRA-BUNGE", None),
        ("OMHSA", "VITERRA-BUNGE", None),
        ("ORGANIZACION MORENO", "VITERRA-BUNGE", None),
        ("CARGILL", "CARGILL", None),
        ("CARGILL S.A.C.I.", "CARGILL", None),
        ("CARGILL AGRO", "CARGILL", None),
        ("CARGILL AMERICAS", "CARGILL", None),
        ("CARGILL UY", "CARGILL", "UY"),
        ("COFCO INTERNATIONAL ARGENTINA S.A.", "COFCO", None),
        ("COFCO UY", "COFCO", "UY"),
        ("NIDERA S.A.", "COFCO", None),
        ("LDC ARGENTINA S.A.", "LDC", None),
        ("LOUIS DREYFUS COMMODITIES", "LDC", None),
        ("LDC PY", "LDC", "PY"),
        ("BUNGE PY", "VITERRA-BUNGE", "PY"),
        ("ADM AGRO S.A.", "ADM", None),
        ("ADM PY", "ADM", "PY"),
        ("ARCHER DANIELS MIDLAND", "ADM", None),
        ("ACEITERA GENERAL DEHEZA S.A.", "AGD", None),
        ("AGD S.A.", "AGD", None),
        ("ACA - ASOC COOPERATIVAS ARGENTINAS", "ACA", None),
        ("MOLINOS AGRO", "MOLINOS", None),
        ("MOLINOS RIO DE LA PLATA", "MOLINOS", None),
        ("CERVECERIA Y MALTERIA QUILMES", "QUILMES", None),
        ("MALTERIA QUILMES S.A.", "QUILMES", None),
        ("QUILMES", "QUILMES", None),
        ("GLENCORE GRAIN", "GLENCORE", None),
        ("OLAM ARGENTINA", "OLAM", None),
        ("EMPRESA CHICA X S.A.", "OTROS", None),
        (None, "OTROS", None),
        ("", "OTROS", None),
        ("   ", "OTROS", None),
    ]
    fallos = 0
    for raw, esperado_canon, esperado_origen in casos:
        canon, origen = canonicalizar_shipper(raw)
        ok = canon == esperado_canon and origen == esperado_origen
        marca = "OK " if ok else "XX "
        if not ok:
            fallos += 1
        print(f"{marca}{raw!r:60} -> {canon!r:16} origen={origen!r}")
    print(f"\nResultado: {len(casos) - fallos}/{len(casos)} casos OK, {fallos} fallos.")


if __name__ == "__main__":
    _self_test()
