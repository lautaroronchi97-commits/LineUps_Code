"""
Calculo de campanas agricolas por producto.

Cada producto tiene su propio ano-campana:
- Soja / Harina soja / Aceite soja: 1-abr -> 31-mar
- Maiz / Sorgo:                      1-mar -> 28-feb
- Trigo / Cebada:                    1-dic -> 30-nov
- Girasol:                           1-feb -> 31-ene

Por que importa: cuando el usuario quiere ver "este ano de maiz vs los ultimos
5 anos", necesitamos alinear por DIA-DE-CAMPANA, no por fecha calendario. Si no,
el mes 1 de la campana actual se compara con el mes 1 calendario de anos
anteriores y todo queda desalineado.

Funciones publicas:
- campana_de(producto, fecha) -> "2024/25"
- dia_de_campana(producto, fecha) -> int (1 = primer dia de la campana)
- fechas_de_campana(producto, campana) -> (start, end)
- campanas_anteriores(producto, fecha_ref, n) -> list["2023/24", "2022/23", ...]
- fecha_equivalente(producto, fecha_ref, campana_dest) -> fecha del mismo dia-de-campana
"""
from __future__ import annotations

from datetime import date, timedelta

# ---------------------------------------------------------------------------
# Configuracion: mes y dia de inicio de cada campana por producto.
# La campana termina un dia antes del proximo arranque (para que cubran el ano).
# ---------------------------------------------------------------------------

CAMPANA_CONFIG: dict[str, tuple[int, int]] = {
    # Complejo soja (1-abr)
    "SBS":       (4, 1),   # Soja (soybeans)
    "SBM":       (4, 1),   # Harina soja (soybean meal)
    "SBO":       (4, 1),   # Aceite soja (soybean oil)
    "NSBO":      (4, 1),   # Non-degummed soybean oil (subtipo)
    "LECITHIN":  (4, 1),   # Lecitina (subproducto soja)
    "SHULLS":    (4, 1),   # Cascaras de soja
    # Maiz y sorgo (1-mar)
    "MAIZE":     (3, 1),
    "CORN GLTN": (3, 1),   # Corn gluten meal
    "SORGHUM":   (3, 1),
    # Trigo y cebada (1-dic)
    "WHEAT":     (12, 1),
    "MALT":      (12, 1),  # Malta (deriva de cebada)
    "BARLEY":    (12, 1),
    "WBP":       (12, 1),  # Wheat by-products
    # Girasol (1-feb)
    "SFSEED":    (2, 1),
    "SFO":       (2, 1),   # Sunflower oil
    "SFMP":      (2, 1),   # Sunflower meal/pellets
}

# Campana default para productos no mapeados (ano calendario).
_DEFAULT_INICIO: tuple[int, int] = (1, 1)


# ---------------------------------------------------------------------------
# Funciones principales
# ---------------------------------------------------------------------------

def _inicio_campana(producto: str | None) -> tuple[int, int]:
    """Devuelve (mes, dia) de inicio de campana para el producto dado."""
    if not producto:
        return _DEFAULT_INICIO
    return CAMPANA_CONFIG.get(producto.upper().strip(), _DEFAULT_INICIO)


def campana_de(producto: str | None, fecha: date) -> str:
    """
    Devuelve la campana a la que pertenece una fecha, en formato "YYYY/YY".

    Ejemplos (MAIZE, inicio 1-mar):
        fecha=2025-04-15 -> "2025/26"  (arranco 1-mar-2025)
        fecha=2025-02-15 -> "2024/25"  (arranco 1-mar-2024)
        fecha=2025-03-01 -> "2025/26"  (primer dia)
        fecha=2025-02-28 -> "2024/25"  (ultimo dia)
    """
    mes_ini, dia_ini = _inicio_campana(producto)
    if (fecha.month, fecha.day) >= (mes_ini, dia_ini):
        anio_inicio = fecha.year
    else:
        anio_inicio = fecha.year - 1
    return f"{anio_inicio}/{str(anio_inicio + 1)[-2:]}"


def fechas_de_campana(producto: str | None, campana: str) -> tuple[date, date]:
    """
    Devuelve (start, end) de una campana "YYYY/YY".

    Ejemplo: fechas_de_campana("MAIZE", "2024/25") -> (2024-03-01, 2025-02-28)
    """
    mes_ini, dia_ini = _inicio_campana(producto)
    anio_inicio = int(campana.split("/")[0])
    inicio = date(anio_inicio, mes_ini, dia_ini)
    # End = start del proximo periodo - 1 dia. Esto maneja anos bisiestos OK.
    fin = date(anio_inicio + 1, mes_ini, dia_ini) - timedelta(days=1)
    return (inicio, fin)


def dia_de_campana(producto: str | None, fecha: date) -> int:
    """
    Numero de dia transcurrido dentro de la campana (1-indexed).

    Util para alinear campanas diferentes en el mismo eje temporal:
    "dia 45 de la campana 2025/26 de maiz" vs "dia 45 de la 2024/25".

    Ejemplos (MAIZE):
        fecha=2025-03-01 -> 1   (primer dia)
        fecha=2025-03-15 -> 15
        fecha=2026-02-28 -> 365 (ultimo dia en ano no bisiesto)
    """
    camp = campana_de(producto, fecha)
    inicio, _ = fechas_de_campana(producto, camp)
    return (fecha - inicio).days + 1


def campanas_anteriores(
    producto: str | None,
    fecha_ref: date,
    n: int = 5,
    incluir_actual: bool = False,
) -> list[str]:
    """
    Devuelve las ultimas N campanas anteriores a la que contiene fecha_ref.

    Args:
        producto: codigo de producto (ej "MAIZE")
        fecha_ref: fecha cuya campana se usa como ancla
        n: cantidad de campanas anteriores a devolver
        incluir_actual: si True, la primera es la campana de fecha_ref

    Returns:
        Lista de strings "YYYY/YY", de mas reciente a mas antigua.
        Ejemplo: ["2024/25", "2023/24", "2022/23", "2021/22", "2020/21"]
    """
    actual = campana_de(producto, fecha_ref)
    anio_actual = int(actual.split("/")[0])
    offset_inicio = 0 if incluir_actual else 1
    anios = range(anio_actual - offset_inicio, anio_actual - offset_inicio - n, -1)
    return [f"{a}/{str(a + 1)[-2:]}" for a in anios]


def fecha_equivalente(
    producto: str | None,
    fecha_ref: date,
    campana_dest: str,
) -> date:
    """
    Convierte una fecha a su fecha-equivalente en otra campana.

    "Mismo dia-de-campana pero en la campana destino". Util para preguntas tipo
    "que paso el dia-45-de-campana del 2020/21 de maiz?".

    Args:
        producto: codigo de producto
        fecha_ref: fecha ancla
        campana_dest: campana destino "YYYY/YY"

    Returns:
        date con el mismo numero de dia-de-campana en campana_dest.

    Ejemplo:
        fecha_equivalente("MAIZE", date(2025,4,15), "2024/25")
        # dia 46 de la campana 2025/26 -> dia 46 de la 2024/25 -> 2024-04-15
    """
    dia = dia_de_campana(producto, fecha_ref)
    inicio_dest, fin_dest = fechas_de_campana(producto, campana_dest)
    fecha_calc = inicio_dest + timedelta(days=dia - 1)
    # Clamp al fin si la campana destino es mas corta (bisiesto vs no bisiesto).
    if fecha_calc > fin_dest:
        return fin_dest
    return fecha_calc


# ---------------------------------------------------------------------------
# Helper para filtrar dataframes por campana
# ---------------------------------------------------------------------------

def filtro_rango_campanas(
    producto: str | None,
    campanas: list[str],
) -> tuple[date, date]:
    """
    Dado un set de campanas para un producto, devuelve (min_start, max_end)
    para usarlo en un filtro de fecha a la DB.

    Ejemplo: filtro_rango_campanas("MAIZE", ["2020/21", "2021/22"])
             -> (2020-03-01, 2022-02-28)
    """
    if not campanas:
        raise ValueError("Lista de campanas vacia.")
    inicios = []
    fines = []
    for c in campanas:
        i, f = fechas_de_campana(producto, c)
        inicios.append(i)
        fines.append(f)
    return (min(inicios), max(fines))


# ---------------------------------------------------------------------------
# Self-test
# ---------------------------------------------------------------------------

def _self_test() -> None:
    casos_campana: list[tuple[str, date, str]] = [
        # (producto, fecha, campana_esperada)
        ("MAIZE", date(2025, 4, 15), "2025/26"),
        ("MAIZE", date(2025, 2, 15), "2024/25"),
        ("MAIZE", date(2025, 3, 1), "2025/26"),
        ("MAIZE", date(2025, 2, 28), "2024/25"),
        ("SBS", date(2026, 4, 1), "2026/27"),
        ("SBS", date(2026, 3, 31), "2025/26"),
        ("WHEAT", date(2025, 12, 1), "2025/26"),
        ("WHEAT", date(2025, 11, 30), "2024/25"),
        ("SFSEED", date(2025, 2, 1), "2025/26"),
        ("SFSEED", date(2025, 1, 31), "2024/25"),
    ]
    fallos = 0
    for prod, f, esperado in casos_campana:
        resultado = campana_de(prod, f)
        ok = resultado == esperado
        marca = "OK " if ok else "XX "
        if not ok:
            fallos += 1
        print(f"{marca}campana_de({prod}, {f}) = {resultado!r} (esperaba {esperado!r})")

    print()

    # Test fechas_de_campana
    casos_fechas: list[tuple[str, str, date, date]] = [
        ("MAIZE", "2024/25", date(2024, 3, 1), date(2025, 2, 28)),
        ("SBS", "2024/25", date(2024, 4, 1), date(2025, 3, 31)),
        ("WHEAT", "2024/25", date(2024, 12, 1), date(2025, 11, 30)),
        ("SFSEED", "2024/25", date(2024, 2, 1), date(2025, 1, 31)),
    ]
    for prod, camp, e_ini, e_fin in casos_fechas:
        ini, fin = fechas_de_campana(prod, camp)
        ok = ini == e_ini and fin == e_fin
        marca = "OK " if ok else "XX "
        if not ok:
            fallos += 1
        print(f"{marca}fechas_de_campana({prod}, {camp}) = ({ini}, {fin})")

    print()

    # Test dia_de_campana
    casos_dia: list[tuple[str, date, int]] = [
        ("MAIZE", date(2025, 3, 1), 1),
        ("MAIZE", date(2025, 3, 15), 15),
        ("SBS", date(2025, 4, 1), 1),
        ("SBS", date(2025, 4, 30), 30),
    ]
    for prod, f, e_dia in casos_dia:
        d = dia_de_campana(prod, f)
        ok = d == e_dia
        marca = "OK " if ok else "XX "
        if not ok:
            fallos += 1
        print(f"{marca}dia_de_campana({prod}, {f}) = {d} (esperaba {e_dia})")

    print()

    # Test campanas_anteriores
    # fecha 2026-04-15 cae en campana 2026/27 (arranca 1-mar-2026),
    # entonces las 5 anteriores son 2025/26 hasta 2021/22.
    ultimas_5 = campanas_anteriores("MAIZE", date(2026, 4, 15), n=5)
    esperado_5 = ["2025/26", "2024/25", "2023/24", "2022/23", "2021/22"]
    ok = ultimas_5 == esperado_5
    marca = "OK " if ok else "XX "
    if not ok:
        fallos += 1
    print(f"{marca}campanas_anteriores(MAIZE, 2026-04-15, n=5) = {ultimas_5}")
    print(f"    esperaba {esperado_5}")

    print()

    # Test fecha_equivalente
    feq = fecha_equivalente("MAIZE", date(2025, 4, 15), "2024/25")
    esperado = date(2024, 4, 15)
    ok = feq == esperado
    marca = "OK " if ok else "XX "
    if not ok:
        fallos += 1
    print(f"{marca}fecha_equivalente(MAIZE, 2025-04-15, 2024/25) = {feq} (esperaba {esperado})")

    total = len(casos_campana) + len(casos_fechas) + len(casos_dia) + 2
    print(f"\nResultado: {total - fallos}/{total} OK, {fallos} fallos.")


if __name__ == "__main__":
    _self_test()
