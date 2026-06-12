"""
estacional.py — Motor de percentiles estacionales para la pestaña MESA.

Problema que resuelve
---------------------
Para decir si un producto está "caliente" hoy, no sirve un umbral absoluto: un
gap de 300.000 tn puede ser altísimo en agosto y normal en plena cosecha. Lo
que importa es cómo se compara el valor de HOY contra la MISMA época de las
campañas anteriores.

Este módulo toma una serie histórica de una métrica (gap de cobertura, tonelaje
de line-up, avance de ventas...) y devuelve el percentil 0-100 del valor actual
dentro de su ventana estacional: "el gap de maíz de hoy está en el percentil 92
de lo visto en esta época en las últimas 5 campañas" → muy caliente.

Alineación por SEMANA DE CAMPAÑA, no calendario: usa `campanas.py` para que el
día-de-campaña actual se compare con el mismo día-de-campaña de años previos
(la soja arranca 1-abr, el maíz 1-mar, etc.).

Módulo PURO: recibe un DataFrame de serie histórica y devuelve floats/None. Sin
red ni DB. Importarlo no tiene efectos secundarios.
"""
from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

import campanas

# Ventana de días alrededor de la fecha-equivalente que se considera "la misma
# época" (±15 días → ventana de 31 días por campaña).
VENTANA_ESTACIONAL_DIAS = 15

# Cantidad de campañas previas a mirar y mínimo para emitir un percentil.
CAMPANAS_HISTORIA = 5
MIN_CAMPANAS = 2


# ---------------------------------------------------------------------------
# 1. Fechas estacionales a muestrear
# ---------------------------------------------------------------------------

def fechas_estacionales(
    producto: str,
    fecha_actual: date,
    ventana_dias: int = VENTANA_ESTACIONAL_DIAS,
    n_campanas: int = CAMPANAS_HISTORIA,
) -> list[tuple[str, date, date]]:
    """
    Devuelve, para cada campaña previa, la ventana de fechas equivalente.

    Para el día-de-campaña de `fecha_actual`, calcula la fecha-equivalente en
    cada una de las `n_campanas` campañas anteriores y arma una ventana
    [equiv − ventana_dias, equiv + ventana_dias].

    Returns:
        Lista de tuplas (campana, fecha_desde, fecha_hasta), una por campaña
        previa, de más reciente a más antigua.
    """
    previas = campanas.campanas_anteriores(producto, fecha_actual, n=n_campanas)
    salida: list[tuple[str, date, date]] = []
    for camp in previas:
        equiv = campanas.fecha_equivalente(producto, fecha_actual, camp)
        desde = equiv - timedelta(days=ventana_dias)
        hasta = equiv + timedelta(days=ventana_dias)
        salida.append((camp, desde, hasta))
    return salida


# ---------------------------------------------------------------------------
# 2. Percentil de un valor dentro de una lista
# ---------------------------------------------------------------------------

def percentil_en_serie(valores: list[float], valor_actual: float) -> float:
    """
    Percentil (0-100) de `valor_actual` dentro de `valores` (rango débil).

    pct = 100 × (#valores ≤ valor_actual) / total. Un valor mayor o igual a
    todos da 100; menor a todos da un número bajo (>0 si empata con el mínimo).

    Asume `valores` no vacío (el llamador controla el mínimo de historia).
    """
    n = len(valores)
    if n == 0:
        return float("nan")
    menores_iguales = sum(1 for v in valores if v <= valor_actual)
    return 100.0 * menores_iguales / n


# ---------------------------------------------------------------------------
# 3. Percentil estacional sobre una serie histórica
# ---------------------------------------------------------------------------

def percentil_estacional(
    serie_hist: pd.DataFrame,
    producto: str,
    fecha_actual: date,
    valor_actual: float | None,
    ventana_dias: int = VENTANA_ESTACIONAL_DIAS,
    n_campanas: int = CAMPANAS_HISTORIA,
    min_campanas: int = MIN_CAMPANAS,
) -> float | None:
    """
    Percentil estacional 0-100 del valor actual contra su historia.

    Args:
        serie_hist: DataFrame con columnas `fecha` (date), `codigo_interno`
            (str) y `valor` (float). Es la serie histórica de la métrica.
        producto: codigo_interno del producto (ej "MAIZE").
        fecha_actual: fecha de referencia (hoy).
        valor_actual: valor de la métrica hoy. Si es None → devuelve None.
        ventana_dias: ±días alrededor de la fecha-equivalente.
        n_campanas: campañas previas a considerar.
        min_campanas: mínimo de campañas con dato para emitir percentil.

    Returns:
        Percentil 0-100, o None si no hay historia suficiente (menos de
        `min_campanas` campañas con datos en la ventana, o valor_actual None).
    """
    if valor_actual is None or pd.isna(valor_actual):
        return None
    if serie_hist is None or serie_hist.empty:
        return None
    if not {"fecha", "codigo_interno", "valor"}.issubset(serie_hist.columns):
        return None

    df = serie_hist[serie_hist["codigo_interno"] == producto].copy()
    if df.empty:
        return None

    # Normalizar fecha a date.
    fechas = pd.to_datetime(df["fecha"], errors="coerce").dt.date
    df = df.assign(_fecha=fechas).dropna(subset=["_fecha"])
    if df.empty:
        return None

    ventanas = fechas_estacionales(producto, fecha_actual, ventana_dias, n_campanas)

    valores: list[float] = []
    campanas_con_dato = 0
    for _camp, desde, hasta in ventanas:
        mask = (df["_fecha"] >= desde) & (df["_fecha"] <= hasta)
        sub = df.loc[mask, "valor"].dropna()
        if not sub.empty:
            campanas_con_dato += 1
            valores.extend(float(v) for v in sub.tolist())

    if campanas_con_dato < min_campanas or not valores:
        return None

    return percentil_en_serie(valores, float(valor_actual))


# ---------------------------------------------------------------------------
# 4. Helper: construir serie histórica desde snapshots
# ---------------------------------------------------------------------------

def construir_serie(
    registros: list[tuple[date, str, float]],
) -> pd.DataFrame:
    """
    Arma el DataFrame de serie histórica desde una lista de (fecha, codigo, valor).

    Conveniencia para los llamadores (dashboard): normaliza tipos y devuelve el
    esquema que espera `percentil_estacional`.

    Returns:
        DataFrame con columnas fecha, codigo_interno, valor. Vacío si no hay
        registros.
    """
    if not registros:
        return pd.DataFrame(columns=["fecha", "codigo_interno", "valor"])
    df = pd.DataFrame(registros, columns=["fecha", "codigo_interno", "valor"])
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
    return df
