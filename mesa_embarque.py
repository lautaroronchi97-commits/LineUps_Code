"""
mesa_embarque.py — Presión por mes de embarque (alineada a posiciones A3).

Traduce el gap de cobertura a una matriz producto × mes calendario de embarque,
para que la mesa lo lea en el idioma de sus posiciones A3 ("maíz julio caliente,
trigo se calienta en octubre") y lo cruce contra los spreads de su Excel.

Para cada producto y cada uno de los próximos meses:
    declarado_m = DJVE cuya ventana de embarque solapa el mes m
    originado_m = line-up con ETB dentro del mes m
    gap_m       = declarado_m − originado_m
    n_buques_m  = buques con ETB en el mes (indicador de densidad del dato)

El percentil estacional por mes (vs el mismo mes de años previos) lo calcula el
llamador con `estacional.py`; este módulo entrega los valores crudos por mes.

Módulo PURO. Reutiliza el equivalente poroto de `mesa_calor` para SOJA_CRUSH.
"""
from __future__ import annotations

from datetime import date

import pandas as pd

import mesa_calor

CODIGOS_CRUSH = mesa_calor.CODIGOS_CRUSH


# ---------------------------------------------------------------------------
# 1. Próximos meses de embarque
# ---------------------------------------------------------------------------

def meses_proximos(fecha: date, n: int = 6) -> list[tuple[int, int]]:
    """
    Devuelve los próximos `n` meses calendario desde `fecha` (incluido el actual).

    Returns: lista de (anio, mes), ej [(2026,6),(2026,7),...,(2026,11)].
    """
    salida: list[tuple[int, int]] = []
    anio, mes = fecha.year, fecha.month
    for _ in range(n):
        salida.append((anio, mes))
        mes += 1
        if mes > 12:
            mes = 1
            anio += 1
    return salida


def _limites_mes(anio: int, mes: int) -> tuple[date, date]:
    """Devuelve (primer_dia, ultimo_dia) del mes."""
    primero = date(anio, mes, 1)
    if mes == 12:
        ultimo = date(anio, 12, 31)
    else:
        ultimo = date(anio + (1 if mes == 12 else 0), (mes % 12) + 1, 1)
        ultimo = date(ultimo.year, ultimo.month, 1)
        from datetime import timedelta
        ultimo = ultimo - timedelta(days=1)
    return primero, ultimo


# ---------------------------------------------------------------------------
# 2. Gap por mes para un producto
# ---------------------------------------------------------------------------

def _declarado_mes(df_djve: pd.DataFrame, codigos: list[str],
                   m_ini: date, m_fin: date) -> float:
    """Suma DJVE cuya ventana de embarque solapa [m_ini, m_fin]."""
    if df_djve.empty:
        return 0.0
    df = df_djve.copy()
    if "codigo_interno" in df.columns:
        df = df[df["codigo_interno"].isin(codigos)]
    if df.empty:
        return 0.0
    ini = pd.to_datetime(df["fecha_inicio_embarque"], errors="coerce").dt.date
    fin = pd.to_datetime(df["fecha_fin_embarque"], errors="coerce").dt.date
    ini = ini.fillna(fin)
    fin = fin.fillna(ini)
    mask = ini.notna() & fin.notna() & (ini <= m_fin) & (fin >= m_ini)
    return float(df.loc[mask, "toneladas"].sum())


def _originado_mes(df_lineup: pd.DataFrame, codigos: list[str],
                   m_ini: date, m_fin: date) -> tuple[float, int]:
    """Suma quantity y cuenta buques con ETB en [m_ini, m_fin]."""
    if df_lineup.empty:
        return 0.0, 0
    df = df_lineup.copy()
    df = df[df["cargo"].isin(codigos)]
    if df.empty:
        return 0.0, 0
    etb = pd.to_datetime(df["etb"], errors="coerce").dt.date
    mask = etb.notna() & (etb >= m_ini) & (etb <= m_fin)
    sub = df[mask].copy()
    if sub.empty:
        return 0.0, 0
    sub["quantity"] = pd.to_numeric(sub["quantity"], errors="coerce").fillna(0)
    return float(sub["quantity"].sum()), int(len(sub))


def gap_por_mes(
    df_djve: pd.DataFrame,
    df_lineup: pd.DataFrame,
    fecha: date,
    producto: str,
    n_meses: int = 6,
) -> pd.DataFrame:
    """
    Matriz de gap por mes de embarque para un producto.

    Para "SOJA_CRUSH" agrega SBM+SBO en equivalente poroto (declarado y
    originado). Para el resto usa el código directo.

    Returns:
        DataFrame con columnas anio, mes, declarado_tn, originado_tn, gap_tn,
        n_buques — una fila por mes próximo.
    """
    if producto == "SOJA_CRUSH":
        codigos = list(CODIGOS_CRUSH)
    else:
        codigos = [producto]

    filas = []
    for anio, mes in meses_proximos(fecha, n_meses):
        m_ini, m_fin = _limites_mes(anio, mes)

        if producto == "SOJA_CRUSH":
            decl_h = _declarado_mes(df_djve, ["SBM"], m_ini, m_fin)
            decl_a = _declarado_mes(df_djve, ["SBO"], m_ini, m_fin)
            declarado = mesa_calor.equivalente_poroto(decl_h, decl_a)
            orig_h, bq_h = _originado_mes(df_lineup, ["SBM"], m_ini, m_fin)
            orig_a, bq_a = _originado_mes(df_lineup, ["SBO"], m_ini, m_fin)
            originado = mesa_calor.equivalente_poroto(orig_h, orig_a)
            n_buques = bq_h + bq_a
        else:
            declarado = _declarado_mes(df_djve, codigos, m_ini, m_fin)
            originado, n_buques = _originado_mes(df_lineup, codigos, m_ini, m_fin)

        filas.append({
            "anio": anio,
            "mes": mes,
            "declarado_tn": declarado,
            "originado_tn": originado,
            "gap_tn": declarado - originado,
            "n_buques": n_buques,
        })

    return pd.DataFrame(filas)
