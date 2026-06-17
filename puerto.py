"""
puerto.py — Estado de las zonas portuarias para el trading desk.

Responde dos preguntas del vendedor FAS que ninguna otra vista cerraba:

  Q5 "¿No hay barcos próximamente?" — SEQUÍA DE DEMANDA. Cuántos buques tienen
      ETB en los próximos N días por zona. Si una zona se queda sin buques que
      vienen a cargar, no hay demanda inminente → mala señal para vender.

  Q6 "¿El puerto está sobrepoblado o no hay barcos?" — CONGESTIÓN ACTUAL vs su
      propia historia. Cuántos buques simultáneos hay hoy por zona y en qué
      percentil cae respecto de los últimos ~90 días. SOBREPOBLADO = mucha
      actividad (demanda firme, pero riesgo de demoras/sobreestadías).
      VACÍO = poca actividad (demanda floja).

Las dos miradas son complementarias: la congestión (Q6) es el estado FÍSICO de
hoy (buques atracados/operando: etb<=hoy<=ets); la sequía (Q5) es el FLUJO que
viene (buques con etb en el futuro cercano).

Módulo PURO: recibe DataFrames, devuelve DataFrames/dicts. Sin red ni DB.
Importarlo no tiene efectos secundarios.
"""
from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from estacional import percentil_en_serie

# Zonas que el trading desk opera (orden de exposición en la web).
ZONAS_OPERATIVAS = [
    "Up River Norte",
    "Up River Sur",
    "Bahia Blanca",
    "Necochea/Quequen",
]

# Umbrales de congestión sobre el percentil de la ventana histórica.
PCTL_SOBREPOBLADO = 80.0   # >= p80 de los últimos 90d → SOBREPOBLADO
PCTL_VACIO = 20.0          # <= p20 → VACÍO

# Horizonte por defecto para la sequía de buques (días hacia adelante).
HORIZONTE_SEQUIA_DIAS = 7

# Mínimo de días con dato en la ventana para emitir un percentil de congestión.
MIN_DIAS_HISTORIA = 10


# ---------------------------------------------------------------------------
# Q6 — Congestión actual vs historia
# ---------------------------------------------------------------------------

def congestion_por_zona(
    df_serie: pd.DataFrame,
    fecha_ref: date,
    ventana_dias: int = 90,
) -> pd.DataFrame:
    """
    Estado de congestión de cada zona: buques de hoy vs su distribución reciente.

    Args:
        df_serie: DataFrame con columnas `fecha` (date), `zona` (str),
            `buques` (int) — buques simultáneos en puerto por día/zona
            (etb<=día<=ets). Es la salida de la serie de congestión del
            dashboard, calculada sobre una ventana que incluya `fecha_ref`.
        fecha_ref: día de referencia (hoy).
        ventana_dias: días hacia atrás para construir la distribución histórica.

    Returns:
        DataFrame con una fila por zona operativa:
          zona, buques_hoy, mediana_hist, max_hist, percentil, estado.
        `estado` ∈ {"SOBREPOBLADO", "NORMAL", "VACIO", "SIN HISTORIA"}.
        Si no hay datos, devuelve una fila por zona con buques_hoy=0 y
        estado="SIN HISTORIA".
    """
    cols = ["zona", "buques_hoy", "mediana_hist", "max_hist", "percentil", "estado"]

    desde = fecha_ref - timedelta(days=ventana_dias)
    if df_serie is None or df_serie.empty:
        base = df_serie if df_serie is not None else pd.DataFrame()
    else:
        f = pd.to_datetime(df_serie["fecha"], errors="coerce").dt.date
        base = df_serie.assign(_f=f)
        base = base[(base["_f"] >= desde) & (base["_f"] <= fecha_ref)]

    filas = []
    for zona in ZONAS_OPERATIVAS:
        if base is None or base.empty:
            sub = pd.DataFrame(columns=["_f", "buques"])
        else:
            sub = base[base["zona"] == zona]

        hoy_rows = sub[sub["_f"] == fecha_ref] if not sub.empty else sub
        buques_hoy = int(hoy_rows["buques"].max()) if not hoy_rows.empty else 0

        serie_hist = (
            [float(v) for v in sub["buques"].dropna().tolist()]
            if not sub.empty else []
        )
        # Días distintos con dato (no filas) para el mínimo de historia.
        dias_con_dato = sub["_f"].nunique() if not sub.empty else 0

        if dias_con_dato < MIN_DIAS_HISTORIA or not serie_hist:
            filas.append({
                "zona": zona, "buques_hoy": buques_hoy,
                "mediana_hist": float("nan"), "max_hist": float("nan"),
                "percentil": None, "estado": "SIN HISTORIA",
            })
            continue

        pctl = percentil_en_serie(serie_hist, float(buques_hoy))
        mediana = float(pd.Series(serie_hist).median())
        maximo = float(max(serie_hist))

        if pctl >= PCTL_SOBREPOBLADO:
            estado = "SOBREPOBLADO"
        elif pctl <= PCTL_VACIO:
            estado = "VACIO"
        else:
            estado = "NORMAL"

        filas.append({
            "zona": zona, "buques_hoy": buques_hoy,
            "mediana_hist": mediana, "max_hist": maximo,
            "percentil": pctl, "estado": estado,
        })

    return pd.DataFrame(filas, columns=cols)


# ---------------------------------------------------------------------------
# Q5 — Sequía de buques (flujo que viene)
# ---------------------------------------------------------------------------

def sequia_buques_por_zona(
    df_lineup_hoy: pd.DataFrame,
    fecha_ref: date,
    horizonte_dias: int = HORIZONTE_SEQUIA_DIAS,
) -> pd.DataFrame:
    """
    Buques que vienen a cargar por zona en los próximos `horizonte_dias`.

    Args:
        df_lineup_hoy: snapshot del line-up de hoy. Debe tener columnas
            `zona`, `etb`, `vessel`, `quantity` y opcionalmente `cargo`.
        fecha_ref: día de referencia (hoy).
        horizonte_dias: ventana hacia adelante (default 7).

    Returns:
        DataFrame con una fila por zona operativa:
          zona, n_buques, tons, productos (lista ordenada), sin_barcos (bool).
        `sin_barcos` es True cuando no hay ningún buque con ETB en la ventana
        → no viene demanda inminente a esa zona.
    """
    cols = ["zona", "n_buques", "tons", "productos", "sin_barcos"]

    if df_lineup_hoy is not None and not df_lineup_hoy.empty:
        df = df_lineup_hoy.copy()
        etb = pd.to_datetime(df["etb"], errors="coerce").dt.date
        fin = fecha_ref + timedelta(days=horizonte_dias)
        df = df[etb.notna() & (etb >= fecha_ref) & (etb <= fin)]
        if "quantity" in df.columns:
            df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0)
    else:
        df = pd.DataFrame(columns=["zona", "vessel", "quantity", "cargo"])

    filas = []
    for zona in ZONAS_OPERATIVAS:
        sub = df[df["zona"] == zona] if not df.empty else df
        n_buques = int(sub["vessel"].nunique()) if not sub.empty else 0
        tons = float(sub["quantity"].sum()) if not sub.empty and "quantity" in sub else 0.0
        if not sub.empty and "cargo" in sub.columns:
            productos = sorted({str(c) for c in sub["cargo"].dropna().unique()})
        else:
            productos = []
        filas.append({
            "zona": zona, "n_buques": n_buques, "tons": tons,
            "productos": productos, "sin_barcos": n_buques == 0,
        })

    return pd.DataFrame(filas, columns=cols)


# ---------------------------------------------------------------------------
# Helpers de presentación (emoji/etiqueta) — usados por la UI, testeable.
# ---------------------------------------------------------------------------

_ESTADO_EMOJI = {
    "SOBREPOBLADO": "🔴",
    "NORMAL": "🟢",
    "VACIO": "⚪",
    "SIN HISTORIA": "·",
}


def emoji_estado(estado: str) -> str:
    """Emoji del estado de congestión para la UI."""
    return _ESTADO_EMOJI.get(estado, "·")
