"""
mesa_calor.py — Índice de calor de mercadería para la pestaña MESA.

Qué hace
--------
Convierte la presión física del mercado (gap de cobertura DJVE−line-up,
densidad del line-up, ritmo de farmer selling) en un único ÍNDICE DE CALOR
0-100 por producto, más su DIRECCIÓN (abriéndose / estable / cerrándose), y
combina ambos en una ACCIÓN sugerida para la mesa (DIFERIR / VENDER YA / ...).

La pregunta que responde: ¿qué producto está CALIENTE (la exportación necesita
mercadería → se puede sobrepagar → diferir) y cuál PESADO (cubiertos → no va a
haber interés → vender ya o comprar barato)?

Fórmula del índice
------------------
    CALOR = w_gap    × pctl(gap_cobertura_30d)
          + w_lineup × pctl(tonelaje_lineup_30d)
          + w_farmer × (100 − pctl(avance_ventas))

Demanda (w_gap + w_lineup = 0.65) domina sobre oferta (w_farmer = 0.35). Los
percentiles son estacionales (`estacional.py`); cada componente que falte se
omite y los pesos se renormalizan sobre los componentes disponibles.

Módulo PURO: recibe DataFrames/valores, devuelve dicts/DataFrames. Sin red ni
DB. Reutiliza `cobertura.py` para los balances. Importarlo no tiene efectos.
"""
from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

import cobertura

# ---------------------------------------------------------------------------
# Parametría (editable — la mesa recalibra desde acá; la UI la muestra)
# ---------------------------------------------------------------------------

# Pesos del índice de calor. Demanda dominante (gap + lineup = 0.65).
W_GAP = 0.35
W_LINEUP = 0.30
W_FARMER = 0.35

# Horizonte de análisis del índice (días hacia adelante).
HORIZONTE_CALOR_DIAS = 30

# Momentum: días hacia atrás para medir la dirección del gap.
K_MOMENTUM_DIAS = 10
# Umbral de movimiento del gap para declarar dirección (media Panamax).
UMBRAL_DIRECCION_TN = 32_500.0

# Rendimientos industriales del crush de soja (1 tn poroto → harina/aceite).
RINDE_HARINA = 0.745
RINDE_ACEITE = 0.19

# Cortes de banda del índice de calor.
BANDA_CALIENTE = 80
BANDA_FIRME = 60
BANDA_NEUTRO = 40
BANDA_PESADO = 20

# Productos que la mesa opera (el resto se ignora en MESA).
PRODUCTOS_MESA = ["MAIZE", "WHEAT", "SOJA_CRUSH", "SBS"]

# Códigos del complejo soja para el equivalente poroto (crush).
CODIGOS_CRUSH = ("SBM", "SBO")

# Etiqueta de producto para display.
PRODUCTO_DISPLAY_MESA = {
    "MAIZE": "Maíz",
    "WHEAT": "Trigo",
    "SOJA_CRUSH": "Soja (crush)",
    "SBS": "Soja poroto",
}


# ---------------------------------------------------------------------------
# 1. Bandas y direcciones
# ---------------------------------------------------------------------------

def clasificar_banda(calor: float | None) -> str:
    """
    Mapea el índice 0-100 a su banda de calor.

    Returns: "CALIENTE" (≥80), "FIRME" (60-80), "NEUTRO" (40-60),
    "PESADO" (20-40), "MUY PESADO" (<20), o "SIN HISTORIA" si calor es None.
    """
    if calor is None or pd.isna(calor):
        return "SIN HISTORIA"
    if calor >= BANDA_CALIENTE:
        return "CALIENTE"
    if calor >= BANDA_FIRME:
        return "FIRME"
    if calor >= BANDA_NEUTRO:
        return "NEUTRO"
    if calor >= BANDA_PESADO:
        return "PESADO"
    return "MUY PESADO"


def clasificar_direccion(delta_gap: float | None,
                         umbral: float = UMBRAL_DIRECCION_TN) -> str:
    """
    Mapea el movimiento del gap a su dirección.

    delta_gap = gap(hoy) − gap(hoy − K). Returns:
      "ABRIENDOSE" si delta ≥ +umbral (el gap crece → se calienta),
      "CERRANDOSE" si delta ≤ −umbral (se cubren → se enfría),
      "ESTABLE"    en el resto. "SIN DATO" si delta_gap es None.
    """
    if delta_gap is None or pd.isna(delta_gap):
        return "SIN DATO"
    if delta_gap >= umbral:
        return "ABRIENDOSE"
    if delta_gap <= -umbral:
        return "CERRANDOSE"
    return "ESTABLE"


# Matriz de acción sugerida: (banda, dirección) → (acción, explicación).
# Las bandas se colapsan en CALIENTE / NEUTRO / PESADO para la matriz.
_MATRIZ_ACCION: dict[tuple[str, str], tuple[str, str]] = {
    ("CALIENTE", "ABRIENDOSE"): ("DIFERIR", "el premio va a mejorar"),
    ("CALIENTE", "ESTABLE"): ("VENDER SELECTIVO", "vender al más corto"),
    ("CALIENTE", "CERRANDOSE"): ("VENDER YA", "se están cubriendo, el premio se desinfla"),
    ("NEUTRO", "ABRIENDOSE"): ("ATENCIÓN", "calentándose"),
    ("NEUTRO", "ESTABLE"): ("SIN SEÑAL", "sin presión clara"),
    ("NEUTRO", "CERRANDOSE"): ("SIN APURO", "demanda relajándose"),
    ("PESADO", "ABRIENDOSE"): ("VIGILAR", "posible giro"),
    ("PESADO", "ESTABLE"): ("NO ESPERAR BID", "no va a haber interés"),
    ("PESADO", "CERRANDOSE"): ("COMPRAR BARATO", "productor presionado"),
}


def _banda_a_nivel(banda: str) -> str:
    """Colapsa las 5 bandas en los 3 niveles de la matriz de acción."""
    if banda in ("CALIENTE", "FIRME"):
        return "CALIENTE"
    if banda in ("PESADO", "MUY PESADO"):
        return "PESADO"
    if banda == "NEUTRO":
        return "NEUTRO"
    return "SIN HISTORIA"


def accion_sugerida(banda: str, direccion: str) -> tuple[str, str]:
    """
    Devuelve (acción, explicación) según banda de calor y dirección.

    Aplica la matriz nivel×dirección de la especificación. Si no hay historia
    o dirección → ("—", "sin datos suficientes").
    """
    nivel = _banda_a_nivel(banda)
    if nivel == "SIN HISTORIA" or direccion == "SIN DATO":
        return ("—", "sin datos suficientes")
    # Dirección estable como fallback si la dirección no está en la matriz.
    return _MATRIZ_ACCION.get((nivel, direccion),
                              _MATRIZ_ACCION.get((nivel, "ESTABLE"), ("—", "")))


# ---------------------------------------------------------------------------
# 2. Equivalente poroto (crush de soja)
# ---------------------------------------------------------------------------

def equivalente_poroto(
    tn_harina: float,
    tn_aceite: float,
    rinde_harina: float = RINDE_HARINA,
    rinde_aceite: float = RINDE_ACEITE,
) -> float:
    """
    Convierte tonelaje de harina (SBM) y aceite (SBO) a equivalente poroto.

    poroto_eq = tn_harina / rinde_harina + tn_aceite / rinde_aceite

    Es la cantidad de poroto calidad FÁBRICA que las plantas necesitan originar
    para producir esos derivados — la demanda real de soja de la industria, que
    es el comprador natural de la soja FAS de la mesa.
    """
    eq = 0.0
    if rinde_harina > 0:
        eq += float(tn_harina) / rinde_harina
    if rinde_aceite > 0:
        eq += float(tn_aceite) / rinde_aceite
    return eq


# ---------------------------------------------------------------------------
# 3. Componentes del índice en una fecha (as-of)
# ---------------------------------------------------------------------------

def gap_cobertura(
    df_djve: pd.DataFrame,
    df_lineup: pd.DataFrame,
    fecha: date,
    producto: str,
    horizonte_dias: int = HORIZONTE_CALOR_DIAS,
) -> float:
    """
    Gap de cobertura (declarado − originado) de un producto en una fecha.

    Para "SOJA_CRUSH" agrega SBM+SBO convertidos a equivalente poroto, tanto en
    el declarado (DJVE) como en el originado (line-up). Para el resto usa el
    balance directo de `cobertura.balance_por_producto`.

    Returns: gap en toneladas (puede ser negativo si está sobre-originado).
    """
    balance = cobertura.balance_por_producto(
        df_djve, df_lineup, fecha, horizonte_dias
    )
    if balance.empty:
        return 0.0

    if producto == "SOJA_CRUSH":
        sub = balance[balance["codigo_interno"].isin(CODIGOS_CRUSH)]
        if sub.empty:
            return 0.0
        decl_h = float(sub[sub["codigo_interno"] == "SBM"]["declarado_tn"].sum())
        decl_a = float(sub[sub["codigo_interno"] == "SBO"]["declarado_tn"].sum())
        orig_h = float(sub[sub["codigo_interno"] == "SBM"]["originado_tn"].sum())
        orig_a = float(sub[sub["codigo_interno"] == "SBO"]["originado_tn"].sum())
        declarado = equivalente_poroto(decl_h, decl_a)
        originado = equivalente_poroto(orig_h, orig_a)
        return declarado - originado

    fila = balance[balance["codigo_interno"] == producto]
    if fila.empty:
        return 0.0
    return float(fila.iloc[0]["falta_cubrir_tn"])


def tonelaje_lineup(
    df_lineup: pd.DataFrame,
    fecha: date,
    producto: str,
    horizonte_dias: int = HORIZONTE_CALOR_DIAS,
) -> float:
    """
    Tonelaje del line-up con ETB en [fecha, fecha+horizonte] para un producto.

    "SOJA_CRUSH" agrega SBM+SBO en equivalente poroto. El resto suma el `cargo`
    directo.
    """
    lineup_h = cobertura._filtrar_lineup_por_ventana(
        df_lineup, fecha, horizonte_dias
    )
    if lineup_h.empty:
        return 0.0
    tmp = lineup_h.copy()
    tmp["quantity"] = pd.to_numeric(tmp["quantity"], errors="coerce").fillna(0)

    if producto == "SOJA_CRUSH":
        tn_h = float(tmp[tmp["cargo"] == "SBM"]["quantity"].sum())
        tn_a = float(tmp[tmp["cargo"] == "SBO"]["quantity"].sum())
        return equivalente_poroto(tn_h, tn_a)

    return float(tmp[tmp["cargo"] == producto]["quantity"].sum())


def direccion_gap(
    df_djve: pd.DataFrame,
    df_lineup_hoy: pd.DataFrame,
    df_lineup_pasado: pd.DataFrame,
    fecha_hoy: date,
    producto: str,
    k_dias: int = K_MOMENTUM_DIAS,
    horizonte_dias: int = HORIZONTE_CALOR_DIAS,
) -> float:
    """
    Movimiento del gap en los últimos `k_dias` (delta_gap para la dirección).

    delta_gap = gap_cobertura(hoy) − gap_cobertura(hoy − k).

    Args:
        df_lineup_hoy: line-up tal como se ve hoy.
        df_lineup_pasado: line-up tal como se veía hace k días (snapshot).
        Ambos se cruzan contra la misma DJVE (aprox: la DJVE histórica suele no
        estar disponible as-of, se acepta usar la actual como proxy).

    Returns: delta del gap en toneladas. El llamador lo pasa a `clasificar_direccion`.
    """
    gap_hoy = gap_cobertura(df_djve, df_lineup_hoy, fecha_hoy, producto, horizonte_dias)
    fecha_pasado = fecha_hoy - timedelta(days=k_dias)
    gap_pasado = gap_cobertura(
        df_djve, df_lineup_pasado, fecha_pasado, producto, horizonte_dias
    )
    return gap_hoy - gap_pasado


# ---------------------------------------------------------------------------
# 4. Índice de calor (combina los percentiles)
# ---------------------------------------------------------------------------

def indice_calor(
    pctl_gap: float | None,
    pctl_lineup: float | None,
    pctl_avance_ventas: float | None,
    w_gap: float = W_GAP,
    w_lineup: float = W_LINEUP,
    w_farmer: float = W_FARMER,
) -> float | None:
    """
    Combina los percentiles de los 3 componentes en el índice de calor 0-100.

    El componente de farmer selling se INVIERTE (menos avance de ventas = más
    retención del productor = más calor). Los componentes None se omiten y los
    pesos se renormalizan sobre los presentes.

    Returns: índice 0-100, o None si no hay ningún componente disponible.
    """
    componentes: list[tuple[float, float]] = []  # (valor, peso)
    if pctl_gap is not None and not pd.isna(pctl_gap):
        componentes.append((float(pctl_gap), w_gap))
    if pctl_lineup is not None and not pd.isna(pctl_lineup):
        componentes.append((float(pctl_lineup), w_lineup))
    if pctl_avance_ventas is not None and not pd.isna(pctl_avance_ventas):
        # Invertir: avance bajo → calor alto.
        componentes.append((100.0 - float(pctl_avance_ventas), w_farmer))

    if not componentes:
        return None

    peso_total = sum(w for _, w in componentes)
    if peso_total <= 0:
        return None
    return sum(v * w for v, w in componentes) / peso_total


# ---------------------------------------------------------------------------
# 5. Etiquetas legibles
# ---------------------------------------------------------------------------

BANDA_EMOJI = {
    "CALIENTE": "🔥",
    "FIRME": "",
    "NEUTRO": "",
    "PESADO": "",
    "MUY PESADO": "🧊",
    "SIN HISTORIA": "",
}

DIRECCION_GLIFO = {
    "ABRIENDOSE": "↗",
    "ESTABLE": "→",
    "CERRANDOSE": "↘",
    "SIN DATO": "·",
}

DIRECCION_LABEL = {
    "ABRIENDOSE": "ABRIÉNDOSE",
    "ESTABLE": "ESTABLE",
    "CERRANDOSE": "CERRÁNDOSE",
    "SIN DATO": "SIN DATO",
}


# ---------------------------------------------------------------------------
# 6. Sparkline SVG inline (helper de UI puro)
# ---------------------------------------------------------------------------

def sparkline_svg(
    valores: list[float],
    color_linea: str = "#6655ee",
    color_punto: str = "#ff3333",
    ancho: int = 140,
    alto: int = 28,
) -> str:
    """
    Genera un sparkline como string SVG embebible en HTML de st.markdown.

    Plotly no se puede embeber dentro de una card HTML custom, así que el
    sparkline de la serie del índice se dibuja como `<svg><polyline/></svg>`.

    Args:
        valores: serie de puntos (ej. índice de calor últimos 30 días).
        color_linea: color del trazo.
        color_punto: color del marcador del último punto.

    Returns:
        String `<svg>...</svg>`. Si hay menos de 2 puntos válidos, devuelve un
        SVG vacío del tamaño pedido (no rompe el layout).
    """
    pad = 3
    serie = [float(v) for v in valores if v is not None and not pd.isna(v)]
    if len(serie) < 2:
        return f'<svg width="{ancho}" height="{alto}"></svg>'

    vmin, vmax = min(serie), max(serie)
    rango = vmax - vmin if vmax > vmin else 1.0
    n = len(serie)
    ancho_util = ancho - 2 * pad
    alto_util = alto - 2 * pad

    puntos = []
    for i, v in enumerate(serie):
        x = pad + (ancho_util * i / (n - 1))
        # Y invertido: valor alto arriba.
        y = pad + alto_util * (1.0 - (v - vmin) / rango)
        puntos.append(f"{x:.1f},{y:.1f}")

    poly = " ".join(puntos)
    ult_x, ult_y = puntos[-1].split(",")
    return (
        f'<svg width="{ancho}" height="{alto}" '
        f'viewBox="0 0 {ancho} {alto}" xmlns="http://www.w3.org/2000/svg">'
        f'<polyline points="{poly}" fill="none" '
        f'stroke="{color_linea}" stroke-width="1.5" '
        f'stroke-linejoin="round" stroke-linecap="round"/>'
        f'<circle cx="{ult_x}" cy="{ult_y}" r="2" fill="{color_punto}"/>'
        f'</svg>'
    )
