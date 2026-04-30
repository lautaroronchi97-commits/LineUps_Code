"""
Dashboard Line-Up Puertos Argentinos - v2 (Bloomberg Terminal Redesign)

Uso local:
    streamlit run dashboard.py

Abre automaticamente http://localhost:8501

Requisitos:
- .env con credenciales Supabase (o st.secrets en Streamlit Cloud).
- Al menos 1 fecha cargada en DB (corre `python backfill.py` primero).

Arquitectura:
- 4 pestanas tematicas orientadas a la mesa de trading:
    1. Panorama  - estado general del dia vs tendencia reciente
    2. Shippers  - quien se esta moviendo (core del redesign)
    3. Productos - un producto a la vez, campana actual vs historicas
    4. Congestion - buques ahora en puerto, agrupacion por zona

- Solo exportaciones (ops=LOAD) y 8 productos prioritarios definidos en
  config.CODIGOS_PRIORITARIOS.
- Shippers canonicalizados (VITERRA+BUNGE+OMHSA unificados, filiales PY/UY
  marcadas con flag origen_alt pero agregadas a la casa matriz).
- Campanas agricolas por producto (soja abr-mar, maiz mar-feb, etc).
- Colores fijos por shipper: VITERRA-BUNGE amber, CARGILL cyan, etc.
- Tema dark Bloomberg en .streamlit/config.toml + plantilla custom plotly.
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone

# En Streamlit Cloud no hay .env: los valores vienen de st.secrets.
# Copiarlos a env vars ANTES de importar db.py (que los busca via os.getenv).
# .strip() porque hemos visto whitespace invisible en el service_role al pegar.
import streamlit as st

# Solo cargamos URL y ANON_KEY desde st.secrets.
# La SERVICE_ROLE_KEY NO debe estar en secrets del dashboard (usa solo anon_key).
# Si alguien la agrego por error, fallamos rapido con un mensaje claro.
for _nombre_secret in ("SUPABASE_URL", "SUPABASE_ANON_KEY"):
    try:
        _valor = st.secrets[_nombre_secret]
        if isinstance(_valor, str):
            os.environ[_nombre_secret] = _valor.strip()
    except (KeyError, FileNotFoundError):
        pass

try:
    if st.secrets.get("SUPABASE_SERVICE_ROLE_KEY"):
        st.error(
            "⚠️ **Configuracion incorrecta:** `SUPABASE_SERVICE_ROLE_KEY` no debe "
            "estar en los secrets del dashboard. "
            "Usa `SUPABASE_ANON_KEY` (clave de solo-lectura). "
            "Ver `.streamlit/secrets.toml.example`."
        )
        st.stop()
except FileNotFoundError:
    pass  # No hay secrets.toml en local; ok, se usa .env

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

import campanas
import clima as clima_mod
import estimaciones as estim_mod
import fob_djve
from config import (
    BLOOMBERG_PALETTE,
    CODIGOS_PRIORITARIOS,
    PRODUCTO_DISPLAY,
    PRODUCTOS_PRIORITARIOS,
    SHIPPER_COLORS,
    zona_de_puerto,
)
from db import (
    djve_ultima_actualizacion,
    ping,
    primera_fecha_cargada,
    query_djve,
    query_en_puerto_ahora,
    query_exports_prioritarios,
    query_lineup,
    ultima_actualizacion_lineup,
    ultima_fecha_cargada,
)
from shipper_norm import SHIPPERS_TOP, aplicar_a_dataframe

# ===========================================================================
# Configuracion general de la pagina
# ===========================================================================

st.set_page_config(
    page_title="Line-Up AR · Trading Desk",
    page_icon="⚓",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Plantilla plotly dark custom (coherente con .streamlit/config.toml).
PLOTLY_TEMPLATE = dict(
    layout=dict(
        paper_bgcolor=BLOOMBERG_PALETTE["bg_card"],
        plot_bgcolor=BLOOMBERG_PALETTE["bg_card"],
        font=dict(
            family="Consolas, Menlo, monospace",
            color=BLOOMBERG_PALETTE["text_primary"],
            size=12,
        ),
        xaxis=dict(
            gridcolor=BLOOMBERG_PALETTE["grid"],
            linecolor=BLOOMBERG_PALETTE["grid"],
            zerolinecolor=BLOOMBERG_PALETTE["grid"],
        ),
        yaxis=dict(
            gridcolor=BLOOMBERG_PALETTE["grid"],
            linecolor=BLOOMBERG_PALETTE["grid"],
            zerolinecolor=BLOOMBERG_PALETTE["grid"],
        ),
        legend=dict(
            bgcolor="rgba(0,0,0,0)",
            bordercolor=BLOOMBERG_PALETTE["grid"],
            borderwidth=1,
        ),
        margin=dict(l=40, r=20, t=40, b=40),
    )
)


def aplicar_tema(fig: go.Figure) -> go.Figure:
    """Aplica el tema Bloomberg a una figura plotly."""
    fig.update_layout(**PLOTLY_TEMPLATE["layout"])
    return fig


# =============================================================================
# CSS GLOBAL — estilo terminal de trading (inspirado en Fintelligentia)
# =============================================================================
_P = BLOOMBERG_PALETTE  # alias corto

st.markdown(
    f"""
    <style>
    /* ---- Fuente: Google Fonts JetBrains Mono (fallback consolas) ---------- */
    @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;600;700&display=swap');

    html, body, [class*="css"], [data-testid], .stApp {{
        font-family: 'JetBrains Mono', 'Consolas', 'Menlo', monospace !important;
    }}

    /* ---- Franja superior cobre/óxido (Fintelligentia signature) ----------- */
    .stApp::before {{
        content: '';
        display: block;
        position: fixed;
        top: 0; left: 0; right: 0;
        height: 3px;
        background: {_P["top_stripe"]};
        z-index: 9999;
    }}

    /* ---- Fondo y superficie base ----------------------------------------- */
    .stApp {{ background-color: {_P["bg_primary"]}; }}
    [data-testid="stSidebar"] {{ background-color: {_P["bg_card"]} !important; }}
    [data-testid="stSidebar"] * {{ border-color: {_P["border"]} !important; }}

    /* ---- Headings --------------------------------------------------------- */
    h1 {{
        font-size: 15px !important;
        font-weight: 700 !important;
        letter-spacing: 0.15em;
        text-transform: uppercase;
        color: {_P["text_primary"]} !important;
        border-bottom: 1px solid {_P["top_stripe"]};
        padding-bottom: 10px;
        margin-bottom: 12px;
    }}
    h2 {{
        font-size: 12px !important;
        font-weight: 600 !important;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        color: {_P["accent"]} !important;
        margin-top: 8px !important;
    }}
    h3 {{
        font-size: 11px !important;
        font-weight: 400 !important;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        color: {_P["text_muted"]} !important;
    }}

    /* ---- Divider: línea sutil --------------------------------------------- */
    hr {{
        border: none !important;
        border-top: 1px solid {_P["border"]} !important;
        margin: 16px 0 !important;
    }}

    /* ---- Caption ---------------------------------------------------------- */
    .stCaption, .stCaption p {{
        font-size: 10px !important;
        color: {_P["text_muted"]} !important;
        letter-spacing: 0.06em;
        text-transform: uppercase;
    }}

    /* ---- KPI Metric cards ------------------------------------------------- */
    [data-testid="stMetric"] {{
        background: {_P["bg_card"]};
        border: 1px solid {_P["border"]};
        border-radius: 2px;
        padding: 14px 16px 10px !important;
    }}
    [data-testid="stMetricLabel"] > div {{
        color: {_P["text_muted"]} !important;
        text-transform: uppercase;
        font-size: 9px !important;
        letter-spacing: 0.10em;
        font-weight: 600;
    }}
    [data-testid="stMetricValue"] > div {{
        color: {_P["accent"]} !important;
        font-size: 22px !important;
        font-weight: 700;
        letter-spacing: 0.02em;
    }}
    [data-testid="stMetricDelta"] > div {{
        font-size: 10px !important;
        letter-spacing: 0.04em;
    }}

    /* ---- Tabs ------------------------------------------------------------- */
    [data-baseweb="tab-list"] {{
        gap: 2px;
        background: {_P["bg_card"]} !important;
        padding: 4px;
        border-radius: 2px;
        border: 1px solid {_P["border"]};
    }}
    [data-baseweb="tab"] {{
        background: transparent !important;
        border-radius: 1px !important;
        font-size: 11px !important;
        font-weight: 600 !important;
        letter-spacing: 0.08em;
        padding: 6px 16px !important;
        color: {_P["text_muted"]} !important;
        border: none !important;
    }}
    [aria-selected="true"][data-baseweb="tab"] {{
        background: {_P["accent"]} !important;
        color: {_P["bg_primary"]} !important;
    }}
    [data-baseweb="tab"]:hover {{
        background: {_P["bg_hover"]} !important;
        color: {_P["text_primary"]} !important;
    }}

    /* ---- DataFrames: look tabla terminal ---------------------------------- */
    [data-testid="stDataFrame"] {{
        border: 1px solid {_P["border"]} !important;
        border-radius: 2px;
    }}
    [data-testid="stDataFrame"] table {{
        font-size: 11px !important;
        letter-spacing: 0.03em;
    }}
    [data-testid="stDataFrame"] th {{
        background: {_P["bg_card"]} !important;
        color: {_P["text_muted"]} !important;
        text-transform: uppercase;
        font-size: 9px !important;
        letter-spacing: 0.08em;
        font-weight: 600;
        border-bottom: 1px solid {_P["border"]} !important;
    }}
    [data-testid="stDataFrame"] td {{
        font-size: 11px !important;
        border-bottom: 1px solid {_P["grid"]} !important;
    }}

    /* ---- Selectbox / multiselect ----------------------------------------- */
    [data-baseweb="select"] {{
        background: {_P["bg_card"]} !important;
        border: 1px solid {_P["border"]} !important;
        border-radius: 2px !important;
        font-size: 11px !important;
    }}
    [data-baseweb="select"] * {{ font-size: 11px !important; }}

    /* ---- Expander --------------------------------------------------------- */
    [data-testid="stExpander"] {{
        border: 1px solid {_P["border"]} !important;
        border-radius: 2px !important;
        background: {_P["bg_card"]} !important;
    }}
    [data-testid="stExpander"] summary {{
        font-size: 10px !important;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        color: {_P["text_muted"]} !important;
    }}

    /* ---- Buttons ---------------------------------------------------------- */
    .stButton button {{
        background: transparent !important;
        border: 1px solid {_P["accent"]} !important;
        color: {_P["accent"]} !important;
        font-size: 11px !important;
        font-weight: 600 !important;
        letter-spacing: 0.10em;
        text-transform: uppercase;
        border-radius: 2px !important;
        padding: 6px 16px !important;
    }}
    .stButton button:hover {{
        background: {_P["accent"]} !important;
        color: {_P["bg_primary"]} !important;
    }}

    /* ---- Sidebar widgets -------------------------------------------------- */
    [data-testid="stSidebar"] label, [data-testid="stSidebar"] .stSelectbox label {{
        font-size: 10px !important;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        color: {_P["text_muted"]} !important;
    }}

    /* ---- Info / warning / error boxes ------------------------------------ */
    [data-testid="stAlert"] {{
        border-radius: 2px !important;
        border-left: 3px solid {_P["accent"]} !important;
        font-size: 11px !important;
    }}

    /* ---- st.info ---------------------------------------------------------- */
    [data-testid="stAlert"][data-baseweb="notification"] {{
        background: {_P["bg_card"]} !important;
    }}

    /* ---- Plotly chart containers ----------------------------------------- */
    .stPlotlyChart {{
        border: 1px solid {_P["border"]};
        border-radius: 2px;
    }}
    </style>
    """,
    unsafe_allow_html=True,
)


# ===========================================================================
# Cached queries
# ===========================================================================

@st.cache_data(ttl=60, show_spinner="Consultando base...")
def cached_ping() -> dict:
    return ping()


@st.cache_data(ttl=60)
def cached_ultima_fecha() -> date | None:
    return ultima_fecha_cargada()


@st.cache_data(ttl=60)
def cached_ultima_actualizacion() -> datetime | None:
    """Timestamp de la ultima fila insertada en lineup (cron diario)."""
    return ultima_actualizacion_lineup()


@st.cache_data(ttl=600)
def cached_primera_fecha() -> date | None:
    return primera_fecha_cargada()


@st.cache_data(ttl=86400, show_spinner="Cargando histórico (esto pasa 1×/día)...")
def cached_master_exports(fecha_max: date) -> pd.DataFrame:
    """
    Master cache: TODOS los exports prioritarios de los últimos 5 años,
    ya normalizados (shipper_canon, origen_alt). Una sola query por día.

    El argumento `fecha_max` se usa SOLO como cache key — pasale
    `cached_ultima_fecha()` para que cuando entre data nueva el cache se
    invalide automáticamente.

    Las demás funciones cached_* derivan de este df via slicing pandas
    (microsegundos), no van más a la DB.
    """
    desde = fecha_max - timedelta(days=365 * 5 + 30)
    return query_exports_prioritarios(fecha_desde=desde, fecha_hasta=fecha_max)


@st.cache_data(ttl=900)
def cached_exports_rango(desde: date, hasta: date) -> pd.DataFrame:
    """
    Slice in-memory del master cache. Microsegundos en lugar de roundtrip
    a Supabase. El master se carga una vez por día.
    """
    df = cached_master_exports(cached_ultima_fecha() or hasta)
    if df.empty:
        return df
    fechas = pd.to_datetime(df["fecha_consulta"]).dt.date
    mask = (fechas >= desde) & (fechas <= hasta)
    return df[mask].copy()


@st.cache_data(ttl=86400, show_spinner="Agregando histórico...")
def cached_serie_diaria_hist(desde: date, hasta: date) -> pd.DataFrame:
    """
    Serie diaria agregada (tons agro LOAD) entre [desde, hasta], derivada
    del master cache. No toca la DB.
    """
    df = cached_master_exports(cached_ultima_fecha() or hasta)
    if df.empty:
        return df
    df = df[df["ops"] == "LOAD"]
    fechas = pd.to_datetime(df["fecha_consulta"]).dt.date
    mask = (fechas >= desde) & (fechas <= hasta)
    df = df.loc[mask, ["fecha_consulta", "quantity"]]
    if df.empty:
        return df
    df["fecha_consulta"] = pd.to_datetime(df["fecha_consulta"])
    diario = (
        df.groupby(df["fecha_consulta"].dt.date, as_index=False)["quantity"]
        .sum()
        .rename(columns={"fecha_consulta": "fecha"})
    )
    return diario


@st.cache_data(ttl=900, show_spinner="Histórico por producto...")
def cached_producto_historico(cargo: str, desde: date, hasta: date) -> pd.DataFrame:
    """Slice del master por cargo + rango. No toca la DB."""
    df = cached_master_exports(cached_ultima_fecha() or hasta)
    if df.empty:
        return df
    fechas = pd.to_datetime(df["fecha_consulta"]).dt.date
    mask = (df["cargo"] == cargo) & (fechas >= desde) & (fechas <= hasta)
    return df[mask].copy()


@st.cache_data(ttl=60)
def cached_en_puerto_ahora(fecha: date) -> pd.DataFrame:
    df = query_en_puerto_ahora(fecha)
    if df.empty:
        return df
    df = aplicar_a_dataframe(df)
    mapa_zona = {p: zona_de_puerto(p) for p in df["port"].unique()}
    df["zona"] = df["port"].map(mapa_zona)
    return df


@st.cache_data(ttl=3600, show_spinner="Consultando pronostico climatico...")
def cached_clima_zonas() -> dict[str, pd.DataFrame]:
    """
    Pronostico 7 dias para las 4 zonas portuarias.
    Cache 1h: Open-Meteo actualiza cada hora y no tiene sentido pegarle
    mas seguido. Si falla, devuelve dict con DataFrames vacios.
    """
    return clima_mod.pronostico_todas_zonas()


@st.cache_data(ttl=300)
def cached_serie_congestion(desde: date, hasta: date) -> pd.DataFrame:
    """
    Para cada dia en [desde, hasta]: cuantos buques tenian etb<=dia<=ets.
    Una sola query del rango y agrupado en pandas (~30x menos round-trips).
    """
    df_full = query_lineup(fecha_desde=desde, fecha_hasta=hasta)
    if df_full.empty:
        return pd.DataFrame()

    df_full = df_full[
        (df_full["ops"] == "LOAD") &
        df_full["etb"].notna() & df_full["ets"].notna()
    ].copy()
    if df_full.empty:
        return pd.DataFrame()

    df_full["fecha"] = pd.to_datetime(df_full["fecha_consulta"]).dt.date
    df_full["etb_d"] = pd.to_datetime(df_full["etb"]).dt.date
    df_full["ets_d"] = pd.to_datetime(df_full["ets"]).dt.date
    mask = (df_full["etb_d"] <= df_full["fecha"]) & (df_full["ets_d"] >= df_full["fecha"])
    df_ep = df_full[mask].copy()
    if df_ep.empty:
        return pd.DataFrame()

    mapa_zona = {p: zona_de_puerto(p) for p in df_ep["port"].unique()}
    df_ep["zona"] = df_ep["port"].map(mapa_zona)
    return (
        df_ep.groupby(["fecha", "zona"])["vessel"]
        .nunique().reset_index(name="buques")
    )


@st.cache_data(ttl=600, show_spinner="Cargando DJVE...")
def cached_djve(anio: int) -> pd.DataFrame:
    """
    DJVE acumuladas (declaraciones juradas de ventas al exterior).

    Prioriza leer de la tabla `djve` de Supabase (la pisa diariamente
    update_djve.py). Si la tabla esta vacia para el ano pedido, hace
    fallback a descargar el XLSX del MAGyP online (~30s).

    Cache 10 min: ya que DJVE se actualiza una vez al dia, 10 min en cache
    es mas que suficiente y evita hammerear la DB.
    """
    df_db = query_djve(anio=anio)
    if not df_db.empty:
        return df_db

    # Fallback: tabla vacia o el scheduled todavia no corrio.
    return fob_djve.descargar_djve_acumuladas(anio)


@st.cache_data(ttl=86400, show_spinner="Descargando estimaciones MAGyP...")
def cached_estimaciones() -> pd.DataFrame:
    """
    Estimaciones agricolas MAGyP (historico completo por cultivo/campania).
    Cache 24h: MAGyP actualiza cada ~6 meses, no tiene sentido pegarle mas.
    El archivo pesa ~15 MB, la descarga tarda ~30s la primera vez.
    """
    return estim_mod.descargar_estimaciones_magyp()


# ---------------------------------------------------------------------------
# Helpers cacheados pesados (vectorizacion de calculos por shipper)
# ---------------------------------------------------------------------------

def _senal_zscore(z: float) -> str:
    """Mapea un z-score a un emoji con etiqueta."""
    if z >= 2:
        return "🔥 HOT"
    if z >= 1:
        return "🟢 ALTO"
    if z >= -1:
        return "🟡 NORMAL"
    if z >= -2:
        return "🟠 BAJO"
    return "🔴 MUY BAJO"


@st.cache_data(ttl=300, show_spinner="Calculando z-scores...")
def _calcular_zscores_shippers(
    df_shp_all: pd.DataFrame,
    fecha_ref: date,
    ventana_dias: int,
) -> pd.DataFrame:
    """
    Calcula buques unicos en ventana rolling de N dias para cada shipper TOP,
    a lo largo de los ultimos 2 anos, y devuelve z-score actual vs su propia
    historia.

    Vectorizado: pivotea (fecha × buque) en una matriz binaria de presencia
    por shipper, hace un rolling-max sobre N dias (= "este buque aparecio
    al menos una vez en la ventana"), y suma columnas para obtener buques
    unicos en cada ventana. ~50× mas rapido que el loop original.

    Cache 5 min: la entrada cambia con fecha_ref / ventana_dias / cantidad de
    filas, asi que dentro de la misma sesion del usuario el cache pega.
    """
    if df_shp_all.empty:
        return pd.DataFrame()

    df = df_shp_all.copy()
    if "fecha_day" not in df.columns:
        df["fecha_day"] = pd.to_datetime(df["fecha_consulta"]).dt.date

    # Solo consideramos shippers TOP (los demas van a "OTROS" pero no
    # calculamos z-score para esa bolsa).
    df = df[df["shipper_canon"].isin(SHIPPERS_TOP)]
    if df.empty:
        return pd.DataFrame()

    all_dates = pd.date_range(
        start=fecha_ref - timedelta(days=730),
        end=fecha_ref, freq="D",
    ).date

    filas: list[dict] = []
    # Hacemos un loop por shipper, pero el calculo POR shipper es vectorizado.
    # 12 iteraciones × ~50ms = ~0.6s total en lugar de 3-8s.
    for shipper, sub in df.groupby("shipper_canon"):
        if sub.empty:
            continue

        # Pivot: filas=fecha, columnas=vessel, valor=1 si aparecio ese dia.
        # drop_duplicates evita filas redundantes de un mismo buque en un dia.
        presencia = (
            sub.assign(presente=1)
            .drop_duplicates(["fecha_day", "vessel"])
            .pivot(index="fecha_day", columns="vessel", values="presente")
            .fillna(0)
            .reindex(all_dates, fill_value=0)
        )

        # Rolling max -> "el buque V aparecio al menos un dia en los ultimos N".
        en_ventana = presencia.rolling(window=ventana_dias, min_periods=1).max()
        # Sumar columnas -> cantidad de buques unicos en esa ventana.
        serie = en_ventana.sum(axis=1)

        actual = float(serie.iloc[-1])
        mean_h = float(serie.mean())
        std_h = float(serie.std())
        z = (actual - mean_h) / std_h if std_h > 0 else 0.0

        filas.append({
            "Shipper": shipper,
            "Buques (vent)": actual,
            "Media hist.": round(mean_h, 1),
            "σ": round(std_h, 1),
            "Z-score": round(z, 2),
            "Senal": _senal_zscore(z),
        })

    # Garantizamos que aparezcan TODOS los shippers TOP, aunque no tengan
    # datos en la ventana de 2 anos (z-score = 0 / N/A en ese caso).
    presentes = {f["Shipper"] for f in filas}
    for shipper in SHIPPERS_TOP:
        if shipper not in presentes:
            filas.append({
                "Shipper": shipper,
                "Buques (vent)": 0,
                "Media hist.": 0,
                "σ": 0,
                "Z-score": 0,
                "Senal": _senal_zscore(0),
            })

    return pd.DataFrame(filas)


# ===========================================================================
# Health check y header
# ===========================================================================

estado = cached_ping()
if not estado["conectado"]:
    st.error(f"No puedo conectarme a Supabase: {estado['error']}")
    st.info(
        "Verifica que `.env` (o los secrets de Streamlit Cloud) "
        "tengan SUPABASE_URL y SUPABASE_ANON_KEY (o SUPABASE_SERVICE_ROLE_KEY) validos."
    )
    st.stop()

if estado["cantidad_filas"] == 0:
    st.warning(
        "La tabla `lineup` esta vacia. "
        "Correr `python backfill.py` antes de usar el dashboard."
    )
    st.stop()


fecha_max_db = cached_ultima_fecha() or date.today()
fecha_min_db = cached_primera_fecha() or date(2020, 1, 1)

# Pre-warm del master cache: una sola pasada al inicio del script
# que carga ~5 años de exports prioritarios. Las funciones cached_*
# derivan slices de este master, evitando ~10 queries adicionales.
_master_warmup = cached_master_exports(fecha_max_db)


# ---------------------------------------------------------------------------
# Sidebar: controles globales
# ---------------------------------------------------------------------------

with st.sidebar:
    st.markdown(
        f"<h2 style='color:{BLOOMBERG_PALETTE['accent']}; "
        f"font-family:Consolas,monospace; margin-top:0;'>"
        f"⚓ LINE-UP · AR</h2>",
        unsafe_allow_html=True,
    )
    st.caption(f"{estado['cantidad_filas']:,} movimientos en base")

    st.divider()

    fecha_ref = st.date_input(
        "Fecha de referencia",
        value=fecha_max_db,
        min_value=fecha_min_db,
        max_value=fecha_max_db,
        format="YYYY-MM-DD",
        key="fecha_ref_global",
    )

    ventana_opciones = {
        "Ultimos 7 dias":  7,
        "Ultimos 15 dias": 15,
        "Ultimos 30 dias": 30,
        "Ultimos 60 dias": 60,
        "Ultimos 90 dias": 90,
    }
    ventana_label = st.selectbox(
        "Ventana de analisis",
        options=list(ventana_opciones.keys()),
        index=2,  # default 30 dias
        key="ventana_dias_global",
    )
    ventana_dias = ventana_opciones[ventana_label]

    st.divider()

    # Info util (fecha actual del usuario vs ultima data en DB).
    st.caption(
        f"**Ultima data en DB:** {fecha_max_db}  \n"
        f"**Hoy:** {date.today()}"
    )
    dias_atrasados = (date.today() - fecha_max_db).days
    if dias_atrasados >= 2:
        # 2+ dias: probable ausencia de publicacion de la fuente (scraper corre
        # todos los dias). Suele pasar fines de semana / feriados.
        st.warning(
            f"⚠ Ultima data: {fecha_max_db} ({dias_atrasados} dias atras).  \n"
            "Suele no haber publicaciones fines de semana / feriados. Si persiste "
            "varios dias habiles seguidos, revisar el cron diario."
        )
    elif dias_atrasados == 1:
        # 1 dia: normal si hoy es lunes o aun no corrio el update del dia.
        st.info(f"Ultima data: {fecha_max_db}. Update corre diario a las 10:00.")


# ---------------------------------------------------------------------------
# Header principal
# ---------------------------------------------------------------------------

st.title("LINE-UP · PUERTOS ARGENTINOS")

# Ultima actualizacion: timestamp del ultimo cron exitoso (max(created_at)
# en lineup). Se muestra en horario ART (UTC-3, sin DST en AR) para que
# el trader vea de un toque "que tan fresco esta esto".
_ult_act = cached_ultima_actualizacion()
if _ult_act is not None:
    _art = _ult_act.astimezone(timezone(timedelta(hours=-3)))
    _ult_act_str = _art.strftime('%d-%b-%Y %H:%M ART')
else:
    _ult_act_str = "sin datos"

st.caption(
    f"🔄 **Ultima actualizacion:** {_ult_act_str}  ·  "
    f"Trading desk · "
    f"{fecha_ref.strftime('%A %d-%b-%Y')} · "
    f"Campana MAIZE {campanas.campana_de('MAIZE', fecha_ref)} · "
    f"Campana SOJA {campanas.campana_de('SBS', fecha_ref)} · "
    f"Campana TRIGO {campanas.campana_de('WHEAT', fecha_ref)}"
)


# ===========================================================================
# Funciones helper para analisis
# ===========================================================================

def clasificar_estado(remarks: str | None) -> str:
    """Clasifica un movimiento en CARGANDO / ARRIBANDO / TERMINADO."""
    if remarks is None or (isinstance(remarks, float) and np.isnan(remarks)):
        return "ARRIBANDO"
    r = str(remarks).upper().strip()
    if "LOADING" in r or "DISCH" in r:
        return "CARGANDO"
    if "CPTD" in r or "SAIL" in r or "COMPL" in r:
        return "TERMINADO"
    return "ARRIBANDO"


def fmt_tons(valor: float | int | None) -> str:
    """Formatea toneladas de forma compacta (Bloomberg style)."""
    if valor is None or pd.isna(valor):
        return "—"
    v = float(valor)
    if abs(v) >= 1_000_000:
        return f"{v / 1_000_000:,.2f}M"
    if abs(v) >= 1_000:
        return f"{v / 1_000:,.0f}K"
    return f"{v:,.0f}"


def pct_change(actual: float, prev: float) -> str:
    """Formatea un cambio porcentual con signo."""
    if not prev or pd.isna(prev):
        return "—"
    pct = (actual - prev) / prev * 100
    signo = "+" if pct >= 0 else ""
    return f"{signo}{pct:.1f}%"


# ===========================================================================
# Panel de alertas accionables: "Señales del día"
# ===========================================================================

def _render_senales_hoy():
    """
    Panel visible con alertas accionables basadas en datos ya cargados.
    No hace queries directas a la DB: usa las funciones cached_* existentes.
    """
    alertas_info    = []
    alertas_success = []
    alertas_warning = []

    # ---- Alerta: datos desactualizados (>36 horas) ----
    _ts = cached_ultima_actualizacion()
    if _ts is None:
        alertas_warning.append("⚠️ No se pudo obtener el timestamp de la ultima actualizacion.")
    else:
        _ahora_utc = datetime.now(timezone.utc)
        _ts_aware = _ts if _ts.tzinfo is not None else _ts.replace(tzinfo=timezone.utc)
        _horas_atras = (_ahora_utc - _ts_aware).total_seconds() / 3600
        if _horas_atras > 36:
            _art_ts = _ts_aware.astimezone(timezone(timedelta(hours=-3)))
            alertas_warning.append(
                f"⚠️ **DATOS DESACTUALIZADOS** — ultima carga: "
                f"{_art_ts.strftime('%d-%b-%Y %H:%M ART')} "
                f"({_horas_atras:.0f}h atras). Verificar cron diario."
            )

    # ---- Datos de hoy y ayer para las alertas de buques ----
    _desde_pan = fecha_ref - timedelta(days=2)
    _df_pan = cached_exports_rango(_desde_pan, fecha_ref)

    if not _df_pan.empty:
        _df_pan["fecha_consulta"] = pd.to_datetime(_df_pan["fecha_consulta"])
        _df_sen_hoy  = _df_pan[_df_pan["fecha_consulta"].dt.date == fecha_ref]
        _df_sen_ayer = _df_pan[_df_pan["fecha_consulta"].dt.date == fecha_ref - timedelta(days=1)]

        # ---- Alerta: buques nuevos grandes (>=50 000 t) no vistos ayer ----
        if not _df_sen_hoy.empty:
            _grandes_hoy = _df_sen_hoy[_df_sen_hoy["quantity"].fillna(0) >= 50_000]
            _vessels_ayer = set(_df_sen_ayer["vessel"].dropna().unique()) if not _df_sen_ayer.empty else set()
            _nuevos_grandes = _grandes_hoy[~_grandes_hoy["vessel"].isin(_vessels_ayer)]

            # Dedupe por vessel (puede haber splits de destino).
            _nuevos_grandes = (
                _nuevos_grandes
                .sort_values("quantity", ascending=False)
                .drop_duplicates("vessel")
            )
            for _, _row in _nuevos_grandes.iterrows():
                _qty_fmt = fmt_tons(_row.get("quantity"))
                _shp = _row.get("shipper_canon") or _row.get("shipper") or "—"
                alertas_success.append(
                    f"🚢 **Buque nuevo grande** · {_row['vessel']} · "
                    f"{_row.get('cargo','?')} · {_qty_fmt} · "
                    f"Shipper: {_shp} · Puerto: {_row.get('port','?')}"
                )

    # ---- Alerta: z-score de shippers (usando calc ya existente) ----
    if not _df_pan.empty:
        _desde_shp_z = fecha_ref - timedelta(days=730)
        _df_z_all = cached_exports_rango(_desde_shp_z, fecha_ref)
        if not _df_z_all.empty:
            _df_z_all["fecha_consulta"] = pd.to_datetime(_df_z_all["fecha_consulta"])
            _df_zscores = _calcular_zscores_shippers(_df_z_all, fecha_ref, ventana_dias)
            if not _df_zscores.empty:
                for _, _zrow in _df_zscores.iterrows():
                    _z = float(_zrow.get("Z-score", 0))
                    _shp_n = _zrow["Shipper"]
                    if _z >= 2:
                        alertas_success.append(
                            f"🔥 **Surge shipper** · {_shp_n} · "
                            f"Z-score {_z:+.1f} (muy por encima de su media historica)"
                        )
                    elif _z <= -2:
                        alertas_info.append(
                            f"🔴 **Caida shipper** · {_shp_n} · "
                            f"Z-score {_z:+.1f} (muy por debajo de su media historica)"
                        )

    # ---- Renderizar ----
    total_alertas = len(alertas_warning) + len(alertas_success) + len(alertas_info)

    st.markdown(
        f"<h2 style='margin-bottom:4px;'>🔔 SEÑALES HOY"
        f"{'  · ' + str(total_alertas) + ' alerta(s)' if total_alertas else ''}"
        f"</h2>",
        unsafe_allow_html=True,
    )

    if total_alertas == 0:
        st.success("✅ Sin señales destacadas hoy.")
    else:
        for msg in alertas_warning:
            st.warning(msg)
        for msg in alertas_success:
            st.success(msg)
        for msg in alertas_info:
            st.info(msg)

    st.divider()


_render_senales_hoy()


# ===========================================================================
# Pestanas
# ===========================================================================

tab_pan, tab_shp, tab_prd, tab_cng = st.tabs([
    "📊 PANORAMA",
    "🏢 SHIPPERS",
    "🌾 PRODUCTOS",
    "⚓ CONGESTION",
])


# ==========================================================================
# PESTANA 1: PANORAMA
# ==========================================================================

@st.fragment
def _render_panorama_tab(fecha_ref, ventana_dias):
    # Traer 90 dias para poder calcular tendencias y promedios moviles.
    desde = fecha_ref - timedelta(days=90)
    df_panorama = cached_exports_rango(desde, fecha_ref)

    if df_panorama.empty:
        st.info(f"Sin datos entre {desde} y {fecha_ref}.")
        return

    df_panorama["fecha_consulta"] = pd.to_datetime(df_panorama["fecha_consulta"])
    df_panorama["estado"] = df_panorama["remarks"].map(clasificar_estado)
    df_hoy = df_panorama[df_panorama["fecha_consulta"].dt.date == fecha_ref]

    # ------------------- KPI row -------------------
    # Referencias temporales para comparaciones.
    df_7d = df_panorama[df_panorama["fecha_consulta"].dt.date == fecha_ref - timedelta(days=7)]
    df_ventana = df_panorama[
        df_panorama["fecha_consulta"].dt.date >= fecha_ref - timedelta(days=ventana_dias)
    ]

    buques_cargando_hoy = df_hoy[df_hoy["estado"] == "CARGANDO"]["vessel"].nunique()
    buques_arribando_hoy = df_hoy[df_hoy["estado"] == "ARRIBANDO"]["vessel"].nunique()
    buques_cargando_7d = df_7d[df_7d["estado"] == "CARGANDO"]["vessel"].nunique() if not df_7d.empty else 0
    buques_arribando_7d = df_7d[df_7d["estado"] == "ARRIBANDO"]["vessel"].nunique() if not df_7d.empty else 0

    tons_hoy = df_hoy["quantity"].fillna(0).sum()
    tons_7d = df_7d["quantity"].fillna(0).sum() if not df_7d.empty else 0

    # Promedio diario de la ventana para referencia.
    tons_promedio_ventana = (
        df_ventana.groupby(df_ventana["fecha_consulta"].dt.date)["quantity"]
        .sum().mean() if not df_ventana.empty else 0
    )

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric(
        "Cargando hoy",
        buques_cargando_hoy,
        delta=f"{buques_cargando_hoy - buques_cargando_7d:+d} vs 7d" if buques_cargando_7d else None,
    )
    k2.metric(
        "Arribando hoy",
        buques_arribando_hoy,
        delta=f"{buques_arribando_hoy - buques_arribando_7d:+d} vs 7d" if buques_arribando_7d else None,
    )
    # Backlog: buques con ETA futura (próximos 7d) = toneladas en la cola.
    # Más accionable que el conteo total de buques del dia.
    df_backlog = df_panorama[
        (df_panorama["fecha_consulta"].dt.date == fecha_ref) &
        (df_panorama["eta"] > fecha_ref) &
        (df_panorama["eta"] <= fecha_ref + timedelta(days=7))
    ]
    tons_backlog_7d = df_backlog["quantity"].fillna(0).sum()
    k3.metric(
        "En cola (próx 7d)",
        fmt_tons(tons_backlog_7d),
        delta=f"{df_backlog['vessel'].nunique()} buques",
        delta_color="off",
        help="Toneladas de buques con ETA en los próximos 7 días — mide la presión de carga inminente.",
    )
    k4.metric("Toneladas hoy", fmt_tons(tons_hoy), delta=pct_change(tons_hoy, tons_7d))
    k5.metric(f"Promedio {ventana_dias}d", fmt_tons(tons_promedio_ventana))

    st.divider()

    # ------------------- Chart 1: Serie temporal ventana -------------------
    st.subheader("Actividad diaria · ventana seleccionada")

    diario = (
        df_ventana.assign(fecha=df_ventana["fecha_consulta"].dt.date)
        .groupby(["fecha", "estado"])
        .agg(buques=("vessel", "nunique"), tons=("quantity", "sum"))
        .reset_index()
    )

    col_g1, col_g2 = st.columns(2)

    with col_g1:
        st.caption("Buques por dia (Cargando vs Arribando)")
        fig_b = px.bar(
            diario, x="fecha", y="buques", color="estado",
            color_discrete_map={
                "CARGANDO":  BLOOMBERG_PALETTE["accent"],
                "ARRIBANDO": BLOOMBERG_PALETTE["accent_blue"],
                "TERMINADO": BLOOMBERG_PALETTE["text_muted"],
            },
            barmode="stack",
            labels={"fecha": "", "buques": "Buques", "estado": ""},
        )
        # x=pd.Timestamp evita un TypeError en plotly al calcular la
        # posicion de la anotacion sobre add_vline con datetime.date.
        _hoy_ts = pd.Timestamp(fecha_ref)
        fig_b.add_vline(
            x=_hoy_ts, line_dash="dot",
            line_color=BLOOMBERG_PALETTE["warning"],
        )
        fig_b.add_annotation(
            x=_hoy_ts, y=1, yref="paper", showarrow=False,
            text="Hoy", font=dict(color=BLOOMBERG_PALETTE["warning"]),
            xanchor="left", yanchor="top",
        )
        aplicar_tema(fig_b)
        fig_b.update_layout(height=340, legend=dict(orientation="h", yanchor="bottom", y=1.02))
        st.plotly_chart(fig_b, use_container_width=True)

    with col_g2:
        st.caption("Toneladas por dia")
        daily_tons = diario.groupby("fecha")["tons"].sum().reset_index()
        # Promedio movil 7d como overlay.
        daily_tons["ma7"] = daily_tons["tons"].rolling(7, min_periods=1).mean()

        # --- Comparativa interanual: año anterior + banda ±1σ histórica ------
        # Histórico liviano: solo 3 columnas, agregado en DB-side a nivel diario.
        _desde_hist = fecha_ref.replace(year=fecha_ref.year - 5)
        _diario_hist = cached_serie_diaria_hist(_desde_hist, fecha_ref - timedelta(days=1))

        _df_ant = pd.DataFrame()
        _band_stats = pd.DataFrame()

        if not _diario_hist.empty:
            _diario_hist = _diario_hist.copy()
            _fechas = pd.to_datetime(_diario_hist["fecha"])
            _diario_hist["_year"] = _fechas.dt.year
            _diario_hist["_doy"]  = _fechas.dt.dayofyear
            _hist_daily = _diario_hist.rename(columns={"quantity": "tons"})

            # --- Año anterior ---
            _anio_ant = fecha_ref.year - 1
            _df_ant_raw = _hist_daily[_hist_daily["_year"] == _anio_ant].copy()
            if not _df_ant_raw.empty:
                _df_ant_raw["fecha_plot"] = (
                    pd.Timestamp(date(fecha_ref.year, 1, 1)) +
                    pd.to_timedelta(_df_ant_raw["_doy"].astype(int) - 1, unit="D")
                ).dt.date
                _df_ant = _df_ant_raw

            # --- Banda ±1σ (años < año anterior para no contaminar con año ant) ---
            _df_band_raw = _hist_daily[_hist_daily["_year"] < _anio_ant].copy()
            if not _df_band_raw.empty:
                _band_stats = (
                    _df_band_raw.groupby("_doy")["tons"]
                    .agg(media="mean", sigma="std")
                    .reset_index()
                )
                _band_stats["fecha_plot"] = (
                    pd.Timestamp(date(fecha_ref.year, 1, 1)) +
                    pd.to_timedelta(_band_stats["_doy"].astype(int) - 1, unit="D")
                ).dt.date
        # ---------------------------------------------------------------------

        fig_t = go.Figure()

        # Banda ±1σ histórica (renderizar primero para que quede debajo).
        if not _band_stats.empty:
            _sigma_fill = pd.concat([
                _band_stats["fecha_plot"],
                _band_stats["fecha_plot"].iloc[::-1],
            ])
            _sigma_y = pd.concat([
                _band_stats["media"] + _band_stats["sigma"].fillna(0),
                (_band_stats["media"] - _band_stats["sigma"].fillna(0)).iloc[::-1],
            ])
            fig_t.add_trace(go.Scatter(
                x=_sigma_fill,
                y=_sigma_y,
                fill="toself",
                fillcolor="rgba(102,85,238,0.10)",
                line=dict(color="rgba(255,255,255,0)"),
                name="Banda hist ±1σ",
                showlegend=True,
                hoverinfo="skip",
            ))

        # Línea año anterior (punteada, más tenue).
        if not _df_ant.empty:
            fig_t.add_trace(go.Scatter(
                x=_df_ant["fecha_plot"],
                y=_df_ant["tons"],
                mode="lines",
                name=f"Año {_anio_ant}",
                line=dict(color="#6655ee", dash="dot", width=1),
                opacity=0.7,
            ))

        fig_t.add_bar(
            x=daily_tons["fecha"], y=daily_tons["tons"],
            name="Toneladas",
            marker_color=BLOOMBERG_PALETTE["accent"],
            opacity=0.7,
        )
        fig_t.add_scatter(
            x=daily_tons["fecha"], y=daily_tons["ma7"],
            name="MM 7d", mode="lines",
            line=dict(color=BLOOMBERG_PALETTE["accent_blue"], width=2),
        )
        # pd.Timestamp para evitar el bug de _mean en plotly sobre date.
        fig_t.add_vline(
            x=pd.Timestamp(fecha_ref), line_dash="dot",
            line_color=BLOOMBERG_PALETTE["warning"],
        )
        aplicar_tema(fig_t)
        fig_t.update_layout(height=340, legend=dict(orientation="h", yanchor="bottom", y=1.02))
        st.plotly_chart(fig_t, use_container_width=True)

    st.divider()

    # ------------------- Tabla resumen por producto -------------------
    st.subheader("Resumen por producto · hoy vs tendencia")

    # Semana actual y semana anterior para calcular Δ semanal.
    semana_actual_desde = fecha_ref - timedelta(days=6)
    semana_ant_desde    = fecha_ref - timedelta(days=13)
    semana_ant_hasta    = fecha_ref - timedelta(days=7)
    df_sem_act = df_panorama[df_panorama["fecha_consulta"].dt.date >= semana_actual_desde]
    df_sem_ant = df_panorama[
        (df_panorama["fecha_consulta"].dt.date >= semana_ant_desde) &
        (df_panorama["fecha_consulta"].dt.date <= semana_ant_hasta)
    ]

    resumen_rows = []
    for codigo, display, _familia in PRODUCTOS_PRIORITARIOS:
        hoy_p = df_hoy[df_hoy["cargo"] == codigo]
        vent_p = df_ventana[df_ventana["cargo"] == codigo]

        b_cargando = hoy_p[hoy_p["estado"] == "CARGANDO"]["vessel"].nunique()
        b_arribando = hoy_p[hoy_p["estado"] == "ARRIBANDO"]["vessel"].nunique()
        tons_p_hoy = hoy_p["quantity"].fillna(0).sum()
        tons_p_prom = (
            vent_p.groupby(vent_p["fecha_consulta"].dt.date)["quantity"]
            .sum().mean() if not vent_p.empty else 0
        )

        # Δ semanal: semana actual (7d) vs semana anterior (7d).
        tons_sem_act = df_sem_act[df_sem_act["cargo"] == codigo]["quantity"].fillna(0).sum()
        tons_sem_ant = df_sem_ant[df_sem_ant["cargo"] == codigo]["quantity"].fillna(0).sum()

        # Top shipper y top destino del dia para este producto.
        hoy_ship = hoy_p.groupby("shipper_canon")["quantity"].sum()
        top_ship = hoy_ship.idxmax() if not hoy_ship.empty else "—"
        hoy_dest = hoy_p.groupby(
            hoy_p["dest_orig"].fillna("s/d").str.strip().str.upper()
        )["quantity"].sum()
        top_dest = hoy_dest.idxmax() if not hoy_dest.empty else "—"

        # Campana actual vs anterior.
        camp_actual = campanas.campana_de(codigo, fecha_ref)
        camp_anterior = campanas.campanas_anteriores(codigo, fecha_ref, n=1)[0]
        inicio_actual, _ = campanas.fechas_de_campana(codigo, camp_actual)
        inicio_ant, fin_ant = campanas.fechas_de_campana(codigo, camp_anterior)

        resumen_rows.append({
            "Producto":       display,
            "Cargando":       b_cargando,
            "Arribando":      b_arribando,
            "Tons hoy":       fmt_tons(tons_p_hoy),
            f"Prom {ventana_dias}d": fmt_tons(tons_p_prom),
            "vs prom":        pct_change(tons_p_hoy, tons_p_prom),
            "Δ sem":          pct_change(tons_sem_act, tons_sem_ant),
            "Top shipper":    top_ship,
            "Top destino":    top_dest,
            "Campana":        camp_actual,
            # Columna interna para ordenar por valor numerico real, ya que la
            # columna "Tons hoy" esta formateada como string ("46K").
            "_tons_hoy_raw":  float(tons_p_hoy or 0),
        })

    df_resumen = pd.DataFrame(resumen_rows)
    # Default: ordenar por toneladas hoy (DESC) — lo que el trader quiere ver
    # primero son los productos mas activos del dia, no el orden alfabetico.
    df_resumen = (
        df_resumen.sort_values("_tons_hoy_raw", ascending=False)
        .drop(columns="_tons_hoy_raw")
        .reset_index(drop=True)
    )
    st.dataframe(df_resumen, use_container_width=True, hide_index=True, height=320)
    _csv_panorama = df_resumen.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="⬇ Descargar CSV",
        data=_csv_panorama,
        file_name=f"panorama_{fecha_ref}.csv",
        mime="text/csv",
        key="dl_panorama",
    )

    st.divider()

    # ------------------- Heatmap puerto x producto -------------------
    st.subheader(f"Heatmap · puerto × producto · {fecha_ref}")

    if df_hoy.empty:
        st.info("Sin data para hoy.")
    else:
        df_hm = df_hoy.copy()
        df_hm["producto"] = df_hm["cargo"].map(PRODUCTO_DISPLAY).fillna(df_hm["cargo"])
        pivot = df_hm.pivot_table(
            index="port", columns="producto",
            values="quantity", aggfunc="sum", fill_value=0,
        )
        if pivot.empty:
            st.info("Sin cruces puerto × producto hoy.")
        else:
            fig_hm = px.imshow(
                pivot, aspect="auto",
                color_continuous_scale=[
                    [0.0, BLOOMBERG_PALETTE["bg_card"]],
                    [0.5, BLOOMBERG_PALETTE["accent_blue"]],
                    [1.0, BLOOMBERG_PALETTE["accent"]],
                ],
                labels={"color": "Tons"},
            )
            aplicar_tema(fig_hm)
            fig_hm.update_layout(height=max(300, 40 + 22 * len(pivot)))
            st.plotly_chart(fig_hm, use_container_width=True)


# ==========================================================================
# PESTANA 2: SHIPPERS (core)
# ==========================================================================

@st.fragment
def _render_shippers_tab(fecha_ref, ventana_dias):
    # Traemos ~2 anos para poder calcular historia por shipper con robustez.
    desde_shp = fecha_ref - timedelta(days=730)
    df_shp_all = cached_exports_rango(desde_shp, fecha_ref)

    if df_shp_all.empty:
        st.info("Sin data historica suficiente.")
        return

    df_shp_all["fecha_consulta"] = pd.to_datetime(df_shp_all["fecha_consulta"])

    # Sub-conjunto = ventana de analisis. Aplicamos dedupe por buque para que
    # las sumas de toneladas reflejen el FLUJO real (no se acumulan los
    # snapshots repetidos del mismo buque). El z-score usa df_shp_all sin
    # tocar; las sumas en esta vista usan df_shp_vent ya deduplicado.
    df_shp_vent = df_shp_all[
        df_shp_all["fecha_consulta"].dt.date >= fecha_ref - timedelta(days=ventana_dias)
    ]
    df_shp_vent = df_shp_vent.sort_values("fecha_consulta").copy()
    _ult_foto_shp = df_shp_vent.groupby("vessel")["fecha_consulta"].transform("max")
    df_shp_vent = df_shp_vent[df_shp_vent["fecha_consulta"] == _ult_foto_shp]

    # ------------------- Ranking por shipper en la ventana -------------------
    st.subheader(f"Ranking top 10 · ultimos {ventana_dias} dias")

    # Agregado ventana.
    agg_vent = (
        df_shp_vent.groupby("shipper_canon")
        .agg(buques=("vessel", "nunique"), tons=("quantity", "sum"))
        .reindex(SHIPPERS_TOP + ["OTROS"], fill_value=0)
        .reset_index()
        .rename(columns={"shipper_canon": "Shipper"})
    )

    # Senal: cada shipper contra su propia historia de los ultimos 2 anos.
    # Calculamos buques-por-ventana-de-N-dias rolling para cada shipper.
    #
    # PERF: la version anterior hacia un loop O(N×M) (~9000 iters) con
    # set-unions Python puro -> 3-8s en cada render. La version vectorizada
    # construye una matriz pivot (fecha × buque) y hace un rolling-max sobre
    # el flag de presencia: 100% numpy/pandas.
    df_senales = _calcular_zscores_shippers(df_shp_all, fecha_ref, ventana_dias)
    df_senales = df_senales.merge(
        agg_vent[["Shipper", "tons"]], on="Shipper", how="left",
    )
    df_senales["Tons"] = df_senales["tons"].apply(fmt_tons)
    df_senales = df_senales.drop(columns=["tons"])
    df_senales = df_senales.sort_values("Z-score", ascending=False)

    st.dataframe(
        df_senales,
        use_container_width=True,
        hide_index=True,
        column_order=[
            "Shipper", "Senal", "Buques (vent)", "Media hist.", "σ", "Z-score", "Tons",
        ],
        height=420,
    )
    _csv_shippers = df_senales.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="⬇ Descargar CSV",
        data=_csv_shippers,
        file_name=f"shippers_{fecha_ref}.csv",
        mime="text/csv",
        key="dl_shippers",
    )

    st.caption(
        "Senal basada en z-score del conteo de buques propios contra ultimos "
        "2 anos del mismo shipper. 🔥 = z≥2 (mas barcos que lo normal, senal "
        "de trade). 🔴 = z≤-2 (muy por debajo de su media historica)."
    )

    st.divider()

    # ------------------- Chart: share por shipper en la ventana -------------------
    col_a, col_b = st.columns(2)

    with col_a:
        st.caption(f"Toneladas por shipper · ultimos {ventana_dias}d")
        agg_tons = (
            df_shp_vent.groupby("shipper_canon")["quantity"].sum()
            .reset_index()
            .rename(columns={"shipper_canon": "Shipper", "quantity": "Tons"})
        )
        agg_tons = agg_tons[agg_tons["Tons"] > 0].sort_values("Tons", ascending=True)
        colors = [SHIPPER_COLORS.get(s, SHIPPER_COLORS["OTROS"]) for s in agg_tons["Shipper"]]

        fig_tons = go.Figure()
        fig_tons.add_bar(
            x=agg_tons["Tons"], y=agg_tons["Shipper"],
            orientation="h",
            marker_color=colors,
            text=agg_tons["Tons"].apply(fmt_tons),
            textposition="outside",
            textfont=dict(color=BLOOMBERG_PALETTE["text_primary"]),
        )
        aplicar_tema(fig_tons)
        fig_tons.update_layout(
            height=380, showlegend=False,
            xaxis_title="Toneladas", yaxis_title="",
        )
        st.plotly_chart(fig_tons, use_container_width=True)

    with col_b:
        st.caption(f"Flujo Paraguay/Uruguay · ultimos {ventana_dias}d")
        df_filial = df_shp_vent[df_shp_vent["origen_alt"].notna()].copy()
        if df_filial.empty:
            st.info("Sin flujo de filiales PY/UY en la ventana.")
        else:
            # Buques por shipper y origen.
            agg_py = (
                df_filial.groupby(["shipper_canon", "origen_alt"])
                .agg(buques=("vessel", "nunique"), tons=("quantity", "sum"))
                .reset_index()
            )
            fig_py = px.bar(
                agg_py, x="tons", y="shipper_canon",
                color="origen_alt", orientation="h",
                color_discrete_map={"PY": "#FF3333", "UY": "#33AAFF"},
                labels={"tons": "Toneladas", "shipper_canon": "", "origen_alt": "Filial"},
            )
            aplicar_tema(fig_py)
            fig_py.update_layout(height=380)
            st.plotly_chart(fig_py, use_container_width=True)

    st.divider()

    # ------------------- Actividad mensual top 10 · ultimos 3 meses -------
    # Vista dinamica de "quien se mueve esta semana/mes". El z-score de mas
    # arriba es buena senal estadistica pero no le dice al trader quien
    # embarco mas TONELADAS hoy. Esta tabla responde: cuanto tonelaje cargo
    # cada shipper en los ultimos 3 meses, y como se compara contra el mes
    # anterior (Δ MoM) y contra el mismo mes hace 12 meses (Δ YoY).
    st.subheader("Actividad mensual top 10 · ultimos 3 meses")
    st.caption(
        "Toneladas por shipper en los ultimos 3 meses. Δ MoM = variacion vs "
        "mes anterior. Δ YoY = variacion vs mismo mes hace 12 meses."
    )

    # Filtro por producto (en expander colapsado para no ocupar espacio).
    with st.expander("Filtrar por producto / cosecha", expanded=False):
        _prod_opts = ["Todos"] + sorted(df_shp_all["cargo"].dropna().unique().tolist())
        _camp_opts_shp = ["Todas"] + sorted(
            df_shp_all["cargo"].apply(
                lambda c: campanas.campana_de(c, fecha_ref)
            ).unique().tolist(),
            reverse=True,
        )
        fmc1, fmc2 = st.columns(2)
        f_prod_mes = fmc1.selectbox("Producto", _prod_opts, key="shp_mes_prod")
        f_camp_mes = fmc2.selectbox("Cosecha", _camp_opts_shp, key="shp_mes_camp")

    hoy_per_shp = pd.Period(fecha_ref, freq="M")
    periodos_3 = [(hoy_per_shp - i) for i in range(2, -1, -1)]
    yoy_per = hoy_per_shp - 12

    # Dedupe: tomar la ultima foto por buque. Asignar al mes de carga via ETB.
    df_shp_mes = df_shp_all.copy()
    if f_prod_mes != "Todos":
        df_shp_mes = df_shp_mes[df_shp_mes["cargo"] == f_prod_mes]
    df_shp_mes = df_shp_mes.sort_values("fecha_consulta")
    ult_foto_shp = df_shp_mes.groupby("vessel")["fecha_consulta"].transform("max")
    df_shp_mes = df_shp_mes[df_shp_mes["fecha_consulta"] == ult_foto_shp]
    df_shp_mes["mes_carga"] = pd.to_datetime(
        df_shp_mes["etb"].fillna(df_shp_mes["eta"]).fillna(df_shp_mes["fecha_consulta"])
    ).dt.to_period("M")
    # Filtro cosecha: si el usuario elige una, filtrar por fecha de inicio de campaña.
    if f_camp_mes != "Todas" and not df_shp_mes.empty:
        # Calcular campaña por mes de carga usando el primer producto disponible
        _prod_ref = f_prod_mes if f_prod_mes != "Todos" else "MAIZE"
        df_shp_mes = df_shp_mes[
            df_shp_mes["mes_carga"].apply(
                lambda m: campanas.campana_de(_prod_ref, m.to_timestamp().date()) == f_camp_mes
            )
        ]
    pivot_mes = (
        df_shp_mes.groupby(["shipper_canon", "mes_carga"])["quantity"].sum()
        .unstack(fill_value=0)
    )
    pivot_top = pivot_mes.reindex(SHIPPERS_TOP).fillna(0)

    def _safe(row, key):
        return float(row[key]) if key in row.index else 0.0

    filas_mes = []
    label_anterior = f"{periodos_3[0].strftime('%b')} {periodos_3[0].year % 100:02d}"
    label_prev = f"{periodos_3[1].strftime('%b')} {periodos_3[1].year % 100:02d}"
    label_actual = f"{periodos_3[2].strftime('%b')} {periodos_3[2].year % 100:02d}"
    for shipper, row in pivot_top.iterrows():
        v_act = _safe(row, periodos_3[2])
        v_prev = _safe(row, periodos_3[1])
        v_ant = _safe(row, periodos_3[0])
        v_yoy = _safe(row, yoy_per)
        delta_mom = ((v_act - v_prev) / v_prev * 100) if v_prev else None
        delta_yoy = ((v_act - v_yoy) / v_yoy * 100) if v_yoy else None
        filas_mes.append({
            "Shipper":      shipper,
            label_anterior: fmt_tons(v_ant),
            label_prev:     fmt_tons(v_prev),
            label_actual:   fmt_tons(v_act),
            "Total 3m":     fmt_tons(v_ant + v_prev + v_act),
            "Δ MoM": (f"{delta_mom:+.0f}%" if delta_mom is not None else "—"),
            "Δ YoY": (f"{delta_yoy:+.0f}%" if delta_yoy is not None else "—"),
            "_sort": v_act,
        })

    df_mes_table = (
        pd.DataFrame(filas_mes)
        .sort_values("_sort", ascending=False)
        .drop(columns="_sort")
        .reset_index(drop=True)
    )
    st.dataframe(df_mes_table, use_container_width=True, hide_index=True, height=380)

    # Chart: grouped bar de los top 6 shippers por toneladas del mes actual.
    top6 = df_mes_table.head(6)["Shipper"].tolist()
    fig_mes_shp = go.Figure()
    mes_labels = [
        f"{p.strftime('%b')} {p.year % 100:02d}" for p in periodos_3
    ]
    for shipper in top6:
        valores = [
            float(pivot_top.loc[shipper, p]) if p in pivot_top.columns else 0.0
            for p in periodos_3
        ]
        fig_mes_shp.add_bar(
            x=mes_labels, y=valores, name=shipper,
            marker_color=SHIPPER_COLORS.get(shipper, SHIPPER_COLORS["OTROS"]),
        )
    aplicar_tema(fig_mes_shp)
    fig_mes_shp.update_layout(
        height=360, barmode="group",
        yaxis_title="Toneladas",
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    st.plotly_chart(fig_mes_shp, use_container_width=True)

    st.divider()

    # ------------------- Drill-down: shipper seleccionado -------------------
    st.subheader("Drill-down por shipper")
    shipper_sel = st.selectbox(
        "Seleccionar shipper",
        options=SHIPPERS_TOP,
        index=0,
    )

    df_one = df_shp_all[df_shp_all["shipper_canon"] == shipper_sel].copy()
    if df_one.empty:
        st.info(f"Sin data para {shipper_sel}.")
    else:
        df_one["mes"] = df_one["fecha_consulta"].dt.to_period("M").dt.to_timestamp()
        mensual = (
            df_one.groupby("mes")
            .agg(buques=("vessel", "nunique"), tons=("quantity", "sum"))
            .reset_index()
        )

        color_shp = SHIPPER_COLORS.get(shipper_sel, SHIPPER_COLORS["OTROS"])

        fig_one = go.Figure()
        fig_one.add_bar(
            x=mensual["mes"], y=mensual["buques"],
            name="Buques", marker_color=color_shp, opacity=0.7,
        )
        fig_one.add_scatter(
            x=mensual["mes"], y=mensual["buques"].rolling(3, min_periods=1).mean(),
            name="MM 3m", mode="lines",
            line=dict(color=BLOOMBERG_PALETTE["accent_blue"], width=2),
        )
        aplicar_tema(fig_one)
        fig_one.update_layout(
            height=340, title=f"{shipper_sel} · buques por mes",
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
        )
        st.plotly_chart(fig_one, use_container_width=True)

        # Breakdown por producto.
        por_prod = (
            df_one[df_one["cargo"].isin(CODIGOS_PRIORITARIOS)]
            .groupby("cargo")["quantity"].sum()
            .reset_index()
        )
        por_prod["producto"] = por_prod["cargo"].map(PRODUCTO_DISPLAY)
        por_prod = por_prod.sort_values("quantity", ascending=True)

        fig_mix = px.bar(
            por_prod, x="quantity", y="producto",
            orientation="h", labels={"quantity": "Toneladas total", "producto": ""},
        )
        fig_mix.update_traces(marker_color=color_shp)
        aplicar_tema(fig_mix)
        fig_mix.update_layout(
            height=300, title=f"{shipper_sel} · mix de productos (total historico ventana)",
        )
        st.plotly_chart(fig_mix, use_container_width=True)


# ==========================================================================
# Render de las pestanas refactoreadas a funciones (PANORAMA y SHIPPERS).
# Se hace aca para que la definicion de las funciones quede arriba en el
# archivo y la ejecucion de los tabs siga el orden visual del usuario.
# ==========================================================================

with tab_pan:
    _render_panorama_tab(fecha_ref, ventana_dias)

with tab_shp:
    _render_shippers_tab(fecha_ref, ventana_dias)


# ==========================================================================
# PESTANA 3: PRODUCTOS (vista por producto con comparacion de campanas)
# ==========================================================================

@st.fragment
def _render_productos_tab(fecha_ref):
    """
    Render de la pestaña Productos. Como `@st.fragment`, cuando el usuario
    cambia el selector de producto SOLO se re-ejecuta esta funcion — el resto
    del dashboard (Panorama, Shippers, Congestion) NO se vuelve a calcular.
    Sin esto, cambiar producto re-disparaba todo el script y la UI se tildaba.
    """
    col_sel, col_info = st.columns([1, 3])
    with col_sel:
        opciones = [(codigo, display) for codigo, display, _ in PRODUCTOS_PRIORITARIOS]
        producto_sel = st.selectbox(
            "Producto",
            options=opciones,
            format_func=lambda x: x[1],
            index=3,  # default MAIZE
        )
        codigo_prd, display_prd = producto_sel

    campana_actual = campanas.campana_de(codigo_prd, fecha_ref)
    inicio_camp, fin_camp = campanas.fechas_de_campana(codigo_prd, campana_actual)

    with col_info:
        st.markdown(
            f"<div style='padding-top:28px;'>"
            f"<b>{display_prd}</b> · Campana {campana_actual} "
            f"({inicio_camp:%d-%b-%Y} → {fin_camp:%d-%b-%Y}) · "
            f"<span style='color:{BLOOMBERG_PALETTE['accent']};'>"
            f"Dia {campanas.dia_de_campana(codigo_prd, fecha_ref)}</span>"
            f" de {(fin_camp - inicio_camp).days + 1}"
            f"</div>",
            unsafe_allow_html=True,
        )

    # Traemos las ultimas 6 campanas (actual + 5 anteriores).
    campanas_ant = campanas.campanas_anteriores(codigo_prd, fecha_ref, n=5, incluir_actual=True)
    rango_desde, rango_hasta = campanas.filtro_rango_campanas(codigo_prd, campanas_ant)

    df_prd = cached_producto_historico(codigo_prd, rango_desde, rango_hasta)

    if df_prd.empty:
        st.info(f"Sin datos de {display_prd} en las ultimas 6 campanas.")
    else:
        df_prd["fecha_consulta"] = pd.to_datetime(df_prd["fecha_consulta"])

        # Dedupe del line-up para metricas de FLUJO (toneladas embarcadas
        # por dia/campana). Cada buque aparece en cada snapshot diario hasta
        # que zarpa; sumar quantity a traves de fecha_consulta infla las
        # toneladas ~10x. Solucion: para cada buque tomar la ultima foto
        # (todas las sub-filas de splits a destinos/puertos quedan) y
        # asignar el embarque al dia/campana de su ETB (fallback ETA o
        # fecha_consulta). Asi un buque cuenta UNA vez, en su mes real de
        # carga, aun si aparecio en 30 snapshots.
        df_prd = df_prd.sort_values("fecha_consulta")
        ult_foto_prd = df_prd.groupby("vessel")["fecha_consulta"].transform("max")
        df_prd = df_prd[df_prd["fecha_consulta"] == ult_foto_prd].copy()
        df_prd["fecha_carga"] = pd.to_datetime(
            df_prd["etb"].fillna(df_prd["eta"]).fillna(df_prd["fecha_consulta"])
        ).dt.date

        # PERF: evitamos `.apply` por fila (ejecutaria campana_de cientos de
        # veces). Mapeamos sobre las fechas unicas y propagamos con `.map`.
        fechas_unicas = pd.Series(df_prd["fecha_carga"].unique())
        mapa_camp = {f: campanas.campana_de(codigo_prd, f) for f in fechas_unicas}
        mapa_dia = {f: campanas.dia_de_campana(codigo_prd, f) for f in fechas_unicas}
        df_prd["campana"] = df_prd["fecha_carga"].map(mapa_camp)
        df_prd["dia_campana"] = df_prd["fecha_carga"].map(mapa_dia)

        # ------------------- KPIs -------------------
        df_actual = df_prd[df_prd["campana"] == campana_actual]
        df_a_la_fecha = df_actual[df_actual["dia_campana"] <= campanas.dia_de_campana(codigo_prd, fecha_ref)]

        # Acumulado a la misma altura en campanas anteriores.
        dia_actual = campanas.dia_de_campana(codigo_prd, fecha_ref)
        acumulados_hist = []
        for camp in campanas_ant[1:]:  # solo las 5 anteriores
            sub = df_prd[(df_prd["campana"] == camp) & (df_prd["dia_campana"] <= dia_actual)]
            acumulados_hist.append(sub["quantity"].fillna(0).sum())

        tons_actual = df_a_la_fecha["quantity"].fillna(0).sum()
        tons_mediana_hist = np.median(acumulados_hist) if acumulados_hist else 0
        tons_promedio_hist = np.mean(acumulados_hist) if acumulados_hist else 0

        # KPI "% Produccion MAGyP embarcado": traer estimacion del MAGyP para
        # la ultima campana cerrada del producto y calcular cuanta produccion
        # ya se embarco. Es el indicador de posicion en la curva de campana
        # (% en la curva de embarque de produccion total esperada).
        prod_magyp_tm = None
        prod_magyp_camp = None
        df_estim_kpi = cached_estimaciones()
        if not df_estim_kpi.empty:
            tot_kpi = estim_mod.totales_nacionales_por_campania(df_estim_kpi)
            cand = tot_kpi[tot_kpi["codigo_interno"] == codigo_prd].sort_values(
                "campania", ascending=False
            )
            if not cand.empty:
                prod_magyp_tm = float(cand.iloc[0]["produccion_tm"])
                prod_magyp_camp = cand.iloc[0]["campania"]

        pct_magyp = (tons_actual / prod_magyp_tm * 100) if prod_magyp_tm else None

        # Ritmo semanal: ultima semana vs misma semana de la campana anterior.
        # Detecta desaceleraciones antes de que se vean en el acumulado.
        semana_prd_actual = df_actual[df_actual["dia_campana"].between(
            max(1, dia_actual - 6), dia_actual
        )]["quantity"].fillna(0).sum()
        semana_prd_ant_acum = []
        for camp in campanas_ant[1:]:
            sub_s = df_prd[(df_prd["campana"] == camp) &
                           (df_prd["dia_campana"].between(max(1, dia_actual - 6), dia_actual))]
            semana_prd_ant_acum.append(sub_s["quantity"].fillna(0).sum())
        mediana_semana_hist = np.median(semana_prd_ant_acum) if semana_prd_ant_acum else 0

        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric(f"Acumulado {campana_actual} a hoy", fmt_tons(tons_actual))
        k2.metric("Mediana ultimas 5", fmt_tons(tons_mediana_hist))
        k3.metric("vs mediana", pct_change(tons_actual, tons_mediana_hist))
        k4.metric(
            "Ritmo ult 7d",
            fmt_tons(semana_prd_actual),
            delta=pct_change(semana_prd_actual, mediana_semana_hist),
            help="Toneladas últimos 7 días vs mismo período en campañas anteriores. Señal temprana de aceleración/desaceleración.",
        )
        if pct_magyp is not None:
            k5.metric(
                "% prod MAGyP",
                f"{pct_magyp:.1f}%",
                delta=f"{prod_magyp_tm/1_000_000:.1f} Mt ({prod_magyp_camp})",
                delta_color="off",
            )
        else:
            k5.metric("Promedio ultimas 5", fmt_tons(tons_promedio_hist))

        st.divider()

        # ------------------- Chart principal: campana actual vs historicas -------------------
        st.subheader(f"Cargamento acumulado · dia-de-campana (vs ultimas 5)")

        # Construir serie acumulada por campana.
        series_por_camp = {}
        for camp in campanas_ant:
            sub = df_prd[df_prd["campana"] == camp].copy()
            if sub.empty:
                continue
            diario = sub.groupby("dia_campana")["quantity"].sum().reset_index()
            diario = diario.sort_values("dia_campana")
            diario["acum"] = diario["quantity"].cumsum()
            # Reindexar al rango completo (1..N) para que las lineas sean suaves.
            total_dias = (fin_camp - inicio_camp).days + 1
            reix = pd.DataFrame({"dia_campana": range(1, total_dias + 1)})
            diario = reix.merge(diario, on="dia_campana", how="left")
            diario["acum"] = diario["acum"].ffill().fillna(0)
            series_por_camp[camp] = diario

        # Banda p10-p90 de las 5 anteriores.
        historicas = [s for c, s in series_por_camp.items() if c != campana_actual]
        if historicas:
            mat = np.array([s["acum"].values for s in historicas])
            p10 = np.percentile(mat, 10, axis=0)
            p90 = np.percentile(mat, 90, axis=0)
            p50 = np.percentile(mat, 50, axis=0)
        else:
            p10 = p90 = p50 = None

        fig_prd = go.Figure()

        # Banda p10-p90.
        if p10 is not None:
            x_dias = list(range(1, len(p10) + 1))
            fig_prd.add_scatter(
                x=x_dias + x_dias[::-1],
                y=list(p90) + list(p10)[::-1],
                fill="toself", fillcolor="rgba(255,153,0,0.15)",
                line=dict(color="rgba(0,0,0,0)"),
                name="p10-p90 (5 ultimas)", showlegend=True,
            )
            fig_prd.add_scatter(
                x=x_dias, y=p50, mode="lines",
                name="Mediana 5 ultimas",
                line=dict(color=BLOOMBERG_PALETTE["warning"], width=2, dash="dash"),
            )

        # Campanas individuales anteriores (lineas finas grises).
        for camp, serie in series_por_camp.items():
            if camp == campana_actual:
                continue
            fig_prd.add_scatter(
                x=serie["dia_campana"], y=serie["acum"],
                mode="lines", name=camp,
                line=dict(color=BLOOMBERG_PALETTE["text_muted"], width=1),
                opacity=0.5,
            )

        # Campana actual en rojo.
        if campana_actual in series_por_camp:
            s_actual = series_por_camp[campana_actual]
            s_actual_hasta_hoy = s_actual[s_actual["dia_campana"] <= dia_actual]
            fig_prd.add_scatter(
                x=s_actual_hasta_hoy["dia_campana"], y=s_actual_hasta_hoy["acum"],
                mode="lines", name=f"{campana_actual} (actual)",
                line=dict(color=BLOOMBERG_PALETTE["negative"], width=3),
            )
            fig_prd.add_vline(
                x=dia_actual, line_dash="dot",
                line_color=BLOOMBERG_PALETTE["warning"],
                annotation_text="Hoy",
            )

        aplicar_tema(fig_prd)
        fig_prd.update_layout(
            height=460,
            xaxis_title="Dia de campana",
            yaxis_title="Toneladas acumuladas",
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
        )
        st.plotly_chart(fig_prd, use_container_width=True)

        # ------------------- Tabla comparativa -------------------
        st.caption("Comparativa acumulado al mismo dia-de-campana")
        tabla_cmp = []
        for camp in campanas_ant:
            sub = df_prd[(df_prd["campana"] == camp) & (df_prd["dia_campana"] <= dia_actual)]
            tons = sub["quantity"].fillna(0).sum()
            buques = sub["vessel"].nunique()
            tabla_cmp.append({
                "Campana": camp,
                "Buques":  buques,
                "Tons":    fmt_tons(tons),
                "Tons_raw": tons,  # para ordenar
            })
        df_cmp = pd.DataFrame(tabla_cmp)
        df_cmp = df_cmp.sort_values("Campana", ascending=False)
        st.dataframe(
            df_cmp.drop(columns=["Tons_raw"]),
            use_container_width=True, hide_index=True,
        )
        _csv_productos = df_cmp.drop(columns=["Tons_raw"]).to_csv(index=False).encode("utf-8")
        st.download_button(
            label="⬇ Descargar CSV",
            data=_csv_productos,
            file_name=f"productos_{fecha_ref}.csv",
            mime="text/csv",
            key="dl_productos",
        )

        # ------------------- DJVE: ventas declaradas (MAGyP) -------------------
        st.divider()
        st.subheader(f"DJVE · ventas declaradas MAGyP · {display_prd}")
        st.caption(
            "Las DJVE (Ley 21.453) son registros oficiales de ventas al exterior. "
            "Un pico en DJVE anticipa actividad portuaria 30-60 dias despues."
        )

        df_djve = cached_djve(fecha_ref.year)
        if df_djve.empty:
            st.info(
                "No pude descargar DJVE del MAGyP. "
                "El servidor a veces devuelve 403/timeout; reintentar."
            )
        else:
            # Serie diaria de toneladas para este producto.
            serie_djve = fob_djve.djve_diarias(df_djve, codigo_interno=codigo_prd)

            if serie_djve.empty:
                st.info(f"Sin DJVE de {display_prd} en {fecha_ref.year}.")
            else:
                # Ultimos 60 dias para no saturar el grafico.
                corte = fecha_ref - timedelta(days=60)
                serie_djve = serie_djve[serie_djve["fecha_registro"] >= corte]

                # KPIs DJVE de los ultimos 30 dias + top exportador.
                djve_30d = fob_djve.djve_por_producto_recientes(
                    df_djve, dias=30, hasta=fecha_ref,
                )
                fila = djve_30d[djve_30d["codigo_interno"] == codigo_prd]

                if not fila.empty:
                    k1, k2, k3 = st.columns(3)
                    k1.metric("DJVE ultimos 30d (tons)",
                              fmt_tons(fila.iloc[0]["toneladas"]))
                    k2.metric("N° de declaraciones",
                              int(fila.iloc[0]["n_djve"]))
                    k3.metric("Top exportador",
                              fila.iloc[0]["razon_social_top"][:25])

                # Chart: barras diarias.
                if not serie_djve.empty:
                    fig_djve = go.Figure()
                    fig_djve.add_bar(
                        x=serie_djve["fecha_registro"],
                        y=serie_djve["toneladas"],
                        marker_color=BLOOMBERG_PALETTE["accent_blue"],
                        opacity=0.75,
                        name="DJVE diarias",
                    )
                    # MM 7d overlay.
                    serie_djve["ma7"] = serie_djve["toneladas"].rolling(7, min_periods=1).mean()
                    fig_djve.add_scatter(
                        x=serie_djve["fecha_registro"], y=serie_djve["ma7"],
                        mode="lines", name="MM 7d",
                        line=dict(color=BLOOMBERG_PALETTE["accent"], width=2),
                    )
                    aplicar_tema(fig_djve)
                    fig_djve.update_layout(
                        height=300, yaxis_title="Toneladas declaradas",
                        legend=dict(orientation="h", yanchor="bottom", y=1.02),
                    )
                    st.plotly_chart(fig_djve, use_container_width=True)

        # ------------------- Waterfall: DJVE vs Line-up (la "panza") ----------
        # Cruce mensual entre lo VENDIDO (DJVE registrada) y lo EMBARCADO
        # (line-up cargado). La diferencia es la panza de oferta pendiente:
        # ventas comprometidas que aun no se cargaron y van a presionar
        # precios cuando se acerquen a delivery. Es la pregunta que el trader
        # hace todos los dias y antes habia que restar mentalmente entre dos
        # graficos separados.
        if not df_djve.empty:
            st.divider()
            st.subheader(f"Oferta pendiente · DJVE vs embarcado · {display_prd}")
            st.caption(
                "Ventas declaradas (DJVE) vs cargas en line-up por mes. "
                "Pendiente = DJVE - Embarcado: ventas que faltan cargar. "
                "Pico de pendiente anticipa presion de precios."
            )

            # Ultimos 6 meses incluyendo el actual.
            hoy_periodo = pd.Period(fecha_ref, freq="M")
            periodos = [(hoy_periodo - i) for i in range(5, -1, -1)]

            # DJVE mensual filtrado por producto seleccionado.
            dj_prd = df_djve[df_djve["codigo_interno"] == codigo_prd].copy()
            if not dj_prd.empty:
                dj_prd["mes"] = pd.to_datetime(dj_prd["fecha_registro"]).dt.to_period("M")
                djve_mensual = (
                    dj_prd.groupby("mes")["toneladas"].sum()
                    .reindex(periodos, fill_value=0)
                )
            else:
                djve_mensual = pd.Series(0, index=periodos, dtype=float)

            # df_prd ya viene deduplicado arriba (una fila por buque, en su
            # ultima foto). Solo agregamos la columna mensual de carga.
            lu_prd = df_prd.copy()
            lu_prd["mes_carga"] = pd.to_datetime(
                lu_prd["etb"].fillna(lu_prd["eta"]).fillna(lu_prd["fecha_consulta"])
            ).dt.to_period("M")
            lineup_mensual = (
                lu_prd.groupby("mes_carga")["quantity"].sum()
                .reindex(periodos, fill_value=0)
            )

            # Pendiente = max(0, DJVE - Line-up). Clip a 0 porque a veces el
            # line-up acumula carga de DJVE de meses anteriores (la pendiente
            # negativa no es informacion accionable).
            pendiente = (djve_mensual - lineup_mensual).clip(lower=0)

            # Labels legibles: "Abr 26" en lugar de "2026-04".
            labels = [f"{p.strftime('%b')} {p.year % 100:02d}" for p in periodos]

            fig_wf = go.Figure()
            fig_wf.add_bar(
                x=labels, y=djve_mensual.values,
                name="DJVE registrada",
                marker_color=BLOOMBERG_PALETTE["accent_blue"],
                text=[fmt_tons(v) for v in djve_mensual.values],
                textposition="outside",
            )
            fig_wf.add_bar(
                x=labels, y=lineup_mensual.values,
                name="Line-up cargado",
                marker_color=BLOOMBERG_PALETTE["accent"],
                text=[fmt_tons(v) for v in lineup_mensual.values],
                textposition="outside",
            )
            fig_wf.add_bar(
                x=labels, y=pendiente.values,
                name="Pendiente (panza)",
                marker_color=BLOOMBERG_PALETTE["negative"],
                text=[fmt_tons(v) if v > 0 else "" for v in pendiente.values],
                textposition="outside",
            )
            aplicar_tema(fig_wf)
            fig_wf.update_layout(
                height=380,
                barmode="group",
                yaxis_title="Toneladas",
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
            )
            st.plotly_chart(fig_wf, use_container_width=True)

            # KPI sintetico: pendiente acumulada en los 6 meses + ratio.
            total_djve = float(djve_mensual.sum())
            total_lineup = float(lineup_mensual.sum())
            total_pend = float(pendiente.sum())
            ratio_pend = (total_pend / total_djve * 100) if total_djve else 0

            # Dias de cobertura: pendiente / ritmo diario promedio 30d.
            ritmo_30d = semana_prd_actual / 7 if semana_prd_actual else None
            dias_cobertura = int(total_pend / ritmo_30d) if ritmo_30d else None

            wk1, wk2, wk3, wk4 = st.columns(4)
            wk1.metric("DJVE 6 meses", fmt_tons(total_djve))
            wk2.metric("Embarcado 6 meses", fmt_tons(total_lineup))
            wk3.metric(
                "📦 Pendiente (panza)",
                fmt_tons(total_pend),
                delta=f"{ratio_pend:.0f}% de DJVE sin cargar",
                delta_color="inverse",
                help="Ventas declaradas (DJVE) menos lo ya embarcado en line-up. Es la demanda forward implícita que presionará precios.",
            )
            if dias_cobertura is not None:
                wk4.metric(
                    "Días de cobertura",
                    f"{dias_cobertura}d",
                    delta="al ritmo de últimos 7d",
                    delta_color="off",
                    help="Cuántos días tardaría en embarcar todo el pendiente al ritmo actual.",
                )
            else:
                wk4.metric("Cobertura", "—")

        # ------------------- Top shippers de este producto -------------------
        st.divider()
        st.subheader(f"Top shippers de {display_prd} · campana {campana_actual}")
        top_shp = (
            df_actual.groupby("shipper_canon")["quantity"].sum()
            .sort_values(ascending=True).reset_index()
        )
        top_shp = top_shp[top_shp["quantity"] > 0]
        colors_shp = [SHIPPER_COLORS.get(s, SHIPPER_COLORS["OTROS"]) for s in top_shp["shipper_canon"]]

        fig_shp_prd = go.Figure()
        fig_shp_prd.add_bar(
            x=top_shp["quantity"], y=top_shp["shipper_canon"],
            orientation="h", marker_color=colors_shp,
            text=top_shp["quantity"].apply(fmt_tons), textposition="outside",
        )
        aplicar_tema(fig_shp_prd)
        fig_shp_prd.update_layout(height=340, showlegend=False)
        st.plotly_chart(fig_shp_prd, use_container_width=True)

        # ------------------- Estimaciones MAGyP: contexto macro ---------------
        st.divider()
        st.subheader(f"Produccion historica · {display_prd} (MAGyP)")
        st.caption(
            "Estimaciones oficiales MAGyP por campana cerrada. "
            "Util para dimensionar el line-up contra produccion real. "
            "Fuente: datos.magyp.gob.ar (actualiza cada ~6 meses)."
        )

        df_estim = cached_estimaciones()
        if df_estim.empty:
            st.warning(
                "No pude descargar las estimaciones del MAGyP. "
                "Reintentar mas tarde o consultar manualmente el dataset."
            )
        else:
            totales = estim_mod.totales_nacionales_por_campania(df_estim)
            ult = estim_mod.ultima_campania_por_cultivo(totales, codigo_prd, n=6)
            ult = estim_mod.variacion_vs_campania_anterior(ult)

            if ult.empty:
                st.info(
                    f"MAGyP no reporta {display_prd} como cultivo separado "
                    "(ej. harinas y aceites no estan en esta fuente)."
                )
            else:
                # KPIs: ultima campana vs anterior.
                fila_ult = ult.iloc[0]
                fila_ant = ult.iloc[1] if len(ult) > 1 else None

                k1, k2, k3, k4 = st.columns(4)
                k1.metric(
                    f"Produccion {fila_ult['campania']}",
                    f"{fila_ult['produccion_tm'] / 1_000_000:.1f} Mt",
                    delta=(f"{fila_ult['pct_vs_anterior']:+.1f}% vs anterior"
                           if pd.notna(fila_ult.get("pct_vs_anterior")) else None),
                )
                k2.metric(
                    "Area sembrada",
                    f"{fila_ult['sembrada_ha'] / 1_000_000:.2f} M ha",
                )
                k3.metric(
                    "Rinde nacional",
                    f"{int(fila_ult['rinde_kgxha']):,} kg/ha",
                )
                if fila_ant is not None:
                    k4.metric(
                        f"Vs {fila_ant['campania']}",
                        f"{fila_ant['produccion_tm'] / 1_000_000:.1f} Mt",
                    )

                # Grafico de barras: ultimas 6 campanas.
                ult_asc = ult.sort_values("campania", ascending=True).reset_index(drop=True)
                # Color del cultivo. PRODUCTO_DISPLAY mapea codigo -> nombre
                # (string), no es un mapa de colores; usamos el accent del
                # tema Bloomberg como color por defecto del cultivo.
                color_estim = BLOOMBERG_PALETTE["accent"]

                fig_estim = go.Figure()
                fig_estim.add_bar(
                    x=ult_asc["campania"],
                    y=ult_asc["produccion_tm"] / 1_000_000,
                    marker_color=color_estim,
                    text=[f"{v:.1f} Mt" for v in ult_asc["produccion_tm"] / 1_000_000],
                    textposition="outside",
                    name="Produccion",
                )
                aplicar_tema(fig_estim)
                fig_estim.update_layout(
                    height=320,
                    yaxis_title="Produccion (Mt)",
                    xaxis_title="Campana",
                    showlegend=False,
                )
                st.plotly_chart(fig_estim, use_container_width=True)

                # Tabla de detalle.
                tabla_estim = ult.copy()
                tabla_estim["Produccion"] = (tabla_estim["produccion_tm"] / 1_000_000).round(2).astype(str) + " Mt"
                tabla_estim["Sembrada"] = (tabla_estim["sembrada_ha"] / 1_000_000).round(2).astype(str) + " M ha"
                tabla_estim["Rinde"] = tabla_estim["rinde_kgxha"].apply(lambda v: f"{int(v):,} kg/ha")
                tabla_estim["Var %"] = tabla_estim["pct_vs_anterior"].apply(
                    lambda v: f"{v:+.1f}%" if pd.notna(v) else "-"
                )
                st.dataframe(
                    tabla_estim[["campania", "Produccion", "Sembrada", "Rinde", "Var %"]]
                    .rename(columns={"campania": "Campana"}),
                    use_container_width=True, hide_index=True,
                )

        # Links a reportes externos (BCBA + BCR) que no se pueden scrapear.
        with st.expander("Reportes semanales externos (BCBA / BCR)"):
            st.caption(
                "Para estimaciones de la campana EN CURSO (no incluidas en MAGyP) "
                "consultar manualmente estos reportes semanales:"
            )
            for nombre, meta in estim_mod.links_reportes_semanales().items():
                st.markdown(
                    f"**[{meta['nombre']}]({meta['url']})**  \n"
                    f"Frecuencia: {meta['frecuencia']}  \n"
                    f"Cubre: {meta['cubre']}"
                )


with tab_prd:
    _render_productos_tab(fecha_ref)


# ==========================================================================
# PESTANA 4: CONGESTION
# ==========================================================================

@st.fragment
def _render_congestion_tab(fecha_ref):
    """
    Render de la pestaña Congestión. Como @st.fragment, cambiar cualquier
    filtro solo rerenderiza esta función sin tocar el resto del dashboard.
    """
    st.subheader(f"Buques en puerto ahora · {fecha_ref}")

    df_en_puerto = cached_en_puerto_ahora(fecha_ref)

    if df_en_puerto.empty:
        st.info(
            f"No hay buques con ETB <= {fecha_ref} <= ETS en el line-up de ese dia. "
            "Puede ser por feriado o porque la fuente no tiene etb/ets cargados."
        )
    else:
        # ------------------- KPIs por zona -------------------
        agg_zona = (
            df_en_puerto.groupby("zona")
            .agg(buques=("vessel", "nunique"), tons=("quantity", "sum"))
            .reset_index()
        )

        cols = st.columns(len(agg_zona))
        for idx, (_, row) in enumerate(agg_zona.iterrows()):
            cols[idx].metric(row["zona"], row["buques"], delta=fmt_tons(row["tons"]))

        st.divider()

        # ------------------- Tabla filtrable -------------------
        st.caption("Detalle por puerto")

        # Preparar DataFrame completo con todas las columnas antes de filtrar.
        df_det = df_en_puerto.assign(
            dias_en_puerto=lambda x: (
                pd.to_datetime(fecha_ref) - pd.to_datetime(x["etb"])
            ).dt.days,
            demora_eta=lambda x: (
                pd.to_datetime(x["etb"]) - pd.to_datetime(x["eta"])
            ).dt.days,
            destino=lambda x: x["dest_orig"].fillna("s/d").str.strip().str.upper(),
        )[
            ["zona", "port", "vessel", "cargo", "quantity", "shipper_canon",
             "destino", "eta", "etb", "ets", "dias_en_puerto", "demora_eta", "remarks"]
        ].rename(columns={
            "zona": "Zona", "port": "Puerto", "vessel": "Buque",
            "cargo": "Producto", "quantity": "Tons",
            "shipper_canon": "Shipper", "destino": "Destino",
            "eta": "ETA orig", "etb": "ETB", "ets": "ETS",
            "dias_en_puerto": "Dias en puerto",
            "demora_eta": "Demora ETA (d)",
            "remarks": "Estado",
        })
        df_det = df_det.sort_values(["Zona", "Puerto"])

        # Filtros: Producto, Shipper, Zona, Destino en una fila.
        fc1, fc2, fc3, fc4 = st.columns(4)
        f_prod  = fc1.multiselect(
            "Producto", sorted(df_det["Producto"].dropna().unique()), key="cng_prod")
        f_ship  = fc2.multiselect(
            "Shipper",  sorted(df_det["Shipper"].dropna().unique()),  key="cng_ship")
        f_zona  = fc3.multiselect(
            "Zona",     sorted(df_det["Zona"].dropna().unique()),     key="cng_zona")
        f_dest  = fc4.multiselect(
            "Destino",  sorted(df_det["Destino"].dropna().unique()),  key="cng_dest")

        mask = pd.Series(True, index=df_det.index)
        if f_prod:  mask &= df_det["Producto"].isin(f_prod)
        if f_ship:  mask &= df_det["Shipper"].isin(f_ship)
        if f_zona:  mask &= df_det["Zona"].isin(f_zona)
        if f_dest:  mask &= df_det["Destino"].isin(f_dest)

        df_filtrado = df_det[mask]
        st.dataframe(df_filtrado, use_container_width=True, hide_index=True, height=480)
        _csv_congestion = df_filtrado.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="⬇ Descargar CSV",
            data=_csv_congestion,
            file_name=f"congestion_{fecha_ref}.csv",
            mime="text/csv",
            key="dl_congestion",
        )
        st.caption(f"**{mask.sum()}** de {len(df_det)} buques · filtros activos: "
                   f"{sum(bool(f) for f in [f_prod,f_ship,f_zona,f_dest])}/4")

        st.divider()

        # ------------------- Evolucion de congestion (ultimos 30d) -------------------
        st.caption("Evolucion de buques simultaneos en puerto · ultimos 30 dias")
        fechas_serie = pd.date_range(end=fecha_ref, periods=30, freq="D").date
        df_cong = cached_serie_congestion(fechas_serie[0], fechas_serie[-1])

        if df_cong.empty:
            st.info("Sin data de congestion en los ultimos 30 dias.")
        else:
            fig_cong = px.line(
                df_cong, x="fecha", y="buques", color="zona",
                labels={"fecha": "", "buques": "Buques simultaneos", "zona": "Zona"},
                color_discrete_sequence=[
                    BLOOMBERG_PALETTE["accent"],
                    BLOOMBERG_PALETTE["accent_blue"],
                    BLOOMBERG_PALETTE["positive"],
                    BLOOMBERG_PALETTE["warning"],
                ],
            )
            # pd.Timestamp para evitar el bug de _mean en plotly sobre date.
            fig_cong.add_vline(
                x=pd.Timestamp(fecha_ref), line_dash="dot",
                line_color=BLOOMBERG_PALETTE["warning"],
            )
            aplicar_tema(fig_cong)
            fig_cong.update_layout(height=360, legend=dict(orientation="h", yanchor="bottom", y=1.02))
            st.plotly_chart(fig_cong, use_container_width=True)

    # -------------------------------------------------------------------
    # Pronostico climatico 7 dias (4 zonas) via Open-Meteo
    # -------------------------------------------------------------------
    st.divider()
    st.subheader("Pronostico 7 dias · zonas portuarias")
    st.caption(
        "Lluvia >5mm o rafaga >40km/h puede parar operaciones (elevadores "
        "no cargan bajo lluvia, puertos cierran con vientos fuertes). "
        "Fuente: Open-Meteo."
    )

    pronosticos = cached_clima_zonas()

    for zona, df_clima in pronosticos.items():
        if df_clima.empty:
            st.warning(f"**{zona}**: no pude traer pronostico (Open-Meteo fallo).")
            continue

        df_clima = df_clima.copy()
        # Vectorización: precomputar las series y usar np.select.
        _codigo = df_clima["codigo"].fillna(0)
        _lluvia = df_clima["lluvia_mm"].fillna(0)
        _rafaga = df_clima["rafaga_kmh"].fillna(0)
        _prob   = df_clima["prob_lluvia"].fillna(0)
        _cond_alto  = (_codigo >= 95) | (_lluvia > 20) | (_rafaga > 60)
        _cond_medio = (_lluvia > 5) | (_rafaga > 40) | (_prob > 70)
        _cond_bajo  = (_lluvia > 1) | (_prob > 40)
        df_clima["riesgo"] = np.select(
            [_cond_alto, _cond_medio, _cond_bajo],
            ["🔴 ALTO", "🟡 MEDIO", "🟢 BAJO"],
            default="⚪ OK",
        )

        with st.expander(f"☁  {zona}", expanded=True):
            # Fila de 7 tarjetas compactas (una por dia).
            cols = st.columns(7)
            for i, (_, row) in enumerate(df_clima.iterrows()):
                with cols[i]:
                    # Card custom con HTML para controlar look Bloomberg.
                    color_borde = {
                        "🔴 ALTO":  BLOOMBERG_PALETTE["negative"],
                        "🟡 MEDIO": BLOOMBERG_PALETTE["warning"],
                        "🟢 BAJO":  BLOOMBERG_PALETTE["positive"],
                        "⚪ OK":    BLOOMBERG_PALETTE["text_muted"],
                    }.get(row["riesgo"], BLOOMBERG_PALETTE["text_muted"])

                    st.markdown(
                        f"""
                        <div style='
                            background:{BLOOMBERG_PALETTE["bg_card"]};
                            border:1px solid {color_borde};
                            border-radius:4px;
                            padding:8px 6px;
                            text-align:center;
                            font-family:Consolas,monospace;
                            min-height:140px;
                        '>
                            <div style='font-size:11px; color:{BLOOMBERG_PALETTE["text_muted"]};'>
                                {row["fecha"].strftime("%a %d-%b")}
                            </div>
                            <div style='font-size:32px; margin:4px 0;'>{row["emoji"]}</div>
                            <div style='font-size:13px; color:{BLOOMBERG_PALETTE["accent"]}; font-weight:700;'>
                                {row["t_min"]:.0f}° / {row["t_max"]:.0f}°C
                            </div>
                            <div style='font-size:11px; color:{BLOOMBERG_PALETTE["accent_blue"]};'>
                                💧 {row["lluvia_mm"]:.1f}mm ({row["prob_lluvia"]:.0f}%)
                            </div>
                            <div style='font-size:11px; color:{BLOOMBERG_PALETTE["text_primary"]};'>
                                💨 {row["viento_kmh"]:.0f} km/h
                            </div>
                            <div style='font-size:10px; margin-top:4px;'>{row["riesgo"]}</div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )


with tab_cng:
    _render_congestion_tab(fecha_ref)


# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------

st.divider()
st.caption(
    f"Line-Up AR · v2 Bloomberg · data {fecha_min_db} → {fecha_max_db}"
)
