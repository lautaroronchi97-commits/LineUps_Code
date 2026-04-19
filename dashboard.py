"""
Dashboard Line-Up de puertos argentinos (agro trading).

Uso local:
    streamlit run dashboard.py

Abre automaticamente http://localhost:8501

Requisitos:
- .env con credenciales Supabase (o st.secrets en Streamlit Cloud).
- Al menos 1 fecha cargada en la DB (corr  python backfill.py primero).

Arquitectura:
- Cuatro pestanas especializadas por caso de uso.
- Queries cacheadas con @st.cache_data (TTL corto en "Hoy", largo en historicos).
- Plotly para graficos interactivos.
"""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import plotly.express as px
import streamlit as st

from config import (
    PRODUCTOS_ACEITES,
    PRODUCTOS_FERTILIZANTES,
    PRODUCTOS_GRANOS,
    PRODUCTOS_HARINAS,
)
from db import ping, query_lineup

# ---------------------------------------------------------------------------
# Configuracion general
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Line-Up Argentina",
    page_icon="🚢",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ---------------------------------------------------------------------------
# Cache helpers: aislamos las queries para reutilizar cache entre pestanas
# ---------------------------------------------------------------------------

@st.cache_data(ttl=60, show_spinner="Consultando movimiento de hoy...")
def cached_hoy(fecha: date) -> pd.DataFrame:
    """Movimiento de una fecha. Cache 60s porque puede actualizarse."""
    return query_lineup(fecha_desde=fecha, fecha_hasta=fecha)


@st.cache_data(ttl=600, show_spinner="Consultando historico...")
def cached_historico(desde: date, hasta: date) -> pd.DataFrame:
    """Historico paginado. Cache 10 min (los datos viejos no cambian)."""
    return query_lineup(fecha_desde=desde, fecha_hasta=hasta)


@st.cache_data(ttl=600)
def cached_rango_disponible() -> tuple[date, date]:
    """Primera y ultima fecha cargadas en la DB."""
    df = query_lineup()  # trae todo (ok para fechas distintas con agg)
    if df.empty:
        hoy = date.today()
        return hoy, hoy
    fechas = pd.to_datetime(df["fecha_consulta"]).dt.date
    return fechas.min(), fechas.max()


@st.cache_data(ttl=60)
def cached_ping() -> dict:
    return ping()


# ---------------------------------------------------------------------------
# Chequeo inicial: DB vacia
# ---------------------------------------------------------------------------

estado = cached_ping()
if not estado["conectado"]:
    st.error(f"No puedo conectarme a Supabase: {estado['error']}")
    st.info(
        "Verifica que el archivo `.env` (o los secrets de Streamlit Cloud) "
        "tengan SUPABASE_URL y SUPABASE_SERVICE_ROLE_KEY validos."
    )
    st.stop()

if estado["cantidad_filas"] == 0:
    st.warning(
        "La tabla `lineup` esta vacia. "
        "Antes de usar el dashboard, cargamos datos historicos corriendo:\n\n"
        "```\npython backfill.py\n```"
    )
    st.stop()

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

st.title("🚢 Line-Up Puertos Argentinos")
st.caption(f"{estado['cantidad_filas']:,} movimientos cargados · fuente: ISA Agents")

fecha_min_db, fecha_max_db = cached_rango_disponible()

# ---------------------------------------------------------------------------
# Pestanas
# ---------------------------------------------------------------------------

tab_hoy, tab_hist, tab_explora, tab_ahora = st.tabs(
    ["📅 Hoy", "📊 Comparativa histórica", "🔍 Exploración", "⚓ En puerto ahora"]
)


# ==========================================================================
# PESTANA 1: HOY
# ==========================================================================

with tab_hoy:
    st.subheader("Movimiento del día")

    col_fecha, _ = st.columns([1, 4])
    with col_fecha:
        # Default: la fecha mas reciente disponible en la DB (no literalmente hoy,
        # porque a las 9am quizas la fuente todavia no publico).
        fecha_sel = st.date_input(
            "Fecha",
            value=fecha_max_db,
            min_value=fecha_min_db,
            max_value=fecha_max_db,
            format="YYYY-MM-DD",
            key="fecha_hoy",
        )

    df_hoy = cached_hoy(fecha_sel)

    if df_hoy.empty:
        st.info(f"No hay movimientos registrados para {fecha_sel} "
                "(fin de semana/feriado o falta la data en DB).")
    else:
        # -------------------- KPIs --------------------
        df_agro = df_hoy[df_hoy["es_agro"] == True]  # noqa: E712

        total_buques = df_hoy["vessel"].nunique()
        tons_agro = int(df_agro["quantity"].fillna(0).sum())
        cant_load = (df_hoy["ops"] == "LOAD").sum()
        cant_disch = (df_hoy["ops"] == "DISCH").sum()

        top_shipper_row = (
            df_hoy.groupby("shipper", dropna=True)["quantity"].sum().sort_values(ascending=False).head(1)
        )
        top_shipper = (
            f"{top_shipper_row.index[0]} ({int(top_shipper_row.iloc[0]):,}t)"
            if not top_shipper_row.empty else "—"
        )

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Buques (únicos)", total_buques)
        k2.metric("Toneladas agro", f"{tons_agro:,}")
        k3.metric("LOAD / DISCH", f"{cant_load} / {cant_disch}")
        k4.metric("Top shipper", top_shipper)

        # -------------------- Filtros sidebar --------------------
        with st.sidebar:
            st.header("Filtros - Hoy")
            port_sel = st.multiselect("Puerto", sorted(df_hoy["port"].dropna().unique()))
            cat_sel = st.multiselect("Categoría", sorted(df_hoy["cat"].dropna().unique()))
            cargo_sel = st.multiselect("Producto", sorted(df_hoy["cargo"].dropna().unique()))
            shipper_sel = st.multiselect("Shipper", sorted(df_hoy["shipper"].dropna().unique()))
            ops_sel = st.multiselect("Operación", sorted(df_hoy["ops"].dropna().unique()))

        df_filtrado = df_hoy.copy()
        if port_sel:
            df_filtrado = df_filtrado[df_filtrado["port"].isin(port_sel)]
        if cat_sel:
            df_filtrado = df_filtrado[df_filtrado["cat"].isin(cat_sel)]
        if cargo_sel:
            df_filtrado = df_filtrado[df_filtrado["cargo"].isin(cargo_sel)]
        if shipper_sel:
            df_filtrado = df_filtrado[df_filtrado["shipper"].isin(shipper_sel)]
        if ops_sel:
            df_filtrado = df_filtrado[df_filtrado["ops"].isin(ops_sel)]

        st.dataframe(
            df_filtrado[
                ["port", "berth", "vessel", "ops", "cat", "cargo", "quantity",
                 "dest_orig", "shipper", "eta", "etb", "ets", "remarks"]
            ],
            use_container_width=True,
            height=340,
        )

        # -------------------- Graficos --------------------
        st.divider()
        g1, g2 = st.columns(2)

        with g1:
            st.markdown("**Toneladas por producto (solo agro)**")
            por_cargo = (
                df_agro.groupby("cargo", dropna=True)["quantity"].sum()
                .sort_values(ascending=True).tail(15).reset_index()
            )
            if por_cargo.empty:
                st.info("Sin data agro este dia.")
            else:
                fig = px.bar(por_cargo, x="quantity", y="cargo", orientation="h",
                             labels={"quantity": "Toneladas", "cargo": "Producto"})
                fig.update_layout(height=400, margin=dict(l=10, r=10, t=30, b=10))
                st.plotly_chart(fig, use_container_width=True)

        with g2:
            st.markdown("**Distribución por puerto**")
            por_puerto = (
                df_hoy.groupby("port", dropna=True)["quantity"].sum()
                .sort_values(ascending=False).head(12).reset_index()
            )
            if por_puerto.empty:
                st.info("Sin data.")
            else:
                fig = px.pie(por_puerto, values="quantity", names="port", hole=0.35)
                fig.update_layout(height=400, margin=dict(l=10, r=10, t=30, b=10))
                st.plotly_chart(fig, use_container_width=True)

        st.markdown("**Heatmap: toneladas por puerto × producto (agro)**")
        piv = (
            df_agro.dropna(subset=["port", "cargo"])
            .pivot_table(index="port", columns="cargo", values="quantity",
                         aggfunc="sum", fill_value=0)
        )
        if piv.empty:
            st.info("Sin data agro para construir el heatmap.")
        else:
            fig = px.imshow(piv, aspect="auto", color_continuous_scale="Blues",
                            labels={"color": "Toneladas"})
            fig.update_layout(height=420, margin=dict(l=10, r=10, t=30, b=10))
            st.plotly_chart(fig, use_container_width=True)


# ==========================================================================
# PESTANA 2: COMPARATIVA HISTORICA
# ==========================================================================

with tab_hist:
    st.subheader("Toneladas diarias - comparativa histórica")

    col_prod, col_ref = st.columns([2, 1])
    with col_prod:
        productos_default = ["MAIZE", "WHEAT", "SBM", "SBO"]
        productos_opciones = sorted(set(
            PRODUCTOS_GRANOS + PRODUCTOS_HARINAS + PRODUCTOS_ACEITES + PRODUCTOS_FERTILIZANTES
        ))
        productos_sel = st.multiselect(
            "Producto(s)",
            options=productos_opciones,
            default=productos_default,
            key="prods_hist",
        )
    with col_ref:
        fecha_ref = st.date_input(
            "Fecha de referencia",
            value=fecha_max_db, min_value=fecha_min_db, max_value=fecha_max_db,
            format="YYYY-MM-DD", key="fecha_ref_hist",
        )

    if not productos_sel:
        st.info("Elegí al menos un producto.")
    else:
        df_h = cached_historico(fecha_min_db, fecha_max_db)
        df_h = df_h[df_h["cargo"].isin(productos_sel)].copy()

        if df_h.empty:
            st.warning("No hay data histórica para esos productos.")
        else:
            df_h["fecha_consulta"] = pd.to_datetime(df_h["fecha_consulta"])
            # Solo LOAD: lo que nos interesa para exportacion.
            df_h = df_h[df_h["ops"] == "LOAD"]

            serie = (
                df_h.groupby(["fecha_consulta", "cargo"])["quantity"].sum()
                .reset_index()
            )

            fig = px.line(
                serie, x="fecha_consulta", y="quantity", color="cargo",
                labels={"fecha_consulta": "Fecha", "quantity": "Toneladas cargando (LOAD)"},
            )
            fig.add_vline(x=pd.to_datetime(fecha_ref), line_dash="dash",
                          line_color="red", annotation_text=" referencia")
            fig.update_layout(height=420, margin=dict(l=10, r=10, t=30, b=10))
            st.plotly_chart(fig, use_container_width=True)

            # -------------------- Bandas: promedio historico mismo mes --------------------
            st.markdown("**Hoy vs promedio histórico del mismo mes (ultimos 5 años)**")

            df_h["anio"] = df_h["fecha_consulta"].dt.year
            df_h["mes"] = df_h["fecha_consulta"].dt.month
            mes_ref = fecha_ref.month
            anio_ref = fecha_ref.year

            tabla_filas = []
            for prod in productos_sel:
                sub = df_h[df_h["cargo"] == prod]
                ref_tons = int(sub[(sub["anio"] == anio_ref) & (sub["mes"] == mes_ref)]["quantity"].sum())

                # Mismo mes de los 5 anios anteriores
                historico = sub[(sub["mes"] == mes_ref) & (sub["anio"] < anio_ref) & (sub["anio"] >= anio_ref - 5)]
                if historico.empty:
                    prom = None
                    desvio_pct = None
                else:
                    # Sumamos por anio y promediamos
                    tot_por_anio = historico.groupby("anio")["quantity"].sum()
                    prom = int(tot_por_anio.mean())
                    desvio_pct = (ref_tons - prom) / prom * 100 if prom > 0 else None

                tabla_filas.append({
                    "Producto": prod,
                    f"Ref ({anio_ref}-{mes_ref:02d}) t": ref_tons,
                    "Prom 5 años mismo mes t": prom if prom is not None else "—",
                    "Desvío %": f"{desvio_pct:+.1f}%" if desvio_pct is not None else "—",
                })

            st.dataframe(pd.DataFrame(tabla_filas), use_container_width=True, hide_index=True)


# ==========================================================================
# PESTANA 3: EXPLORACION
# ==========================================================================

with tab_explora:
    st.subheader("Exploración libre")

    col_d1, col_d2 = st.columns(2)
    with col_d1:
        desde = st.date_input("Desde", value=max(fecha_min_db, fecha_max_db - timedelta(days=90)),
                              min_value=fecha_min_db, max_value=fecha_max_db, key="expl_desde")
    with col_d2:
        hasta = st.date_input("Hasta", value=fecha_max_db,
                              min_value=fecha_min_db, max_value=fecha_max_db, key="expl_hasta")

    if desde > hasta:
        st.error("La fecha 'Desde' es posterior a 'Hasta'.")
        st.stop()

    df_e = cached_historico(desde, hasta)

    if df_e.empty:
        st.info("No hay data en este rango.")
    else:
        col_f1, col_f2, col_f3, col_f4 = st.columns(4)
        with col_f1:
            e_port = st.multiselect("Puerto", sorted(df_e["port"].dropna().unique()), key="expl_port")
        with col_f2:
            e_cargo = st.multiselect("Producto", sorted(df_e["cargo"].dropna().unique()), key="expl_cargo")
        with col_f3:
            e_ship = st.multiselect("Shipper", sorted(df_e["shipper"].dropna().unique()), key="expl_ship")
        with col_f4:
            e_dest = st.multiselect("Destino/Origen", sorted(df_e["dest_orig"].dropna().unique()), key="expl_dest")

        dff = df_e.copy()
        if e_port: dff = dff[dff["port"].isin(e_port)]
        if e_cargo: dff = dff[dff["cargo"].isin(e_cargo)]
        if e_ship: dff = dff[dff["shipper"].isin(e_ship)]
        if e_dest: dff = dff[dff["dest_orig"].isin(e_dest)]

        st.caption(f"{len(dff):,} filas")
        st.dataframe(dff, use_container_width=True, height=340)

        st.download_button(
            "📥 Descargar CSV",
            data=dff.to_csv(index=False).encode("utf-8"),
            file_name=f"lineup_{desde}_{hasta}.csv",
            mime="text/csv",
        )

        st.divider()
        g1, g2 = st.columns(2)

        with g1:
            st.markdown("**Evolución temporal**")
            if dff.empty:
                st.info("Sin data con los filtros actuales.")
            else:
                dff2 = dff.copy()
                dff2["fecha_consulta"] = pd.to_datetime(dff2["fecha_consulta"])
                serie = dff2.groupby("fecha_consulta")["quantity"].sum().reset_index()
                fig = px.line(serie, x="fecha_consulta", y="quantity",
                              labels={"fecha_consulta": "Fecha", "quantity": "Toneladas"})
                fig.update_layout(height=320, margin=dict(l=10, r=10, t=30, b=10))
                st.plotly_chart(fig, use_container_width=True)

        with g2:
            st.markdown("**Top shippers**")
            top_s = (dff.groupby("shipper", dropna=True)["quantity"].sum()
                     .sort_values(ascending=True).tail(15).reset_index())
            if top_s.empty:
                st.info("Sin data.")
            else:
                fig = px.bar(top_s, x="quantity", y="shipper", orientation="h",
                             labels={"quantity": "Toneladas", "shipper": "Shipper"})
                fig.update_layout(height=320, margin=dict(l=10, r=10, t=30, b=10))
                st.plotly_chart(fig, use_container_width=True)


# ==========================================================================
# PESTANA 4: BUQUES EN PUERTO AHORA
# ==========================================================================

with tab_ahora:
    st.subheader("Buques en puerto hoy (ETB ≤ hoy ≤ ETS)")

    hoy = date.today()
    # Buscamos en el ultimo scrape: filas de fecha_max_db cuyo etb <= hoy <= ets.
    df_ult = cached_hoy(fecha_max_db).copy()

    if df_ult.empty:
        st.info("No hay data del ultimo dia cargado.")
    else:
        # Convertir strings ISO a date para comparar.
        for col in ("etb", "ets"):
            if col in df_ult.columns:
                df_ult[col] = pd.to_datetime(df_ult[col], errors="coerce").dt.date

        mask = df_ult["etb"].notna() & df_ult["ets"].notna() & \
               (df_ult["etb"] <= hoy) & (df_ult["ets"] >= hoy)
        df_ahora = df_ult[mask].copy()

        if df_ahora.empty:
            st.info(f"Sin buques con ETB <= {hoy} <= ETS en el último día cargado ({fecha_max_db}).")
        else:
            def estado_emoji(r):
                if r == "LOADING": return "🔵 LOADING"
                if r == "DISCHARGING": return "🟠 DISCHARGING"
                if r == "CPTD": return "✅ CPTD"
                return "⏳ programado"

            df_ahora["estado"] = df_ahora["remarks"].apply(estado_emoji)

            for puerto, grupo in df_ahora.groupby("port"):
                st.markdown(f"### ⚓ {puerto}  ·  {len(grupo)} buques")
                st.dataframe(
                    grupo[["vessel", "berth", "ops", "cat", "cargo", "quantity",
                           "shipper", "dest_orig", "etb", "ets", "estado"]],
                    use_container_width=True,
                    hide_index=True,
                )


# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------

st.divider()
st.caption(f"Rango de fechas cargadas en DB: {fecha_min_db} → {fecha_max_db}")
