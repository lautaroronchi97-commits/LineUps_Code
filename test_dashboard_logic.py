"""
Smoke test que simula la logica de cada pestana sin levantar Streamlit.

Ejecuta las queries y transformaciones pesadas para detectar errores runtime
que `streamlit run` no muestra hasta que uno abre la pestana en el browser.
"""
from __future__ import annotations

import traceback
from datetime import date, timedelta

import numpy as np
import pandas as pd

import campanas
from config import (
    CODIGOS_PRIORITARIOS,
    PRODUCTO_DISPLAY,
    PRODUCTOS_PRIORITARIOS,
    SHIPPER_COLORS,
    zona_de_puerto,
)
from db import (
    primera_fecha_cargada,
    query_en_puerto_ahora,
    query_exports_prioritarios,
    query_lineup,
    ultima_fecha_cargada,
)
from shipper_norm import SHIPPERS_TOP, aplicar_a_dataframe


def _clasificar_estado(remarks):
    if remarks is None or (isinstance(remarks, float) and np.isnan(remarks)):
        return "ARRIBANDO"
    r = str(remarks).upper().strip()
    if "LOADING" in r or "DISCH" in r:
        return "CARGANDO"
    if "CPTD" in r or "SAIL" in r or "COMPL" in r:
        return "TERMINADO"
    return "ARRIBANDO"


def test_panorama(fecha_ref: date, ventana_dias: int = 30) -> None:
    print("\n=== PANORAMA ===")
    desde = fecha_ref - timedelta(days=90)
    df = query_exports_prioritarios(fecha_desde=desde, fecha_hasta=fecha_ref)
    print(f"query_exports_prioritarios({desde}, {fecha_ref}) -> shape={df.shape}")
    assert "shipper_canon" in df.columns
    assert "origen_alt" in df.columns

    df["fecha_consulta"] = pd.to_datetime(df["fecha_consulta"])
    df["estado"] = df["remarks"].apply(_clasificar_estado)
    df_hoy = df[df["fecha_consulta"].dt.date == fecha_ref]
    print(f"Hoy ({fecha_ref}): {len(df_hoy)} filas, {df_hoy['vessel'].nunique()} buques unicos")

    # Estados
    estados = df_hoy["estado"].value_counts()
    print(f"Estados: {estados.to_dict()}")

    # Resumen por producto
    for codigo, display, _ in PRODUCTOS_PRIORITARIOS:
        sub = df_hoy[df_hoy["cargo"] == codigo]
        tons = sub["quantity"].fillna(0).sum()
        print(f"  {display:15} cargando={sub[sub['estado']=='CARGANDO']['vessel'].nunique():3} "
              f"arribando={sub[sub['estado']=='ARRIBANDO']['vessel'].nunique():3} "
              f"tons={int(tons):>10,}")


def test_shippers(fecha_ref: date, ventana_dias: int = 30) -> None:
    print("\n=== SHIPPERS ===")
    desde = fecha_ref - timedelta(days=730)
    df = query_exports_prioritarios(fecha_desde=desde, fecha_hasta=fecha_ref)
    print(f"Historico 2 anos: shape={df.shape}")

    df["fecha_consulta"] = pd.to_datetime(df["fecha_consulta"])
    df["fecha_day"] = df["fecha_consulta"].dt.date

    # Simular el calculo z-score por shipper
    all_dates = pd.date_range(
        start=fecha_ref - timedelta(days=730), end=fecha_ref, freq="D",
    )

    for shipper in SHIPPERS_TOP[:3]:  # solo top 3 para velocidad de test
        sub = df[df["shipper_canon"] == shipper]
        buques_por_dia = sub.groupby("fecha_day")["vessel"].unique()
        serie_buques = []
        for fin in all_dates:
            ini = fin - timedelta(days=ventana_dias)
            dentro = buques_por_dia.loc[
                (buques_por_dia.index >= ini.date()) &
                (buques_por_dia.index <= fin.date())
            ]
            if len(dentro) == 0:
                serie_buques.append(0)
            else:
                unicos = set()
                for arr in dentro.values:
                    unicos.update(arr)
                serie_buques.append(len(unicos))
        serie = pd.Series(serie_buques, index=all_dates)
        mean_hist = serie.mean()
        std_hist = serie.std()
        actual = serie.iloc[-1]
        z = (actual - mean_hist) / std_hist if std_hist > 0 else 0
        print(f"  {shipper:15} actual={actual:3.0f} mean={mean_hist:5.1f} "
              f"std={std_hist:5.2f} z={z:+.2f}")


def test_productos(fecha_ref: date, codigo: str = "MAIZE") -> None:
    print(f"\n=== PRODUCTOS ({codigo}) ===")
    camp_actual = campanas.campana_de(codigo, fecha_ref)
    inicio, fin = campanas.fechas_de_campana(codigo, camp_actual)
    dia = campanas.dia_de_campana(codigo, fecha_ref)
    print(f"Campana {camp_actual}: {inicio} -> {fin} (dia {dia})")

    camps_ant = campanas.campanas_anteriores(codigo, fecha_ref, n=5, incluir_actual=True)
    rango_desde, rango_hasta = campanas.filtro_rango_campanas(codigo, camps_ant)
    print(f"Rango completo (6 campanas): {rango_desde} -> {rango_hasta}")

    df = query_lineup(fecha_desde=rango_desde, fecha_hasta=rango_hasta, cargos=[codigo])
    df = df[df["ops"] == "LOAD"]
    df = aplicar_a_dataframe(df)
    print(f"Data historica: {df.shape[0]} filas")

    df["fecha_consulta"] = pd.to_datetime(df["fecha_consulta"])
    df["fecha_day"] = df["fecha_consulta"].dt.date
    df["campana"] = df["fecha_day"].apply(lambda f: campanas.campana_de(codigo, f))
    df["dia_campana"] = df["fecha_day"].apply(lambda f: campanas.dia_de_campana(codigo, f))

    # Acumulados por campana al mismo dia-de-campana.
    print("Acumulados al mismo dia-de-campana:")
    for camp in camps_ant:
        sub = df[(df["campana"] == camp) & (df["dia_campana"] <= dia)]
        tons = sub["quantity"].fillna(0).sum()
        buques = sub["vessel"].nunique()
        marca = " <- ACTUAL" if camp == camp_actual else ""
        print(f"  {camp}: {buques:4} buques, {int(tons):>12,}t{marca}")

    # Construir matriz para p10-p90
    series_historicas = []
    for camp in camps_ant:
        if camp == camp_actual:
            continue
        sub = df[df["campana"] == camp]
        if sub.empty:
            continue
        diario = sub.groupby("dia_campana")["quantity"].sum().reset_index()
        diario = diario.sort_values("dia_campana")
        diario["acum"] = diario["quantity"].cumsum()
        total_dias = (fin - inicio).days + 1
        reix = pd.DataFrame({"dia_campana": range(1, total_dias + 1)})
        diario = reix.merge(diario, on="dia_campana", how="left")
        diario["acum"] = diario["acum"].ffill().fillna(0)
        series_historicas.append(diario)

    if series_historicas:
        mat = np.array([s["acum"].values for s in series_historicas])
        print(f"Matriz historica shape: {mat.shape}")
        p10 = np.percentile(mat, 10, axis=0)
        p50 = np.percentile(mat, 50, axis=0)
        p90 = np.percentile(mat, 90, axis=0)
        print(f"Al dia {dia}: p10={int(p10[dia-1]):,} mediana={int(p50[dia-1]):,} p90={int(p90[dia-1]):,}")


def test_estimaciones() -> None:
    print("\n=== ESTIMACIONES MAGyP ===")
    import estimaciones
    df = estimaciones.descargar_estimaciones_magyp()
    print(f"CSV descargado: {df.shape[0]:,} filas")
    if df.empty:
        print("ERROR: DataFrame vacio")
        return
    tot = estimaciones.totales_nacionales_por_campania(df)
    print(f"Totales nacionales: {tot.shape[0]} filas")
    for codigo in ["MAIZE", "SBS", "WHEAT", "BARLEY", "SORGHUM", "SFSEED"]:
        ult = estimaciones.ultima_campania_por_cultivo(tot, codigo, n=3)
        if ult.empty:
            print(f"  {codigo:10} sin data")
            continue
        ult = estimaciones.variacion_vs_campania_anterior(ult)
        fila = ult.iloc[0]
        pct = fila.get("pct_vs_anterior")
        pct_str = f"{pct:+.1f}%" if pd.notna(pct) else "-"
        print(
            f"  {codigo:10} {fila['campania']}: "
            f"{fila['produccion_tm']/1_000_000:6.2f} Mt  "
            f"rinde {int(fila['rinde_kgxha']):5,} kg/ha  {pct_str}"
        )


def test_congestion(fecha_ref: date) -> None:
    print("\n=== CONGESTION ===")
    df = query_en_puerto_ahora(fecha_ref)
    print(f"En puerto ahora: shape={df.shape}")
    if df.empty:
        print("Sin data de buques en puerto")
        return
    df = aplicar_a_dataframe(df)
    df["zona"] = df["port"].apply(zona_de_puerto)
    print("Por zona:")
    for zona, sub in df.groupby("zona"):
        print(f"  {zona:25} {sub['vessel'].nunique():3} buques, "
              f"{int(sub['quantity'].fillna(0).sum()):>10,} tons")


if __name__ == "__main__":
    fecha_ref = ultima_fecha_cargada() or date.today()
    print(f"Fecha de referencia: {fecha_ref}")
    print(f"Primera fecha en DB: {primera_fecha_cargada()}")

    for test in (test_panorama, test_shippers, test_productos, test_congestion):
        try:
            test(fecha_ref)
        except Exception:
            print(f"\n!!! ERROR en {test.__name__}:")
            traceback.print_exc()

    # Test estimaciones (no necesita fecha_ref, hace su propia red).
    try:
        test_estimaciones()
    except Exception:
        print("\n!!! ERROR en test_estimaciones:")
        traceback.print_exc()
