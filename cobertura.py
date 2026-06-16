"""
Cobertura exportadora: cruce DECLARADO (DJVE) vs ORIGINADO (line-up) -> SEÑAL.

Idea de negocio (trading desk Gran Rosario)
--------------------------------------------
Un exportador hace dos cosas en momentos distintos:

1. DECLARA al exterior (DJVE / Ley 21.453): se compromete a embarcar X toneladas
   de un producto en una ventana de embarque. Es una venta FOB ya cerrada.
2. ORIGINA el grano: consigue la mercaderia fisica y programa el buque que la
   carga. Eso aparece en el line-up portuario (ISA) como un buque con ETB en
   una fecha, cargo (producto) y quantity (toneladas).

La diferencia entre lo declarado y lo originado es la POSICION CORTA del
exportador:

    falta_cubrir = declarado - originado

- Si un shipper declaro mucho y NO tiene buques en line-up (falta_cubrir > 0,
  ratio < 1) esta CORTO: tiene que salir a comprar grano al mercado interno
  para cumplir el embarque -> presion ALCISTA sobre el precio FAS local.
- Si tiene mas buques en line-up que lo declarado (ratio > 1) esta
  SOBRE-ORIGINADO: ya compro de mas, demanda de compra interna agotada ->
  sesgo BAJISTA.
- Si el line-up esta muy cargado relativo al declarado en una ventana corta,
  hay congestion logistica (riesgo de demoras, sobreestadias, costos).

Clave de cruce
--------------
DJVE identifica al exportador por `razon_social` (nombre legal libre, ej
"CARGILL S.A.C.I."). El line-up usa `shipper_canon` (ya canonicalizado por
`shipper_norm`). Para cruzarlos hay que canonicalizar tambien la razon social
de la DJVE. El cruce final es por `(shipper_canon, codigo_interno)`.

Este modulo es PURO: recibe DataFrames y devuelve DataFrames/dicts. No hace
red ni DB. Importarlo no tiene efectos secundarios.
"""
from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

import campanas
import config
from shipper_norm import canonicalizar_shipper

# Umbral por debajo del cual no consideramos "declarado significativo" para
# emitir una señal alcista. Evita gritar ALCISTA por una DJVE testimonial.
# 5.000 tn ~ menos de un decimo de un Panamax tipico.
DECLARADO_MINIMO_SIGNIFICATIVO_TN = 5_000.0

# Reglas de señal (documentadas tambien en senales_trading).
RATIO_CORTO = 0.7        # ratio < 0.7 -> exportadores cortos -> ALCISTA FAS
RATIO_SOBRE_ORIGEN = 1.3  # ratio > 1.3 -> compraron de mas -> BAJISTA

# Cuanta tonelada concentrada en una sola semana dispara alerta de congestion.
# Calibrado a Gran Rosario: ~6 Panamax (60k c/u) cargando la misma semana.
CONGESTION_TN_SEMANA = 360_000.0


# ---------------------------------------------------------------------------
# 1. Canonicalizacion de la razon social DJVE
# ---------------------------------------------------------------------------

def canonicalizar_djve(df_djve: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega la columna `shipper_canon` (y `origen_alt`) a la DJVE aplicando
    `shipper_norm.canonicalizar_shipper` sobre `razon_social`.

    Sin esto no se puede cruzar la DJVE (nombres legales) con el line-up
    (shippers ya canonicalizados). Reutiliza la misma logica de regex que usa
    el line-up, asi "CARGILL S.A.C.I." de la DJVE matchea "CARGILL" del
    line-up.

    Args:
        df_djve: DataFrame de `fob_djve.descargar_djve_acumuladas`. Necesita la
            columna `razon_social`.

    Returns:
        Copia del DataFrame con columnas extra `shipper_canon` y `origen_alt`.
        Si el DataFrame esta vacio o no tiene `razon_social`, devuelve una copia
        con esas columnas en None.
    """
    df = df_djve.copy()
    if df.empty or "razon_social" not in df.columns:
        df["shipper_canon"] = None
        df["origen_alt"] = None
        return df

    # Optimizacion: canonicalizamos solo los valores unicos de razon_social y
    # mapeamos de vuelta (la regex es cara y hay muchas filas con la misma
    # razon social). Salida identica al .map() por fila.
    serie = df["razon_social"]
    uniques = serie.dropna().unique()
    lut = {u: canonicalizar_shipper(u) for u in uniques}
    fallback = ("OTROS", None)
    pares = [lut.get(v, fallback) for v in serie]
    df["shipper_canon"] = [p[0] for p in pares]
    df["origen_alt"] = [p[1] for p in pares]
    return df


# ---------------------------------------------------------------------------
# Helpers internos de filtrado por ventana temporal
# ---------------------------------------------------------------------------

def _filtrar_djve_por_ventana(
    df_djve: pd.DataFrame,
    fecha_ref: date,
    horizonte_dias: int,
) -> pd.DataFrame:
    """
    Filtra DJVE cuya ventana de embarque se solapa con [fecha_ref, fecha_ref+H].

    Una DJVE "cuenta" para el horizonte si su ventana de embarque
    [fecha_inicio_embarque, fecha_fin_embarque] se cruza con el horizonte de
    analisis. Solo se consideran productos mapeados al line-up (codigo_interno
    no nulo). Filas sin ventana de embarque se descartan (no podemos ubicarlas
    en el tiempo).
    """
    if df_djve.empty:
        return df_djve

    fecha_fin = fecha_ref + timedelta(days=horizonte_dias)
    df = df_djve.copy()

    # Solo productos cruzables con el line-up.
    if "codigo_interno" in df.columns:
        df = df[df["codigo_interno"].notna()]
    if df.empty:
        return df

    ini = pd.to_datetime(df["fecha_inicio_embarque"], errors="coerce").dt.date
    fin = pd.to_datetime(df["fecha_fin_embarque"], errors="coerce").dt.date
    # Si falta el fin, usamos el inicio como ventana puntual y viceversa.
    ini = ini.fillna(fin)
    fin = fin.fillna(ini)

    # Solapamiento de intervalos: ini <= fecha_fin AND fin >= fecha_ref.
    mask = ini.notna() & fin.notna() & (ini <= fecha_fin) & (fin >= fecha_ref)
    return df[mask].copy()


def _filtrar_lineup_por_ventana(
    df_lineup: pd.DataFrame,
    fecha_ref: date,
    horizonte_dias: int,
) -> pd.DataFrame:
    """
    Filtra line-up con ETB dentro de [fecha_ref, fecha_ref+H].

    El ETB (Estimated Time of Berthing) es la fecha en que el buque atraca a
    cargar: lo tomamos como el momento de "originacion efectiva" del grano.
    Filas sin ETB se descartan.
    """
    if df_lineup.empty:
        return df_lineup

    fecha_fin = fecha_ref + timedelta(days=horizonte_dias)
    df = df_lineup.copy()

    etb = pd.to_datetime(df["etb"], errors="coerce").dt.date
    mask = etb.notna() & (etb >= fecha_ref) & (etb <= fecha_fin)
    return df[mask].copy()


def _ratio(originado: float, declarado: float) -> float:
    """ratio_cobertura = originado / declarado, robusto a declarado == 0."""
    if declarado <= 0:
        # Sin nada declarado: si hay originado lo marcamos como sobre-originado
        # extremo (inf); si no hay nada, ratio neutro (NaN se trata aparte).
        return float("inf") if originado > 0 else float("nan")
    return originado / declarado


def _ratio_vec(originado: pd.Series, declarado: pd.Series) -> pd.Series:
    """
    Version vectorizada de `_ratio` para aplicar sobre columnas enteras.

    Reproduce exactamente la semantica escalar:
      - declarado <= 0 y originado  > 0 -> inf (sobre-originado extremo)
      - declarado <= 0 y originado <= 0 -> NaN (ni declarado ni originado)
      - declarado  > 0                  -> originado / declarado
    """
    import numpy as np

    orig = pd.to_numeric(originado, errors="coerce")
    decl = pd.to_numeric(declarado, errors="coerce")
    # Division segura (evita warnings de /0); luego sobreescribimos los bordes.
    with np.errstate(divide="ignore", invalid="ignore"):
        ratio = orig / decl
    sin_declarado = decl <= 0
    ratio = ratio.where(
        ~sin_declarado,
        other=np.where(orig > 0, float("inf"), float("nan")),
    )
    return ratio


# ---------------------------------------------------------------------------
# 2. Balance por producto
# ---------------------------------------------------------------------------

def balance_por_producto(
    df_djve: pd.DataFrame,
    df_lineup: pd.DataFrame,
    fecha_ref: date,
    horizonte_dias: int = 60,
) -> pd.DataFrame:
    """
    Balance DECLARADO vs ORIGINADO por producto (codigo_interno).

    Para cada producto en el horizonte [fecha_ref, fecha_ref+horizonte_dias]:
      - declarado_tn   = suma DJVE cuya ventana de embarque toca el horizonte.
      - originado_tn   = suma quantity del line-up con ETB en el horizonte.
      - falta_cubrir_tn = declarado_tn - originado_tn (posicion corta).
      - ratio_cobertura = originado_tn / declarado_tn.
            1.0  -> perfectamente cubierto
            <1   -> corto (debe comprar grano interno -> alcista FAS)
            >1   -> sobre-originado (ya compro de mas -> bajista)
            inf  -> originado sin nada declarado
            NaN  -> ni declarado ni originado

    El DataFrame de DJVE NO necesita estar canonicalizado para esta funcion
    (agrega solo por producto), pero conviene pasarle el codigo_interno ya
    presente (lo trae `fob_djve`).

    Returns:
        DataFrame ordenado por declarado_tn desc con columnas:
        codigo_interno, producto_display, declarado_tn, originado_tn,
        falta_cubrir_tn, ratio_cobertura, n_djve, n_buques.
        DataFrame vacio si no hay nada que reportar.
    """
    djve_h = _filtrar_djve_por_ventana(df_djve, fecha_ref, horizonte_dias)
    lineup_h = _filtrar_lineup_por_ventana(df_lineup, fecha_ref, horizonte_dias)

    # Agregado DJVE por producto.
    if djve_h.empty:
        decl = pd.DataFrame(columns=["codigo_interno", "declarado_tn", "n_djve"])
    else:
        decl = (
            djve_h.groupby("codigo_interno")
            .agg(
                declarado_tn=("toneladas", "sum"),
                n_djve=("toneladas", "count"),
            )
            .reset_index()
        )

    # Agregado line-up por producto (la columna de producto es `cargo`).
    if lineup_h.empty:
        orig = pd.DataFrame(columns=["codigo_interno", "originado_tn", "n_buques"])
    else:
        tmp = lineup_h.copy()
        tmp["quantity"] = pd.to_numeric(tmp["quantity"], errors="coerce").fillna(0)
        orig = (
            tmp.groupby("cargo")
            .agg(
                originado_tn=("quantity", "sum"),
                n_buques=("quantity", "count"),
            )
            .reset_index()
            .rename(columns={"cargo": "codigo_interno"})
        )

    if decl.empty and orig.empty:
        return pd.DataFrame(
            columns=[
                "codigo_interno", "producto_display", "declarado_tn",
                "originado_tn", "falta_cubrir_tn", "ratio_cobertura",
                "n_djve", "n_buques",
            ]
        )

    df = decl.merge(orig, on="codigo_interno", how="outer")
    for col in ("declarado_tn", "originado_tn", "n_djve", "n_buques"):
        df[col] = df[col].fillna(0)

    df["falta_cubrir_tn"] = df["declarado_tn"] - df["originado_tn"]
    df["ratio_cobertura"] = _ratio_vec(df["originado_tn"], df["declarado_tn"])
    df["producto_display"] = df["codigo_interno"].map(
        config.PRODUCTO_DISPLAY
    ).fillna(df["codigo_interno"])

    df = df[
        [
            "codigo_interno", "producto_display", "declarado_tn",
            "originado_tn", "falta_cubrir_tn", "ratio_cobertura",
            "n_djve", "n_buques",
        ]
    ]
    return df.sort_values("declarado_tn", ascending=False).reset_index(drop=True)


# ---------------------------------------------------------------------------
# 3. Balance por shipper
# ---------------------------------------------------------------------------

def balance_por_shipper(
    df_djve: pd.DataFrame,
    df_lineup: pd.DataFrame,
    fecha_ref: date,
    horizonte_dias: int = 60,
) -> pd.DataFrame:
    """
    Balance DECLARADO vs ORIGINADO por (shipper_canon, codigo_interno).

    Identifica QUIEN esta corto y en QUE producto. La DJVE se canonicaliza
    internamente (no hace falta pasarla pre-canonicalizada). El cruce con el
    line-up es por (shipper_canon, codigo_interno), de modo que "CARGILL
    S.A.C.I." de la DJVE matchea "CARGILL" del line-up.

    Returns:
        DataFrame ordenado por falta_cubrir_tn desc (mas cortos arriba) con:
        shipper_canon, codigo_interno, producto_display, declarado_tn,
        originado_tn, falta_cubrir_tn, ratio_cobertura.
        DataFrame vacio si no hay nada que reportar.
    """
    # Canonicalizar DJVE para que la clave de cruce coincida con el line-up.
    djve_canon = canonicalizar_djve(df_djve)
    djve_h = _filtrar_djve_por_ventana(djve_canon, fecha_ref, horizonte_dias)
    lineup_h = _filtrar_lineup_por_ventana(df_lineup, fecha_ref, horizonte_dias)

    if djve_h.empty:
        decl = pd.DataFrame(
            columns=["shipper_canon", "codigo_interno", "declarado_tn"]
        )
    else:
        decl = (
            djve_h.groupby(["shipper_canon", "codigo_interno"])
            .agg(declarado_tn=("toneladas", "sum"))
            .reset_index()
        )

    if lineup_h.empty:
        orig = pd.DataFrame(
            columns=["shipper_canon", "codigo_interno", "originado_tn"]
        )
    else:
        tmp = lineup_h.copy()
        tmp["quantity"] = pd.to_numeric(tmp["quantity"], errors="coerce").fillna(0)
        orig = (
            tmp.groupby(["shipper_canon", "cargo"])
            .agg(originado_tn=("quantity", "sum"))
            .reset_index()
            .rename(columns={"cargo": "codigo_interno"})
        )

    if decl.empty and orig.empty:
        return pd.DataFrame(
            columns=[
                "shipper_canon", "codigo_interno", "producto_display",
                "declarado_tn", "originado_tn", "falta_cubrir_tn",
                "ratio_cobertura",
            ]
        )

    df = decl.merge(orig, on=["shipper_canon", "codigo_interno"], how="outer")
    for col in ("declarado_tn", "originado_tn"):
        df[col] = df[col].fillna(0)

    df["falta_cubrir_tn"] = df["declarado_tn"] - df["originado_tn"]
    df["ratio_cobertura"] = _ratio_vec(df["originado_tn"], df["declarado_tn"])
    df["producto_display"] = df["codigo_interno"].map(
        config.PRODUCTO_DISPLAY
    ).fillna(df["codigo_interno"])

    df = df[
        [
            "shipper_canon", "codigo_interno", "producto_display",
            "declarado_tn", "originado_tn", "falta_cubrir_tn",
            "ratio_cobertura",
        ]
    ]
    return df.sort_values("falta_cubrir_tn", ascending=False).reset_index(drop=True)


# ---------------------------------------------------------------------------
# 4. Carga del line-up (congestion)
# ---------------------------------------------------------------------------

def carga_lineup(df_lineup: pd.DataFrame, fecha_ref: date) -> dict:
    """
    Metricas de congestion del line-up a futuro (ETB >= fecha_ref).

    Mira la cola de buques que todavia no cargaron (ETB en el futuro respecto
    de fecha_ref) y mide cuanta presion logistica hay y donde se concentra.

    Returns:
        dict con:
          - n_buques: cantidad de buques en cola.
          - toneladas_total: tn totales esperando para cargar.
          - toneladas_por_puerto: dict {puerto: tn} ordenado desc.
          - semana_pico: ISO date (lunes) de la semana de ETB con mas tn, o None.
          - tn_semana_pico: tn concentradas en esa semana.
          - toneladas_por_semana: dict {lunes_iso: tn} ordenado cronologico.
          - congestion: bool, True si tn_semana_pico supera el umbral.
    """
    vacio = {
        "n_buques": 0,
        "toneladas_total": 0.0,
        "toneladas_por_puerto": {},
        "semana_pico": None,
        "tn_semana_pico": 0.0,
        "toneladas_por_semana": {},
        "congestion": False,
    }
    if df_lineup.empty or "etb" not in df_lineup.columns:
        return vacio

    df = df_lineup.copy()
    etb = pd.to_datetime(df["etb"], errors="coerce")
    df["_etb_dt"] = etb
    df = df[df["_etb_dt"].notna() & (df["_etb_dt"].dt.date >= fecha_ref)]
    if df.empty:
        return vacio

    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0)

    n_buques = int(len(df))
    tn_total = float(df["quantity"].sum())

    # Toneladas por puerto.
    por_puerto = (
        df.groupby("port")["quantity"].sum().sort_values(ascending=False)
    )
    tn_por_puerto = {str(k): float(v) for k, v in por_puerto.items()}

    # Toneladas por semana (lunes de la semana ISO del ETB).
    semana = df["_etb_dt"].dt.to_period("W-SUN").dt.start_time.dt.date
    por_semana = df.groupby(semana)["quantity"].sum().sort_index()
    tn_por_semana = {k.isoformat(): float(v) for k, v in por_semana.items()}

    if not por_semana.empty:
        semana_pico = por_semana.idxmax()
        tn_pico = float(por_semana.max())
    else:
        semana_pico = None
        tn_pico = 0.0

    return {
        "n_buques": n_buques,
        "toneladas_total": tn_total,
        "toneladas_por_puerto": tn_por_puerto,
        "semana_pico": semana_pico.isoformat() if semana_pico else None,
        "tn_semana_pico": tn_pico,
        "toneladas_por_semana": tn_por_semana,
        "congestion": tn_pico >= CONGESTION_TN_SEMANA,
    }


# ---------------------------------------------------------------------------
# 5. Señales de trading
# ---------------------------------------------------------------------------

def _intensidad_por_faltante(falta_tn: float) -> int:
    """
    Escala el faltante absoluto a una intensidad 1-5.

    Buckets pensados en multiplos de Panamax (~60k tn):
      <60k -> 1, <180k -> 2, <360k -> 3, <720k -> 4, >=720k -> 5.
    """
    f = abs(float(falta_tn))
    if f < 60_000:
        return 1
    if f < 180_000:
        return 2
    if f < 360_000:
        return 3
    if f < 720_000:
        return 4
    return 5


def senales_trading(
    balance_producto: pd.DataFrame,
    carga: dict,
) -> pd.DataFrame:
    """
    Traduce el balance por producto + carga del line-up en señales accionables.

    Reglas:
      - ratio_cobertura < RATIO_CORTO (0.7) y declarado significativo
        (>= DECLARADO_MINIMO_SIGNIFICATIVO_TN) -> ALCISTA FAS. Los exportadores
        estan cortos y deben comprar grano interno. Intensidad por magnitud del
        faltante.
      - ratio_cobertura > RATIO_SOBRE_ORIGEN (1.3) -> BAJISTA. Sobre-originado:
        ya compraron de mas, demanda de compra agotada.
      - Line-up muy cargado en una sola semana (carga["congestion"]) ->
        una señal CONGESTION extra (riesgo logistico / demoras / sobreestadias).

    Args:
        balance_producto: salida de `balance_por_producto`.
        carga: salida de `carga_lineup`.

    Returns:
        DataFrame con: codigo_interno, producto_display, señal, intensidad (1-5),
        racional. Una fila por producto con señal + (opcional) una fila
        CONGESTION global. Vacio si no hay señales.
    """
    filas: list[dict] = []

    if balance_producto is not None and not balance_producto.empty:
        for _, r in balance_producto.iterrows():
            ratio = r["ratio_cobertura"]
            declarado = float(r["declarado_tn"])
            originado = float(r["originado_tn"])
            falta = float(r["falta_cubrir_tn"])
            disp = r["producto_display"]

            # ALCISTA: corto y con declarado significativo.
            if (
                pd.notna(ratio)
                and ratio < RATIO_CORTO
                and declarado >= DECLARADO_MINIMO_SIGNIFICATIVO_TN
            ):
                cob_pct = f"{ratio * 100:.0f}%"
                filas.append({
                    "codigo_interno": r["codigo_interno"],
                    "producto_display": disp,
                    "señal": "ALCISTA FAS",
                    "intensidad": _intensidad_por_faltante(falta),
                    "racional": (
                        f"Exportadores cortos en {disp}: declararon "
                        f"{declarado:,.0f} tn y solo tienen {originado:,.0f} tn "
                        f"en line-up (cobertura {cob_pct}). Faltan "
                        f"{falta:,.0f} tn -> deben comprar grano interno -> "
                        f"presion alcista sobre el FAS."
                    ),
                })
            # BAJISTA: sobre-originado.
            elif pd.notna(ratio) and ratio > RATIO_SOBRE_ORIGEN:
                cob_txt = "inf" if ratio == float("inf") else f"{ratio * 100:.0f}%"
                filas.append({
                    "codigo_interno": r["codigo_interno"],
                    "producto_display": disp,
                    "señal": "BAJISTA",
                    "intensidad": _intensidad_por_faltante(falta),
                    "racional": (
                        f"Sobre-originado en {disp}: line-up {originado:,.0f} tn "
                        f"vs declarado {declarado:,.0f} tn (cobertura {cob_txt}). "
                        f"Ya compraron de mas -> demanda de compra interna "
                        f"agotada -> sesgo bajista."
                    ),
                })

    # Señal de congestion global (no asociada a un producto puntual).
    if carga and carga.get("congestion"):
        tn_pico = carga.get("tn_semana_pico", 0.0)
        semana = carga.get("semana_pico")
        intensidad = min(5, max(1, int(tn_pico // CONGESTION_TN_SEMANA) + 2))
        filas.append({
            "codigo_interno": "*",
            "producto_display": "Todos",
            "señal": "CONGESTION",
            "intensidad": intensidad,
            "racional": (
                f"Line-up sobrecargado: {tn_pico:,.0f} tn concentradas en la "
                f"semana del {semana}. Riesgo de demoras de atraque, "
                f"sobreestadias y sobrecostos logisticos (sesgo bajista por "
                f"sobreoferta fisica en puerto)."
            ),
        })

    if not filas:
        return pd.DataFrame(
            columns=[
                "codigo_interno", "producto_display", "señal",
                "intensidad", "racional",
            ]
        )

    df = pd.DataFrame(filas)
    return df.sort_values("intensidad", ascending=False).reset_index(drop=True)


# ---------------------------------------------------------------------------
# 6. Helpers de contexto (DJVE solo, sin line-up)
# ---------------------------------------------------------------------------

def concentracion_por_shipper(
    df_djve: pd.DataFrame,
    fecha_ref: date | None = None,
    horizonte_dias: int = 60,
) -> pd.DataFrame:
    """
    Ranking de exportadores por toneladas declaradas (DJVE).

    Util cuando el line-up no esta disponible: muestra quien declaro mas y por
    ende quien tiene mas exposicion a cubrir. Si fecha_ref es None, agrega toda
    la DJVE recibida; si se pasa, filtra por ventana de embarque en el horizonte.

    Returns:
        DataFrame: shipper_canon, declarado_tn, n_djve, share (% del total),
        ordenado por declarado_tn desc.
    """
    djve_canon = canonicalizar_djve(df_djve)
    if fecha_ref is not None:
        djve_canon = _filtrar_djve_por_ventana(
            djve_canon, fecha_ref, horizonte_dias
        )
    if djve_canon.empty:
        return pd.DataFrame(
            columns=["shipper_canon", "declarado_tn", "n_djve", "share"]
        )

    agg = (
        djve_canon.groupby("shipper_canon")
        .agg(
            declarado_tn=("toneladas", "sum"),
            n_djve=("toneladas", "count"),
        )
        .reset_index()
        .sort_values("declarado_tn", ascending=False)
    )
    total = agg["declarado_tn"].sum()
    agg["share"] = agg["declarado_tn"] / total if total > 0 else 0.0
    return agg.reset_index(drop=True)


def contexto_campana(codigo_interno: str, fecha_ref: date) -> str:
    """
    Devuelve la campana vigente del producto en fecha_ref (ej "2025/26").

    Wrapper fino sobre `campanas.campana_de` para etiquetar los reportes con
    la campana correcta por producto (la soja arranca 1-abr, el maiz 1-mar,
    etc.). Util para el contexto de los reportes de cobertura.
    """
    return campanas.campana_de(codigo_interno, fecha_ref)
