"""
fas_comprador.py — Vista COMPRADORES FAS para el trading desk.

Identifica qué exportadores están más urgentes de comprar grano en el mercado
FAS local, basándose en:

  1. Su POSICIÓN CORTA: cuántas toneladas declararon en DJVE menos cuántas
     tienen comprometidas en el line-up (gap = falta_cubrir).
  2. La CERCANÍA DE SUS ETB: el exportador con un barco llegando en 3 días
     sin grano tiene mucha más urgencia que uno con un barco en 25 días.

Lógica de negocio
-----------------
Para el trader vendedor FAS, el line-up es su termómetro de demanda:
  - Exportador con ETB próximo + gap alto → NECESITA comprar urgente → bid firme.
  - Exportador cubierto (originado ≥ declarado) → sin presión → puede bajar bid.

El URGENCIA SCORE combina ambas dimensiones:
  score = (falta_cubrir_tn / PANAMAX_TN) * (1 + max(0, 1 - dias_etb / horizonte))

Con esto, mismo gap pero ETB más cercano → score más alto → aparece arriba en
la tabla → es el primero a quien venderle.

Módulo PURO: recibe DataFrames, devuelve DataFrames/dicts. Sin red ni DB.
Importarlo no tiene efectos secundarios.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta

import pandas as pd

import config
import cobertura

logger = logging.getLogger(__name__)

# Un Panamax típico carga ~65.000 tn. Es la unidad de calibración del score.
PANAMAX_TN = 65_000.0

# Productos relevantes para un trader FAS de soja/maíz/trigo.
PRODUCTOS_FAS = {"SBS", "SBM", "SBO", "MAIZE", "WHEAT"}

HORIZONTES_DEFAULT = [7, 15, 30]

# Umbrales para el semáforo de urgencia.
UMBRAL_ROJO = 7    # ETB dentro de 7 días → ROJO
UMBRAL_AMBAR = 15  # ETB dentro de 15 días → AMBAR

# Umbrales para etiquetas de perfil histórico.
PCT_COMPRADOR_HABITUAL = 0.60
PCT_CORTO_FRECUENTE = 0.30


# ---------------------------------------------------------------------------
# 1. Score de urgencia
# ---------------------------------------------------------------------------

def _score_urgencia(falta_cubrir_tn: float, dias_proximo_etb: int,
                    horizonte: int) -> float:
    """
    Score compuesto de urgencia compradora.

    Combina el tamaño de la posición corta (en múltiplos de Panamax) con la
    proximidad del primer ETB sin cubrir.

    - falta_cubrir_tn <= 0 → score = 0 (cubierto o sobre-originado).
    - dias_proximo_etb = 0 → factor de proximidad = 2 (máximo, barco ya en puerto).
    - dias_proximo_etb >= horizonte → factor de proximidad = 1 (sin urgencia extra).
    """
    if falta_cubrir_tn <= 0:
        return 0.0
    proximidad = max(0.0, 1.0 - dias_proximo_etb / horizonte)
    return (falta_cubrir_tn / PANAMAX_TN) * (1.0 + proximidad)


# ---------------------------------------------------------------------------
# 2. Urgencia por shipper
# ---------------------------------------------------------------------------

def urgencia_por_shipper(
    df_djve: pd.DataFrame,
    df_lineup: pd.DataFrame,
    fecha_ref: date,
    horizontes: list[int] = HORIZONTES_DEFAULT,
) -> dict[int, pd.DataFrame]:
    """
    Calcula la urgencia compradora de cada exportador para cada horizonte.

    Para cada horizonte (típicamente 7, 15 y 30 días):
      - Calcula la posición corta por (shipper_canon, codigo_interno) usando
        `cobertura.balance_por_shipper`.
      - Filtra a los productos relevantes para el trader FAS (PRODUCTOS_FAS).
      - Excluye exportadores sin DJVE declarada (no son compradores urgentes;
        si tienen line-up sin DJVE, es un sobre-originado, no un comprador).
      - Agrega n_buques y dias_proximo_etb calculados sobre el line-up filtrado.
      - Calcula urgencia_score y ordena DESC (más urgente arriba).

    Args:
        df_djve:    DataFrame de fob_djve con columna `codigo_interno`.
        df_lineup:  DataFrame de db.query_exports_prioritarios con `shipper_canon`,
                    `cargo`, `quantity`, `etb`.
        fecha_ref:  Fecha de referencia (hoy).
        horizontes: Lista de horizontes en días.

    Returns:
        Dict {horizonte: DataFrame} con columnas:
            shipper_canon, codigo_interno, producto_display,
            declarado_tn, originado_tn, falta_cubrir_tn, ratio_cobertura,
            n_buques, dias_proximo_etb, urgencia_score.
        DataFrames vacíos (mismas columnas) si no hay datos.
    """
    _cols = [
        "shipper_canon", "codigo_interno", "producto_display",
        "declarado_tn", "originado_tn", "falta_cubrir_tn", "ratio_cobertura",
        "n_buques", "dias_proximo_etb", "urgencia_score",
    ]

    resultado: dict[int, pd.DataFrame] = {}

    for horizonte in horizontes:
        vacio = pd.DataFrame(columns=_cols)

        if df_djve.empty:
            resultado[horizonte] = vacio
            continue

        # Balance DECLARADO vs ORIGINADO por (shipper_canon, codigo_interno).
        balance = cobertura.balance_por_shipper(
            df_djve, df_lineup, fecha_ref, horizonte
        )

        if balance.empty:
            resultado[horizonte] = vacio
            continue

        # Filtrar a productos FAS.
        balance = balance[balance["codigo_interno"].isin(PRODUCTOS_FAS)].copy()

        # Excluir filas donde el shipper no tiene nada declarado.
        balance = balance[balance["declarado_tn"] > 0].copy()

        if balance.empty:
            resultado[horizonte] = vacio
            continue

        # --- n_buques y dias_proximo_etb ---
        # Necesitamos el line-up filtrado para contar buques y calcular el ETB
        # más próximo por (shipper_canon, cargo). Usamos el helper de cobertura.
        lineup_h = cobertura._filtrar_lineup_por_ventana(
            df_lineup, fecha_ref, horizonte
        )

        if not lineup_h.empty and "etb" in lineup_h.columns:
            tmp = lineup_h.copy()
            tmp["quantity"] = pd.to_numeric(tmp["quantity"], errors="coerce").fillna(0)

            # Días hasta cada ETB desde fecha_ref.
            tmp["_etb_date"] = pd.to_datetime(tmp["etb"], errors="coerce").dt.date
            tmp["_dias_etb"] = tmp["_etb_date"].apply(
                lambda d: (d - fecha_ref).days if pd.notna(d) else horizonte
            )
            tmp["_dias_etb"] = tmp["_dias_etb"].clip(lower=0)

            # Agrupar por (shipper_canon, cargo).
            agg = (
                tmp.groupby(["shipper_canon", "cargo"])
                .agg(
                    n_buques=("vessel", "nunique"),
                    dias_proximo_etb=("_dias_etb", "min"),
                )
                .reset_index()
                .rename(columns={"cargo": "codigo_interno"})
            )
        else:
            agg = pd.DataFrame(
                columns=["shipper_canon", "codigo_interno",
                         "n_buques", "dias_proximo_etb"]
            )

        # Merge balance con n_buques y dias_proximo_etb.
        df = balance.merge(
            agg, on=["shipper_canon", "codigo_interno"], how="left"
        )
        df["n_buques"] = df["n_buques"].fillna(0).astype(int)
        # Sin ETB en el lineup → usamos el final del horizonte (conservador).
        df["dias_proximo_etb"] = df["dias_proximo_etb"].fillna(horizonte).astype(int)

        # --- urgencia_score ---
        df["urgencia_score"] = df.apply(
            lambda r: _score_urgencia(
                r["falta_cubrir_tn"], r["dias_proximo_etb"], horizonte
            ),
            axis=1,
        )

        df = df[_cols].sort_values("urgencia_score", ascending=False).reset_index(drop=True)
        resultado[horizonte] = df

    return resultado


# ---------------------------------------------------------------------------
# 3. Perfil histórico del exportador
# ---------------------------------------------------------------------------

def perfil_historico(
    df_lineup_hist: pd.DataFrame,
    shipper_canon: str,
    codigo_interno: str,
    df_djve_hist: pd.DataFrame,
    fecha_ref: date,
    ventana_dias: int = 90,
) -> dict:
    """
    Analiza el comportamiento histórico de un exportador en el mercado FAS.

    Divide los últimos `ventana_dias` días en semanas (aprox. 12 semanas para
    90 días). En cada semana calcula si el exportador estuvo corto (falta_cubrir > 0).

    El resultado describe la FRECUENCIA y MAGNITUD con que este exportador
    necesita comprar en el FAS — no su precio histórico (no disponible en DB).

    Args:
        df_lineup_hist: Line-up histórico (al menos los últimos `ventana_dias` días).
        shipper_canon:  Nombre canonizado del exportador.
        codigo_interno: Código del producto (SBS, MAIZE, WHEAT...).
        df_djve_hist:   DJVE histórica.
        fecha_ref:      Fecha de referencia (hoy).
        ventana_dias:   Días de historia a analizar (default 90).

    Returns:
        Dict con:
          pct_periodos_corto (float): % de semanas con posición corta.
          promedio_gap_tn (float): gap promedio en semanas con posición corta.
          max_gap_tn (float): gap máximo histórico.
          label (str): "COMPRADOR HABITUAL" / "CORTO FRECUENTE" / "CUBRE BIEN"
                       / "SIN HISTORIA".
    """
    _sin_historia = {
        "pct_periodos_corto": 0.0,
        "promedio_gap_tn": 0.0,
        "max_gap_tn": 0.0,
        "label": "SIN HISTORIA",
    }

    if df_djve_hist.empty:
        return _sin_historia

    # Filtrar al shipper y producto de interés en el historial.
    djve_sh = df_djve_hist[
        df_djve_hist.get("codigo_interno", pd.Series(dtype=str)) == codigo_interno
    ].copy() if "codigo_interno" in df_djve_hist.columns else pd.DataFrame()

    lineup_sh = df_lineup_hist[
        (df_lineup_hist.get("shipper_canon", pd.Series(dtype=str)) == shipper_canon)
        & (df_lineup_hist.get("cargo", pd.Series(dtype=str)) == codigo_interno)
    ].copy() if "shipper_canon" in df_lineup_hist.columns else pd.DataFrame()

    if djve_sh.empty:
        return _sin_historia

    # TODO (fase 3, perf, NO aplicado a proposito): este loop de ~12 semanas
    # llama a cobertura.balance_por_shipper una vez por semana. Una version
    # totalmente vectorizada (sin loop) seria mas rapida, pero cada semana usa
    # una ventana de embarque de 7 dias SOLAPADA con la siguiente y la semantica
    # exacta de solapamiento de intervalos DJVE/ETB de balance_por_shipper es
    # dificil de replicar sin alterar los resultados. No se reescribe porque no
    # se puede garantizar salida identica a los tests existentes. El costo
    # dominante (re-canonicalizar la DJVE en cada llamada) YA se elimino via el
    # memo de cobertura.canonicalizar_djve (fase 2), asi que cada balance opera
    # sobre frames chicos ya canonicalizados.
    # Analizar semana a semana en los últimos ventana_dias días.
    semana_dias = 7
    n_semanas = ventana_dias // semana_dias
    if n_semanas == 0:
        return _sin_historia

    semanas_corto = 0
    gaps = []

    for i in range(n_semanas):
        # Semana de referencia: empezamos desde la más reciente y vamos atrás.
        fecha_semana = fecha_ref - timedelta(days=i * semana_dias)

        try:
            balance = cobertura.balance_por_shipper(
                djve_sh, df_lineup_hist if not lineup_sh.empty else pd.DataFrame(),
                fecha_semana, horizonte_dias=7,
            )
        except Exception:
            continue

        if balance.empty:
            continue

        fila = balance[
            (balance["shipper_canon"] == shipper_canon)
            & (balance["codigo_interno"] == codigo_interno)
        ]
        if fila.empty:
            continue

        gap = fila.iloc[0]["falta_cubrir_tn"]
        if gap > 0:
            semanas_corto += 1
            gaps.append(gap)

    if n_semanas == 0:
        return _sin_historia

    pct = semanas_corto / n_semanas

    if pct >= PCT_COMPRADOR_HABITUAL:
        label = "COMPRADOR HABITUAL"
    elif pct >= PCT_CORTO_FRECUENTE:
        label = "CORTO FRECUENTE"
    else:
        label = "CUBRE BIEN"

    return {
        "pct_periodos_corto": round(pct, 3),
        "promedio_gap_tn": round(sum(gaps) / len(gaps), 0) if gaps else 0.0,
        "max_gap_tn": round(max(gaps), 0) if gaps else 0.0,
        "label": label,
    }


# ---------------------------------------------------------------------------
# 4. Tabla wide de urgencia (los 3 horizontes combinados)
# ---------------------------------------------------------------------------

def tabla_urgencia(
    resultados_por_horizonte: dict[int, pd.DataFrame],
    perfiles: dict[tuple[str, str], dict],
) -> pd.DataFrame:
    """
    Combina los resultados de los 3 horizontes en una tabla wide ordenada.

    Útil para tener un solo DataFrame que muestre la evolución de la posición
    corta en el tiempo (7d → 15d → 30d) por exportador y producto.

    Args:
        resultados_por_horizonte: Salida de `urgencia_por_shipper`.
        perfiles: Dict {(shipper_canon, codigo_interno): dict_perfil}.
                  Puede estar vacío; en ese caso se usa "SIN HISTORIA".

    Returns:
        DataFrame con columnas:
            shipper_canon, codigo_interno, producto_display,
            falta_7d, falta_15d, falta_30d,
            dias_proximo_etb, urgencia_score_7d, perfil_label, señal.
        Ordenado por urgencia_score_7d DESC. Vacío si no hay datos.
    """
    df_7 = resultados_por_horizonte.get(7, pd.DataFrame())
    df_15 = resultados_por_horizonte.get(15, pd.DataFrame())
    df_30 = resultados_por_horizonte.get(30, pd.DataFrame())

    if df_7.empty:
        return pd.DataFrame(columns=[
            "shipper_canon", "codigo_interno", "producto_display",
            "falta_7d", "falta_15d", "falta_30d",
            "dias_proximo_etb", "urgencia_score_7d", "perfil_label", "señal",
        ])

    base = df_7[["shipper_canon", "codigo_interno", "producto_display",
                  "falta_cubrir_tn", "dias_proximo_etb", "urgencia_score"]].rename(
        columns={
            "falta_cubrir_tn": "falta_7d",
            "urgencia_score": "urgencia_score_7d",
        }
    )

    # Agregar falta de otros horizontes.
    for h, df_h, col in [(15, df_15, "falta_15d"), (30, df_30, "falta_30d")]:
        if not df_h.empty:
            sub = df_h[["shipper_canon", "codigo_interno", "falta_cubrir_tn"]].rename(
                columns={"falta_cubrir_tn": col}
            )
            base = base.merge(sub, on=["shipper_canon", "codigo_interno"], how="left")
        else:
            base[col] = float("nan")

    # Perfil histórico.
    base["perfil_label"] = base.apply(
        lambda r: perfiles.get(
            (r["shipper_canon"], r["codigo_interno"]), {}
        ).get("label", "SIN HISTORIA"),
        axis=1,
    )

    # Semáforo de urgencia.
    def _señal(dias: int) -> str:
        if dias <= UMBRAL_ROJO:
            return "ROJO"
        if dias <= UMBRAL_AMBAR:
            return "AMBAR"
        return "VERDE"

    base["señal"] = base["dias_proximo_etb"].apply(_señal)

    cols_final = [
        "shipper_canon", "codigo_interno", "producto_display",
        "falta_7d", "falta_15d", "falta_30d",
        "dias_proximo_etb", "urgencia_score_7d", "perfil_label", "señal",
    ]
    return (
        base[cols_final]
        .sort_values("urgencia_score_7d", ascending=False)
        .reset_index(drop=True)
    )


# ---------------------------------------------------------------------------
# Self-test (solo como script directo)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    from datetime import date as _date

    print("=== FAS COMPRADOR — SELF-TEST (DataFrames sintéticos) ===")

    ref = _date(2026, 6, 1)

    # DJVE sintética: Cargill declaró 500k tn de maíz con ventana 5-35 días
    djve = pd.DataFrame([{
        "nro_djve": 1,
        "razon_social": "CARGILL SACI",
        "shipper_canon": "CARGILL",
        "codigo_interno": "MAIZE",
        "producto": "MAIZE",
        "toneladas": 500_000,
        "fecha_registro": ref,
        "fecha_inicio_embarque": ref + timedelta(days=5),
        "fecha_fin_embarque": ref + timedelta(days=35),
    }])

    # Line-up sintético: Cargill tiene 1 buque (65k tn) con ETB en 8 días
    lineup = pd.DataFrame([{
        "vessel": "MV TEST",
        "cargo": "MAIZE",
        "quantity": 65_000,
        "etb": ref + timedelta(days=8),
        "shipper_canon": "CARGILL",
        "origen_alt": None,
        "ops": "LOAD",
    }])

    resultados = urgencia_por_shipper(djve, lineup, ref)

    for h, df in resultados.items():
        print(f"\nHorizonte {h}d:")
        if df.empty:
            print("  (vacío)")
        else:
            row = df.iloc[0]
            print(f"  {row['shipper_canon']} | {row['producto_display']} | "
                  f"falta: {row['falta_cubrir_tn']:,.0f} tn | "
                  f"ETB en: {row['dias_proximo_etb']}d | "
                  f"score: {row['urgencia_score']:.2f}")

    tabla = tabla_urgencia(resultados, {})
    print(f"\nTabla wide: {tabla.shape[0]} filas, señal: {tabla.iloc[0]['señal'] if not tabla.empty else '—'}")
    print("\nSelf-test OK.")
    sys.exit(0)
