"""
mesa_diff.py — "Qué cambió desde ayer" para la pestaña MESA.

La logística naval no cambia abruptamente de un día al otro: lo valioso cada
mañana es el DELTA vs el día hábil anterior. Este módulo compara dos snapshots
(hoy vs ayer) y emite solo los cambios MATERIALES, ordenados por importancia:

  1. Cambios de DIRECCIÓN del gap (el evento más importante).
  2. Cambios de BANDA del índice de calor.
  3. Movimientos de gap ≥ umbral.
  4. Buques nuevos ≥ umbral.
  5. DJVE nuevas ≥ umbral.

Módulo PURO: recibe DataFrames y dicts de estado, devuelve una lista de eventos
(dicts). Sin red ni DB. El formateo a texto/HTML lo hace el dashboard.
"""
from __future__ import annotations

from datetime import date

import pandas as pd

# Umbrales de materialidad (editables).
UMBRAL_BUQUE_NUEVO_TN = 30_000.0
UMBRAL_DJVE_NUEVA_TN = 20_000.0
UMBRAL_GAP_MOVIMIENTO_TN = 32_500.0

# Orden de prioridad de los tipos de evento (menor = más arriba).
_PRIORIDAD = {"DIR": 0, "BANDA": 1, "GAP": 2, "BUQUE": 3, "DJVE": 4}


# ---------------------------------------------------------------------------
# 1. Cambios de estado (dirección / banda / gap) por producto
# ---------------------------------------------------------------------------

def cambios_estado(
    estados_hoy: dict[str, dict],
    estados_ayer: dict[str, dict],
    umbral_gap: float = UMBRAL_GAP_MOVIMIENTO_TN,
) -> list[dict]:
    """
    Detecta cambios de dirección, banda y gap entre dos estados por producto.

    Args:
        estados_hoy / estados_ayer: dict {producto: {"banda", "direccion",
            "calor", "gap_tn"}}. Productos sin estado en ambos se ignoran.

    Returns:
        Lista de eventos dict con: tipo ("DIR"/"BANDA"/"GAP"), producto,
        desde, hasta, detalle, prioridad.
    """
    eventos: list[dict] = []
    for prod, eh in estados_hoy.items():
        ea = estados_ayer.get(prod)
        if not ea:
            continue

        # Cambio de dirección.
        dir_h, dir_a = eh.get("direccion"), ea.get("direccion")
        if dir_h and dir_a and dir_h != dir_a and "SIN" not in (dir_h, dir_a):
            eventos.append({
                "tipo": "DIR",
                "producto": prod,
                "desde": dir_a,
                "hasta": dir_h,
                "detalle": eh.get("gap_tn"),
                "prioridad": _PRIORIDAD["DIR"],
            })

        # Cambio de banda.
        b_h, b_a = eh.get("banda"), ea.get("banda")
        if b_h and b_a and b_h != b_a and "SIN HISTORIA" not in (b_h, b_a):
            eventos.append({
                "tipo": "BANDA",
                "producto": prod,
                "desde": b_a,
                "hasta": b_h,
                "calor_desde": ea.get("calor"),
                "calor_hasta": eh.get("calor"),
                "prioridad": _PRIORIDAD["BANDA"],
            })

        # Movimiento de gap ≥ umbral.
        g_h, g_a = eh.get("gap_tn"), ea.get("gap_tn")
        if g_h is not None and g_a is not None and abs(g_h - g_a) >= umbral_gap:
            eventos.append({
                "tipo": "GAP",
                "producto": prod,
                "desde": g_a,
                "hasta": g_h,
                "delta": g_h - g_a,
                "prioridad": _PRIORIDAD["GAP"],
            })

    return eventos


# ---------------------------------------------------------------------------
# 2. Buques nuevos en el line-up
# ---------------------------------------------------------------------------

def buques_nuevos(
    df_lineup_hoy: pd.DataFrame,
    df_lineup_ayer: pd.DataFrame,
    umbral_tn: float = UMBRAL_BUQUE_NUEVO_TN,
) -> list[dict]:
    """
    Buques presentes hoy y ausentes ayer, con tonelaje ≥ umbral.

    Identidad del buque: (vessel, cargo). Para cada buque nuevo relevante emite
    un evento con shipper, producto, tonelaje, puerto y ETB.
    """
    if df_lineup_hoy is None or df_lineup_hoy.empty:
        return []

    def _claves(df: pd.DataFrame) -> set:
        if df is None or df.empty or "vessel" not in df.columns:
            return set()
        return set(zip(df["vessel"].astype(str), df["cargo"].astype(str)))

    claves_ayer = _claves(df_lineup_ayer)

    df = df_lineup_hoy.copy()
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0)

    eventos: list[dict] = []
    for _, r in df.iterrows():
        clave = (str(r.get("vessel")), str(r.get("cargo")))
        if clave in claves_ayer:
            continue
        if float(r["quantity"]) < umbral_tn:
            continue
        eventos.append({
            "tipo": "BUQUE",
            "producto": r.get("cargo"),
            "vessel": r.get("vessel"),
            "shipper": r.get("shipper_canon") or r.get("shipper"),
            "toneladas": float(r["quantity"]),
            "puerto": r.get("port"),
            "etb": r.get("etb"),
            "prioridad": _PRIORIDAD["BUQUE"],
        })
    return eventos


# ---------------------------------------------------------------------------
# 3. DJVE nuevas
# ---------------------------------------------------------------------------

def djve_nuevas(
    df_djve: pd.DataFrame,
    fecha_corte: date,
    umbral_tn: float = UMBRAL_DJVE_NUEVA_TN,
) -> list[dict]:
    """
    DJVE registradas en/después de `fecha_corte`, agregadas por exportador-producto.

    Usa `fecha_registro` para detectar lo nuevo. Agrega por (shipper/razon,
    codigo_interno) y emite los grupos con tonelaje ≥ umbral.
    """
    if df_djve is None or df_djve.empty:
        return []
    if "fecha_registro" not in df_djve.columns:
        return []

    df = df_djve.copy()
    freg = pd.to_datetime(df["fecha_registro"], errors="coerce").dt.date
    df = df[freg.notna() & (freg >= fecha_corte)]
    if df.empty:
        return []

    col_nombre = "shipper_canon" if "shipper_canon" in df.columns else "razon_social"
    grupos = (
        df.groupby([col_nombre, "codigo_interno"])["toneladas"]
        .sum()
        .reset_index()
    )

    eventos: list[dict] = []
    for _, r in grupos.iterrows():
        if float(r["toneladas"]) < umbral_tn:
            continue
        eventos.append({
            "tipo": "DJVE",
            "producto": r["codigo_interno"],
            "shipper": r[col_nombre],
            "toneladas": float(r["toneladas"]),
            "prioridad": _PRIORIDAD["DJVE"],
        })
    return eventos


# ---------------------------------------------------------------------------
# 4. Combinador
# ---------------------------------------------------------------------------

def construir_diff(
    estados_hoy: dict[str, dict],
    estados_ayer: dict[str, dict],
    df_lineup_hoy: pd.DataFrame,
    df_lineup_ayer: pd.DataFrame,
    df_djve: pd.DataFrame,
    fecha_corte_djve: date,
    max_eventos: int | None = None,
) -> list[dict]:
    """
    Combina todos los detectores y ordena por prioridad (dirección primero).

    Args:
        max_eventos: si se pasa, recorta a los N más prioritarios.

    Returns:
        Lista de eventos ordenada: DIR → BANDA → GAP → BUQUE → DJVE. Dentro de
        cada tipo, por tonelaje/magnitud descendente.
    """
    eventos: list[dict] = []
    eventos += cambios_estado(estados_hoy, estados_ayer)
    eventos += buques_nuevos(df_lineup_hoy, df_lineup_ayer)
    eventos += djve_nuevas(df_djve, fecha_corte_djve)

    def _magnitud(ev: dict) -> float:
        if ev["tipo"] in ("BUQUE", "DJVE"):
            return float(ev.get("toneladas", 0))
        if ev["tipo"] == "GAP":
            return abs(float(ev.get("delta", 0)))
        return 1e12  # DIR y BANDA siempre arriba dentro de su prioridad

    eventos.sort(key=lambda e: (e["prioridad"], -_magnitud(e)))

    if max_eventos is not None:
        return eventos[:max_eventos]
    return eventos
