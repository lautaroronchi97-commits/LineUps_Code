"""
verificar_mesa.py — Chequeo de pre-vuelo de la pestaña MESA.

Antes de levantar el dashboard, este script responde tres preguntas:

  1. ¿Hay conexión a Supabase? (credenciales OK)
  2. ¿Hay datos cargados? (snapshots de line-up + DJVE)
  3. ¿Qué productos tienen HISTORIA ESTACIONAL suficiente para el índice de
     calor, y cuáles van a salir como "SIN HISTORIA" en las cards?

Usa exactamente la misma lógica de ventanas estacionales que el dashboard
(`estacional.fechas_estacionales` + mínimo de campañas), así el veredicto
coincide con lo que vas a ver en pantalla. No escribe nada: solo lee y reporta.

Uso:
    python verificar_mesa.py

Sale con código 0 si MESA es operable (al menos los datos mínimos), 1 si falta
algo bloqueante (sin conexión o sin snapshots).
"""
from __future__ import annotations

import sys
from datetime import date

# Productos de la mesa y el código del line-up que cada uno mira para la
# alineación de campaña (SOJA_CRUSH se alinea por el complejo soja = SBM).
_PRODUCTOS = [
    ("MAIZE", "MAIZE", "Maíz"),
    ("WHEAT", "WHEAT", "Trigo"),
    ("SOJA_CRUSH", "SBM", "Soja (crush)"),
    ("SBS", "SBS", "Soja poroto"),
]

# Códigos del line-up que aportan tonelaje a cada producto de MESA.
_CODIGOS_LINEUP = {
    "MAIZE": ["MAIZE"],
    "WHEAT": ["WHEAT"],
    "SOJA_CRUSH": ["SBM", "SBO"],
    "SBS": ["SBS"],
}


def _c(texto: str, color: str) -> str:
    """Colorea texto para la terminal (degrada a plano si no hay TTY)."""
    if not sys.stdout.isatty():
        return texto
    codigos = {"verde": "92", "rojo": "91", "ambar": "93",
               "gris": "90", "bold": "1"}
    return f"\033[{codigos.get(color, '0')}m{texto}\033[0m"


def main() -> int:
    print(_c("=== VERIFICACIÓN PRE-VUELO · PESTAÑA MESA ===", "bold"))
    print()

    # --- Imports diferidos: si faltan deps, mensaje claro en vez de traceback ---
    try:
        import pandas as pd  # noqa: F401
        import estacional
        import campanas  # noqa: F401
        import db
    except ModuleNotFoundError as exc:
        print(_c(f"✗ Falta una dependencia: {exc.name}", "rojo"))
        print("  Instalá el entorno:  pip install -r requirements.txt")
        return 1

    # --- 1. Conexión (con query real, no solo ping) ---
    print(_c("1. CONEXIÓN A SUPABASE", "bold"))
    try:
        fecha_max = db.ultima_fecha_cargada()
    except RuntimeError as exc:
        # Típicamente: faltan credenciales (SUPABASE_URL / key).
        print(_c("   ✗ Sin credenciales o sin conexión.", "rojo"))
        print(f"     {exc}")
        return 1
    except Exception as exc:
        print(_c(f"   ✗ Error de conexión: {exc}", "rojo"))
        print("     Revisá SUPABASE_URL y la key en el archivo .env")
        return 1
    print(_c("   ✓ Conectado.", "verde"))
    print()

    # --- 2. Datos cargados ---
    print(_c("2. DATOS CARGADOS", "bold"))
    fecha_min = db.primera_fecha_cargada()
    if fecha_max is None:
        print(_c("   ✗ La tabla lineup está vacía.", "rojo"))
        print("     Cargá datos:  python backfill.py  (histórico)")
        print("                   python update_today.py  (snapshot del día)")
        return 1
    print(f"   ✓ Line-up: {fecha_min} → {fecha_max}")

    # Master 5 años (lo mismo que usa el dashboard).
    from datetime import timedelta
    desde = fecha_max - timedelta(days=365 * 5 + 30)
    master = db.query_exports_prioritarios(fecha_desde=desde, fecha_hasta=fecha_max)
    if master.empty:
        print(_c("   ✗ Sin exportaciones prioritarias en los últimos 5 años.", "rojo"))
        return 1

    fechas_snap = sorted({
        f for f in pd.to_datetime(master["fecha_consulta"], errors="coerce")
        .dt.date if f is not None
    })
    print(f"   ✓ Snapshots distintos: {len(fechas_snap)}")

    # DJVE.
    try:
        djve = db.query_djve(anio=fecha_max.year)
        n_djve = 0 if djve is None or djve.empty else len(djve)
    except Exception:
        n_djve = 0
    if n_djve == 0:
        print(_c("   ⚠ DJVE vacía para el año actual.", "ambar"))
        print("     El gap de cobertura quedará en 0. Cargá:  python update_djve.py")
    else:
        print(f"   ✓ DJVE {fecha_max.year}: {n_djve} declaraciones.")
    print()

    # --- 3. Historia estacional por producto ---
    print(_c("3. HISTORIA ESTACIONAL PARA EL ÍNDICE DE CALOR", "bold"))
    print(_c(f"   (referencia: {fecha_max} · mínimo {estacional.MIN_CAMPANAS} "
             f"campañas con snapshots)", "gris"))
    print()

    min_camp = estacional.MIN_CAMPANAS
    algun_ok = False
    for prod, cod_camp, display in _PRODUCTOS:
        codigos_lu = _CODIGOS_LINEUP[prod]
        # ¿El producto siquiera aparece en el line-up?
        presente = master["cargo"].isin(codigos_lu).any()

        ventanas = estacional.fechas_estacionales(cod_camp, fecha_max)
        # Cuántas campañas previas tienen al menos un snapshot en su ventana.
        camp_con_dato = 0
        snaps_total = 0
        for _camp, d0, d1 in ventanas:
            n = sum(1 for f in fechas_snap if d0 <= f <= d1)
            if n > 0:
                camp_con_dato += 1
                snaps_total += n

        suficiente = presente and camp_con_dato >= min_camp
        if suficiente:
            algun_ok = True
            marca = _c("✓ ÍNDICE", "verde")
            detalle = (f"{camp_con_dato} campañas con historia "
                       f"({snaps_total} snapshots en ventana)")
        elif not presente:
            marca = _c("✗ SIN DATA", "rojo")
            detalle = "el producto no aparece en el line-up cargado"
        else:
            marca = _c("⚠ SIN HISTORIA", "ambar")
            detalle = (f"solo {camp_con_dato} campaña(s) con snapshots "
                       f"(necesita {min_camp}) → card mostrará '—'")

        print(f"   {marca}  {display:<14} {detalle}")

    print()
    print(_c("=== RESUMEN ===", "bold"))
    if algun_ok:
        print(_c("MESA es OPERATIVA: al menos un producto tendrá índice de "
                 "calor numérico.", "verde"))
        print("Los productos en SIN HISTORIA degradan con gracia (muestran '—'),")
        print("pero el tape, la matriz por mes y las zonas funcionan igual.")
        print()
        print("Levantar:  streamlit run dashboard.py")
        return 0
    else:
        print(_c("MESA levanta pero NINGÚN producto tiene historia estacional "
                 "suficiente.", "ambar"))
        print("Todas las cards mostrarán '—'. Cargá más campañas de snapshots")
        print("(python backfill.py con rango histórico) para activar el índice.")
        return 0


if __name__ == "__main__":
    sys.exit(main())
