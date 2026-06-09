"""
Tests unitarios de cobertura.py (cruce DECLARADO vs ORIGINADO -> SEÑAL).

Sin red ni DB: construimos DataFrames sinteticos de DJVE y line-up a mano.
Verificamos:
  - El cruce por shipper canonicalizado ("CARGILL SACI" DJVE == "CARGILL" line-up).
  - falta_cubrir_tn correcto en los tres casos: corto, cubierto, sobre-originado.
  - senales_trading emite ALCISTA (ratio<0.7), BAJISTA (ratio>1.3), CONGESTION.
  - Casos borde: DataFrames vacios, producto sin DJVE, producto sin line-up.

Correr: python -m unittest test_cobertura -v
"""
from __future__ import annotations

import math
import unittest
from datetime import date

import pandas as pd

import cobertura


# ---------------------------------------------------------------------------
# Fixtures sinteticos
# ---------------------------------------------------------------------------

FECHA_REF = date(2026, 6, 1)


def _djve(filas: list[dict]) -> pd.DataFrame:
    """
    Construye un DataFrame DJVE con el esquema que devuelve fob_djve.
    Cada fila acepta: razon_social, codigo_interno, toneladas,
    fecha_inicio_embarque, fecha_fin_embarque.
    """
    cols = [
        "nro_djve", "fecha_registro", "fecha_presentacion", "producto",
        "toneladas", "fecha_inicio_embarque", "fecha_fin_embarque",
        "opcion", "razon_social", "codigo_interno",
    ]
    if not filas:
        return pd.DataFrame(columns=cols)
    df = pd.DataFrame(filas)
    for c in cols:
        if c not in df.columns:
            df[c] = None
    return df[cols]


def _lineup(filas: list[dict]) -> pd.DataFrame:
    """
    Construye un DataFrame line-up con el esquema de query_exports_prioritarios.
    Cada fila acepta: shipper_canon, cargo, quantity, etb, port, vessel.
    """
    cols = [
        "port", "vessel", "ops", "cat", "cargo", "quantity",
        "shipper", "shipper_canon", "origen_alt", "eta", "etb", "ets",
    ]
    if not filas:
        return pd.DataFrame(columns=cols)
    df = pd.DataFrame(filas)
    for c in cols:
        if c not in df.columns:
            df[c] = None
    if "ops" in df.columns:
        df["ops"] = df["ops"].fillna("LOAD")
    return df[cols]


# ---------------------------------------------------------------------------
# 1. Canonicalizacion DJVE + cruce por shipper
# ---------------------------------------------------------------------------

class TestCanonicalizacionYCruce(unittest.TestCase):

    def test_canonicalizar_djve_agrega_shipper_canon(self):
        df = _djve([
            {"razon_social": "CARGILL S.A.C.I.", "codigo_interno": "SBS",
             "toneladas": 60000,
             "fecha_inicio_embarque": date(2026, 6, 10),
             "fecha_fin_embarque": date(2026, 6, 20)},
        ])
        out = cobertura.canonicalizar_djve(df)
        self.assertIn("shipper_canon", out.columns)
        self.assertEqual(out.loc[0, "shipper_canon"], "CARGILL")

    def test_cruce_nombre_legal_djve_matchea_canon_lineup(self):
        # DJVE usa "CARGILL SACI"; line-up usa "CARGILL". Deben cruzar.
        df_djve = _djve([
            {"razon_social": "CARGILL SACI", "codigo_interno": "MAIZE",
             "toneladas": 100000,
             "fecha_inicio_embarque": date(2026, 6, 10),
             "fecha_fin_embarque": date(2026, 6, 25)},
        ])
        df_lineup = _lineup([
            {"shipper_canon": "CARGILL", "cargo": "MAIZE", "quantity": 60000,
             "etb": date(2026, 6, 15), "port": "TIMBUES", "vessel": "MV TEST"},
        ])
        bal = cobertura.balance_por_shipper(df_djve, df_lineup, FECHA_REF, 60)
        self.assertEqual(len(bal), 1)
        fila = bal.iloc[0]
        self.assertEqual(fila["shipper_canon"], "CARGILL")
        self.assertEqual(fila["codigo_interno"], "MAIZE")
        self.assertEqual(fila["declarado_tn"], 100000)
        self.assertEqual(fila["originado_tn"], 60000)
        self.assertEqual(fila["falta_cubrir_tn"], 40000)

    def test_viterra_bunge_se_fusionan_en_el_cruce(self):
        # "OLEAGINOSA MORENO" (DJVE) y "BUNGE" (line-up) -> ambos VITERRA-BUNGE.
        df_djve = _djve([
            {"razon_social": "OLEAGINOSA MORENO HNOS S.A.",
             "codigo_interno": "SBS", "toneladas": 50000,
             "fecha_inicio_embarque": date(2026, 6, 5),
             "fecha_fin_embarque": date(2026, 6, 15)},
        ])
        df_lineup = _lineup([
            {"shipper_canon": "VITERRA-BUNGE", "cargo": "SBS", "quantity": 50000,
             "etb": date(2026, 6, 10), "port": "SAN LORENZO", "vessel": "MV X"},
        ])
        bal = cobertura.balance_por_shipper(df_djve, df_lineup, FECHA_REF, 60)
        self.assertEqual(len(bal), 1)
        self.assertEqual(bal.iloc[0]["shipper_canon"], "VITERRA-BUNGE")
        self.assertEqual(bal.iloc[0]["ratio_cobertura"], 1.0)


# ---------------------------------------------------------------------------
# 2. falta_cubrir_tn: corto / cubierto / sobre-originado
# ---------------------------------------------------------------------------

class TestBalancePorProducto(unittest.TestCase):

    def test_caso_corto(self):
        # Declarado 100k, originado 50k -> falta 50k, ratio 0.5.
        df_djve = _djve([
            {"razon_social": "LDC ARGENTINA S.A.", "codigo_interno": "WHEAT",
             "toneladas": 100000,
             "fecha_inicio_embarque": date(2026, 6, 10),
             "fecha_fin_embarque": date(2026, 6, 20)},
        ])
        df_lineup = _lineup([
            {"shipper_canon": "LDC", "cargo": "WHEAT", "quantity": 50000,
             "etb": date(2026, 6, 15), "port": "ROSARIO", "vessel": "MV W"},
        ])
        bal = cobertura.balance_por_producto(df_djve, df_lineup, FECHA_REF, 60)
        fila = bal.iloc[0]
        self.assertEqual(fila["falta_cubrir_tn"], 50000)
        self.assertAlmostEqual(fila["ratio_cobertura"], 0.5)

    def test_caso_cubierto(self):
        # Declarado == originado -> falta 0, ratio 1.0.
        df_djve = _djve([
            {"razon_social": "ADM AGRO S.A.", "codigo_interno": "SORGHUM",
             "toneladas": 30000,
             "fecha_inicio_embarque": date(2026, 6, 1),
             "fecha_fin_embarque": date(2026, 6, 30)},
        ])
        df_lineup = _lineup([
            {"shipper_canon": "ADM", "cargo": "SORGHUM", "quantity": 30000,
             "etb": date(2026, 6, 12), "port": "TIMBUES", "vessel": "MV S"},
        ])
        bal = cobertura.balance_por_producto(df_djve, df_lineup, FECHA_REF, 60)
        fila = bal.iloc[0]
        self.assertEqual(fila["falta_cubrir_tn"], 0)
        self.assertAlmostEqual(fila["ratio_cobertura"], 1.0)

    def test_caso_sobre_originado(self):
        # Declarado 40k, originado 80k -> falta -40k, ratio 2.0.
        df_djve = _djve([
            {"razon_social": "COFCO INTERNATIONAL ARGENTINA S.A.",
             "codigo_interno": "SBM", "toneladas": 40000,
             "fecha_inicio_embarque": date(2026, 6, 5),
             "fecha_fin_embarque": date(2026, 6, 25)},
        ])
        df_lineup = _lineup([
            {"shipper_canon": "COFCO", "cargo": "SBM", "quantity": 80000,
             "etb": date(2026, 6, 14), "port": "SAN MARTIN", "vessel": "MV M"},
        ])
        bal = cobertura.balance_por_producto(df_djve, df_lineup, FECHA_REF, 60)
        fila = bal.iloc[0]
        self.assertEqual(fila["falta_cubrir_tn"], -40000)
        self.assertAlmostEqual(fila["ratio_cobertura"], 2.0)

    def test_producto_display_se_mapea(self):
        df_djve = _djve([
            {"razon_social": "CARGILL", "codigo_interno": "SBS",
             "toneladas": 10000,
             "fecha_inicio_embarque": date(2026, 6, 10),
             "fecha_fin_embarque": date(2026, 6, 20)},
        ])
        bal = cobertura.balance_por_producto(df_djve, _lineup([]), FECHA_REF, 60)
        self.assertEqual(bal.iloc[0]["producto_display"], "Soja")

    def test_ventana_fuera_de_horizonte_se_excluye(self):
        # Ventana de embarque arranca 200 dias despues -> fuera de horizonte 60.
        df_djve = _djve([
            {"razon_social": "CARGILL", "codigo_interno": "MAIZE",
             "toneladas": 90000,
             "fecha_inicio_embarque": date(2026, 12, 1),
             "fecha_fin_embarque": date(2026, 12, 31)},
        ])
        bal = cobertura.balance_por_producto(df_djve, _lineup([]), FECHA_REF, 60)
        self.assertTrue(bal.empty)


# ---------------------------------------------------------------------------
# 3. Señales de trading
# ---------------------------------------------------------------------------

class TestSenalesTrading(unittest.TestCase):

    def test_emite_alcista_cuando_ratio_menor_07(self):
        # Declarado 200k, originado 80k -> ratio 0.4 < 0.7 -> ALCISTA FAS.
        df_djve = _djve([
            {"razon_social": "CARGILL", "codigo_interno": "MAIZE",
             "toneladas": 200000,
             "fecha_inicio_embarque": date(2026, 6, 10),
             "fecha_fin_embarque": date(2026, 6, 20)},
        ])
        df_lineup = _lineup([
            {"shipper_canon": "CARGILL", "cargo": "MAIZE", "quantity": 80000,
             "etb": date(2026, 6, 15), "port": "TIMBUES", "vessel": "MV A"},
        ])
        bal = cobertura.balance_por_producto(df_djve, df_lineup, FECHA_REF, 60)
        carga = cobertura.carga_lineup(df_lineup, FECHA_REF)
        senales = cobertura.senales_trading(bal, carga)
        maiz = senales[senales["codigo_interno"] == "MAIZE"]
        self.assertEqual(len(maiz), 1)
        self.assertEqual(maiz.iloc[0]["señal"], "ALCISTA FAS")
        # falta 120k -> intensidad 2 (>=60k y <180k).
        self.assertEqual(maiz.iloc[0]["intensidad"], 2)

    def test_emite_bajista_cuando_ratio_mayor_13(self):
        # Declarado 50k, originado 100k -> ratio 2.0 > 1.3 -> BAJISTA.
        df_djve = _djve([
            {"razon_social": "COFCO", "codigo_interno": "SBM",
             "toneladas": 50000,
             "fecha_inicio_embarque": date(2026, 6, 5),
             "fecha_fin_embarque": date(2026, 6, 25)},
        ])
        df_lineup = _lineup([
            {"shipper_canon": "COFCO", "cargo": "SBM", "quantity": 100000,
             "etb": date(2026, 6, 14), "port": "SAN MARTIN", "vessel": "MV B"},
        ])
        bal = cobertura.balance_por_producto(df_djve, df_lineup, FECHA_REF, 60)
        carga = cobertura.carga_lineup(df_lineup, FECHA_REF)
        senales = cobertura.senales_trading(bal, carga)
        sbm = senales[senales["codigo_interno"] == "SBM"]
        self.assertEqual(len(sbm), 1)
        self.assertEqual(sbm.iloc[0]["señal"], "BAJISTA")

    def test_no_emite_alcista_si_declarado_no_significativo(self):
        # Declarado 3k (< umbral 5k) aunque ratio bajo -> no ALCISTA.
        df_djve = _djve([
            {"razon_social": "OTRA EMPRESA", "codigo_interno": "BARLEY",
             "toneladas": 3000,
             "fecha_inicio_embarque": date(2026, 6, 10),
             "fecha_fin_embarque": date(2026, 6, 20)},
        ])
        bal = cobertura.balance_por_producto(df_djve, _lineup([]), FECHA_REF, 60)
        senales = cobertura.senales_trading(bal, {})
        self.assertTrue(
            senales[senales["codigo_interno"] == "BARLEY"].empty
        )

    def test_emite_congestion_cuando_semana_supera_umbral(self):
        # 7 buques de 60k = 420k en la misma semana -> CONGESTION.
        filas = [
            {"shipper_canon": "CARGILL", "cargo": "SBS", "quantity": 60000,
             "etb": date(2026, 6, 8 + (i % 3)), "port": "TIMBUES",
             "vessel": f"MV C{i}"}
            for i in range(7)
        ]
        df_lineup = _lineup(filas)
        carga = cobertura.carga_lineup(df_lineup, FECHA_REF)
        self.assertTrue(carga["congestion"])
        senales = cobertura.senales_trading(pd.DataFrame(), carga)
        cong = senales[senales["señal"] == "CONGESTION"]
        self.assertEqual(len(cong), 1)
        self.assertGreaterEqual(cong.iloc[0]["intensidad"], 3)


# ---------------------------------------------------------------------------
# 4. Carga del line-up
# ---------------------------------------------------------------------------

class TestCargaLineup(unittest.TestCase):

    def test_metricas_basicas(self):
        df_lineup = _lineup([
            {"shipper_canon": "CARGILL", "cargo": "SBS", "quantity": 60000,
             "etb": date(2026, 6, 10), "port": "TIMBUES", "vessel": "MV 1"},
            {"shipper_canon": "LDC", "cargo": "MAIZE", "quantity": 40000,
             "etb": date(2026, 6, 12), "port": "ROSARIO", "vessel": "MV 2"},
        ])
        carga = cobertura.carga_lineup(df_lineup, FECHA_REF)
        self.assertEqual(carga["n_buques"], 2)
        self.assertEqual(carga["toneladas_total"], 100000)
        self.assertEqual(carga["toneladas_por_puerto"]["TIMBUES"], 60000)
        self.assertFalse(carga["congestion"])

    def test_excluye_buques_con_etb_pasado(self):
        df_lineup = _lineup([
            {"shipper_canon": "CARGILL", "cargo": "SBS", "quantity": 60000,
             "etb": date(2026, 5, 1), "port": "TIMBUES", "vessel": "MV OLD"},
        ])
        carga = cobertura.carga_lineup(df_lineup, FECHA_REF)
        self.assertEqual(carga["n_buques"], 0)
        self.assertEqual(carga["toneladas_total"], 0.0)


# ---------------------------------------------------------------------------
# 5. Casos borde
# ---------------------------------------------------------------------------

class TestCasosBorde(unittest.TestCase):

    def test_djve_y_lineup_vacios(self):
        bal_p = cobertura.balance_por_producto(_djve([]), _lineup([]), FECHA_REF)
        bal_s = cobertura.balance_por_shipper(_djve([]), _lineup([]), FECHA_REF)
        carga = cobertura.carga_lineup(_lineup([]), FECHA_REF)
        senales = cobertura.senales_trading(bal_p, carga)
        self.assertTrue(bal_p.empty)
        self.assertTrue(bal_s.empty)
        self.assertEqual(carga["n_buques"], 0)
        self.assertTrue(senales.empty)

    def test_canonicalizar_djve_vacio(self):
        out = cobertura.canonicalizar_djve(_djve([]))
        self.assertIn("shipper_canon", out.columns)
        self.assertTrue(out.empty)

    def test_producto_sin_lineup_queda_corto_total(self):
        # Declarado sin nada en line-up -> originado 0, ratio 0, falta = declarado.
        df_djve = _djve([
            {"razon_social": "CARGILL", "codigo_interno": "MAIZE",
             "toneladas": 120000,
             "fecha_inicio_embarque": date(2026, 6, 10),
             "fecha_fin_embarque": date(2026, 6, 20)},
        ])
        bal = cobertura.balance_por_producto(df_djve, _lineup([]), FECHA_REF, 60)
        fila = bal.iloc[0]
        self.assertEqual(fila["originado_tn"], 0)
        self.assertEqual(fila["falta_cubrir_tn"], 120000)
        self.assertEqual(fila["ratio_cobertura"], 0.0)
        senales = cobertura.senales_trading(bal, {})
        self.assertEqual(senales.iloc[0]["señal"], "ALCISTA FAS")

    def test_producto_sin_djve_es_originado_sin_declarar(self):
        # Line-up sin DJVE -> declarado 0, ratio inf -> BAJISTA (sobre-originado).
        df_lineup = _lineup([
            {"shipper_canon": "CARGILL", "cargo": "SFSEED", "quantity": 25000,
             "etb": date(2026, 6, 15), "port": "BAHIA BLANCA", "vessel": "MV G"},
        ])
        bal = cobertura.balance_por_producto(_djve([]), df_lineup, FECHA_REF, 60)
        fila = bal.iloc[0]
        self.assertEqual(fila["declarado_tn"], 0)
        self.assertEqual(fila["originado_tn"], 25000)
        self.assertTrue(math.isinf(fila["ratio_cobertura"]))
        senales = cobertura.senales_trading(bal, {})
        self.assertEqual(senales.iloc[0]["señal"], "BAJISTA")

    def test_concentracion_por_shipper(self):
        df_djve = _djve([
            {"razon_social": "CARGILL SACI", "codigo_interno": "SBS",
             "toneladas": 75000,
             "fecha_inicio_embarque": date(2026, 6, 5),
             "fecha_fin_embarque": date(2026, 6, 15)},
            {"razon_social": "LDC ARGENTINA", "codigo_interno": "SBS",
             "toneladas": 25000,
             "fecha_inicio_embarque": date(2026, 6, 5),
             "fecha_fin_embarque": date(2026, 6, 15)},
        ])
        conc = cobertura.concentracion_por_shipper(df_djve, FECHA_REF, 60)
        self.assertEqual(conc.iloc[0]["shipper_canon"], "CARGILL")
        self.assertAlmostEqual(conc.iloc[0]["share"], 0.75)


if __name__ == "__main__":
    unittest.main(verbosity=2)
