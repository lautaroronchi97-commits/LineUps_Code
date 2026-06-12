"""
test_mesa_calor.py — Tests del índice de calor, bandas, dirección y matriz.

Sin red, sin DB. DataFrames sintéticos.
"""
import unittest
from datetime import date, timedelta

import pandas as pd

import mesa_calor
from mesa_calor import (
    accion_sugerida,
    clasificar_banda,
    clasificar_direccion,
    equivalente_poroto,
    gap_cobertura,
    indice_calor,
    sparkline_svg,
    tonelaje_lineup,
)

REF = date(2026, 6, 1)


def _djve(codigo, tn, ini_dias=0, fin_dias=30, ref=REF):
    return {
        "nro_djve": 1,
        "razon_social": "CARGILL SA",
        "codigo_interno": codigo,
        "producto": codigo,
        "toneladas": tn,
        "fecha_registro": ref,
        "fecha_inicio_embarque": ref + timedelta(days=ini_dias),
        "fecha_fin_embarque": ref + timedelta(days=fin_dias),
    }


def _vessel(codigo, tn, etb_dias, ref=REF):
    return {
        "vessel": f"MV_{codigo}_{etb_dias}",
        "cargo": codigo,
        "quantity": tn,
        "etb": ref + timedelta(days=etb_dias),
        "shipper_canon": "CARGILL",
        "port": "TIMBUES",
    }


class TestClasificarBanda(unittest.TestCase):
    def test_caliente(self):
        self.assertEqual(clasificar_banda(85), "CALIENTE")
        self.assertEqual(clasificar_banda(80), "CALIENTE")

    def test_firme(self):
        self.assertEqual(clasificar_banda(70), "FIRME")

    def test_neutro(self):
        self.assertEqual(clasificar_banda(50), "NEUTRO")

    def test_pesado(self):
        self.assertEqual(clasificar_banda(30), "PESADO")

    def test_muy_pesado(self):
        self.assertEqual(clasificar_banda(10), "MUY PESADO")

    def test_none_es_sin_historia(self):
        self.assertEqual(clasificar_banda(None), "SIN HISTORIA")


class TestClasificarDireccion(unittest.TestCase):
    def test_abriendose(self):
        self.assertEqual(clasificar_direccion(50_000), "ABRIENDOSE")

    def test_cerrandose(self):
        self.assertEqual(clasificar_direccion(-50_000), "CERRANDOSE")

    def test_estable(self):
        self.assertEqual(clasificar_direccion(1_000), "ESTABLE")

    def test_none_sin_dato(self):
        self.assertEqual(clasificar_direccion(None), "SIN DATO")

    def test_umbral_exacto_abriendose(self):
        self.assertEqual(
            clasificar_direccion(mesa_calor.UMBRAL_DIRECCION_TN), "ABRIENDOSE"
        )


class TestAccionSugerida(unittest.TestCase):
    def test_caliente_abriendose_difiere(self):
        accion, _ = accion_sugerida("CALIENTE", "ABRIENDOSE")
        self.assertEqual(accion, "DIFERIR")

    def test_caliente_cerrandose_vender_ya(self):
        accion, _ = accion_sugerida("CALIENTE", "CERRANDOSE")
        self.assertEqual(accion, "VENDER YA")

    def test_pesado_cerrandose_comprar(self):
        accion, _ = accion_sugerida("PESADO", "CERRANDOSE")
        self.assertEqual(accion, "COMPRAR BARATO")

    def test_firme_se_trata_como_caliente(self):
        accion, _ = accion_sugerida("FIRME", "ABRIENDOSE")
        self.assertEqual(accion, "DIFERIR")

    def test_sin_historia_da_guion(self):
        accion, _ = accion_sugerida("SIN HISTORIA", "ESTABLE")
        self.assertEqual(accion, "—")

    def test_sin_direccion_da_guion(self):
        accion, _ = accion_sugerida("CALIENTE", "SIN DATO")
        self.assertEqual(accion, "—")


class TestEquivalentePoroto(unittest.TestCase):
    def test_suma_harina_y_aceite(self):
        # 74.5k harina / 0.745 = 100k; 19k aceite / 0.19 = 100k → 200k
        eq = equivalente_poroto(74_500, 19_000)
        self.assertAlmostEqual(eq, 200_000, places=0)

    def test_solo_harina(self):
        eq = equivalente_poroto(74_500, 0)
        self.assertAlmostEqual(eq, 100_000, places=0)

    def test_cero(self):
        self.assertEqual(equivalente_poroto(0, 0), 0.0)


class TestGapCobertura(unittest.TestCase):
    def test_gap_positivo_cuando_corto(self):
        djve = pd.DataFrame([_djve("MAIZE", 200_000)])
        lineup = pd.DataFrame([_vessel("MAIZE", 65_000, 10)])
        gap = gap_cobertura(djve, lineup, REF, "MAIZE")
        self.assertAlmostEqual(gap, 135_000, places=0)

    def test_gap_negativo_cuando_sobre_originado(self):
        djve = pd.DataFrame([_djve("MAIZE", 50_000)])
        lineup = pd.DataFrame([_vessel("MAIZE", 65_000, 10)])
        gap = gap_cobertura(djve, lineup, REF, "MAIZE")
        self.assertAlmostEqual(gap, -15_000, places=0)

    def test_soja_crush_agrega_derivados(self):
        # DJVE: 74.5k SBM + 19k SBO → 200k poroto eq declarado; sin lineup.
        djve = pd.DataFrame([
            _djve("SBM", 74_500),
            _djve("SBO", 19_000),
        ])
        gap = gap_cobertura(djve, pd.DataFrame(), REF, "SOJA_CRUSH")
        self.assertAlmostEqual(gap, 200_000, places=-2)

    def test_dfs_vacios_da_cero(self):
        self.assertEqual(
            gap_cobertura(pd.DataFrame(), pd.DataFrame(), REF, "MAIZE"), 0.0
        )


class TestTonelajeLineup(unittest.TestCase):
    def test_suma_en_horizonte(self):
        lineup = pd.DataFrame([
            _vessel("MAIZE", 65_000, 5),
            _vessel("MAIZE", 30_000, 20),
            _vessel("MAIZE", 50_000, 40),  # fuera del horizonte 30d
        ])
        tn = tonelaje_lineup(lineup, REF, "MAIZE", horizonte_dias=30)
        self.assertAlmostEqual(tn, 95_000, places=0)

    def test_soja_crush_equivalente(self):
        lineup = pd.DataFrame([
            _vessel("SBM", 74_500, 5),
            _vessel("SBO", 19_000, 5),
        ])
        tn = tonelaje_lineup(lineup, REF, "SOJA_CRUSH")
        self.assertAlmostEqual(tn, 200_000, places=-2)

    def test_vacio_da_cero(self):
        self.assertEqual(tonelaje_lineup(pd.DataFrame(), REF, "MAIZE"), 0.0)


class TestIndiceCalor(unittest.TestCase):
    def test_todos_los_componentes(self):
        # gap 100, lineup 100, avance 0 (invertido → 100) → 100
        calor = indice_calor(100, 100, 0)
        self.assertAlmostEqual(calor, 100.0, places=1)

    def test_farmer_se_invierte(self):
        # avance alto (100) → componente farmer = 0
        calor = indice_calor(0, 0, 100)
        self.assertAlmostEqual(calor, 0.0, places=1)

    def test_componente_faltante_renormaliza(self):
        # Solo gap disponible = 80 → índice = 80 (renormalizado)
        calor = indice_calor(80, None, None)
        self.assertAlmostEqual(calor, 80.0, places=1)

    def test_todos_none_da_none(self):
        self.assertIsNone(indice_calor(None, None, None))

    def test_demanda_domina(self):
        # gap+lineup altos (100,100), farmer alto (avance 100 → 0).
        # peso demanda 0.65 vs farmer 0.35 → 100*0.65/(1.0)... renormaliza a 1.
        calor = indice_calor(100, 100, 100)
        # (100*.35 + 100*.30 + 0*.35) / 1.0 = 65
        self.assertAlmostEqual(calor, 65.0, places=1)


class TestSparklineSvg(unittest.TestCase):
    def test_devuelve_svg(self):
        svg = sparkline_svg([1, 2, 3, 4, 5])
        self.assertTrue(svg.startswith("<svg"))
        self.assertTrue(svg.endswith("</svg>"))
        self.assertIn("polyline", svg)

    def test_pocos_puntos_svg_vacio(self):
        svg = sparkline_svg([1])
        self.assertTrue(svg.startswith("<svg"))
        self.assertNotIn("polyline", svg)

    def test_marca_ultimo_punto(self):
        svg = sparkline_svg([1, 2, 3])
        self.assertIn("circle", svg)

    def test_ignora_nan(self):
        svg = sparkline_svg([1, None, 3, float("nan"), 5])
        self.assertIn("polyline", svg)


if __name__ == "__main__":
    unittest.main(verbosity=2)
