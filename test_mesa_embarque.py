"""
test_mesa_embarque.py — Tests de la matriz por mes de embarque.

Sin red, sin DB.
"""
import unittest
from datetime import date

import pandas as pd

import mesa_embarque
from mesa_embarque import gap_por_mes, meses_proximos

REF = date(2026, 6, 1)


class TestMesesProximos(unittest.TestCase):
    def test_devuelve_n_meses(self):
        meses = meses_proximos(REF, 6)
        self.assertEqual(len(meses), 6)

    def test_incluye_mes_actual(self):
        meses = meses_proximos(REF, 6)
        self.assertEqual(meses[0], (2026, 6))

    def test_cruza_anio(self):
        meses = meses_proximos(date(2026, 11, 1), 4)
        self.assertEqual(meses, [(2026, 11), (2026, 12), (2027, 1), (2027, 2)])


class TestLimitesMes(unittest.TestCase):
    def test_mes_normal(self):
        ini, fin = mesa_embarque._limites_mes(2026, 6)
        self.assertEqual(ini, date(2026, 6, 1))
        self.assertEqual(fin, date(2026, 6, 30))

    def test_diciembre(self):
        ini, fin = mesa_embarque._limites_mes(2026, 12)
        self.assertEqual(ini, date(2026, 12, 1))
        self.assertEqual(fin, date(2026, 12, 31))

    def test_febrero_no_bisiesto(self):
        ini, fin = mesa_embarque._limites_mes(2026, 2)
        self.assertEqual(fin, date(2026, 2, 28))


class TestGapPorMes(unittest.TestCase):
    def test_djve_en_mes_junio(self):
        # DJVE con ventana de embarque en junio.
        djve = pd.DataFrame([{
            "codigo_interno": "MAIZE",
            "toneladas": 100_000,
            "fecha_inicio_embarque": date(2026, 6, 5),
            "fecha_fin_embarque": date(2026, 6, 25),
        }])
        lineup = pd.DataFrame([{
            "cargo": "MAIZE",
            "quantity": 65_000,
            "etb": date(2026, 6, 10),
        }])
        df = gap_por_mes(djve, lineup, REF, "MAIZE", n_meses=6)
        junio = df[(df["anio"] == 2026) & (df["mes"] == 6)].iloc[0]
        self.assertAlmostEqual(junio["declarado_tn"], 100_000, places=0)
        self.assertAlmostEqual(junio["originado_tn"], 65_000, places=0)
        self.assertAlmostEqual(junio["gap_tn"], 35_000, places=0)
        self.assertEqual(junio["n_buques"], 1)

    def test_devuelve_fila_por_mes(self):
        df = gap_por_mes(pd.DataFrame(), pd.DataFrame(), REF, "MAIZE", n_meses=6)
        self.assertEqual(len(df), 6)

    def test_soja_crush_equivalente_poroto(self):
        djve = pd.DataFrame([
            {
                "codigo_interno": "SBM", "toneladas": 74_500,
                "fecha_inicio_embarque": date(2026, 6, 5),
                "fecha_fin_embarque": date(2026, 6, 25),
            },
            {
                "codigo_interno": "SBO", "toneladas": 19_000,
                "fecha_inicio_embarque": date(2026, 6, 5),
                "fecha_fin_embarque": date(2026, 6, 25),
            },
        ])
        df = gap_por_mes(djve, pd.DataFrame(), REF, "SOJA_CRUSH", n_meses=2)
        junio = df[df["mes"] == 6].iloc[0]
        self.assertAlmostEqual(junio["declarado_tn"], 200_000, places=-2)

    def test_mes_sin_datos_da_cero(self):
        df = gap_por_mes(pd.DataFrame(), pd.DataFrame(), REF, "WHEAT", n_meses=3)
        self.assertTrue((df["gap_tn"] == 0).all())


if __name__ == "__main__":
    unittest.main(verbosity=2)
