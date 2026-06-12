"""
test_estacional.py — Tests del motor de percentiles estacionales.

Sin red, sin DB. Series sintéticas.
"""
import unittest
from datetime import date, timedelta

import pandas as pd

import estacional
from estacional import (
    construir_serie,
    fechas_estacionales,
    percentil_en_serie,
    percentil_estacional,
)


class TestPercentilEnSerie(unittest.TestCase):
    def test_valor_maximo_da_100(self):
        self.assertEqual(percentil_en_serie([1, 2, 3, 4], 5), 100.0)

    def test_valor_igual_al_maximo_da_100(self):
        self.assertEqual(percentil_en_serie([1, 2, 3, 4], 4), 100.0)

    def test_valor_minimo(self):
        # 1 de 4 valores <= 1 → 25
        self.assertEqual(percentil_en_serie([1, 2, 3, 4], 1), 25.0)

    def test_valor_mediano(self):
        # 2 de 4 <= 2 → 50
        self.assertEqual(percentil_en_serie([1, 2, 3, 4], 2), 50.0)

    def test_serie_vacia_da_nan(self):
        self.assertTrue(pd.isna(percentil_en_serie([], 5)))


class TestFechasEstacionales(unittest.TestCase):
    def test_devuelve_n_campanas(self):
        vts = fechas_estacionales("MAIZE", date(2026, 6, 1), n_campanas=5)
        self.assertEqual(len(vts), 5)

    def test_ventana_centrada_en_equivalente(self):
        vts = fechas_estacionales("MAIZE", date(2026, 6, 1),
                                  ventana_dias=15, n_campanas=3)
        for _camp, desde, hasta in vts:
            self.assertEqual((hasta - desde).days, 30)

    def test_campanas_son_previas(self):
        vts = fechas_estacionales("MAIZE", date(2026, 6, 1), n_campanas=3)
        # MAIZE jun-2026 cae en campaña 2026/27 → previas 2025/26, 2024/25, 2023/24
        camps = [c for c, _, _ in vts]
        self.assertEqual(camps, ["2025/26", "2024/25", "2023/24"])


class TestPercentilEstacional(unittest.TestCase):
    def _serie_constante(self, producto, fecha_ref, valor, n_campanas=5):
        """Construye historia con el mismo valor en cada ventana estacional."""
        registros = []
        for _camp, desde, hasta in fechas_estacionales(
            producto, fecha_ref, n_campanas=n_campanas
        ):
            d = desde
            while d <= hasta:
                registros.append((d, producto, valor))
                d += timedelta(days=3)
        return construir_serie(registros)

    def test_valor_alto_da_percentil_alto(self):
        ref = date(2026, 6, 1)
        serie = self._serie_constante("MAIZE", ref, 100_000)
        pctl = percentil_estacional(serie, "MAIZE", ref, 500_000)
        self.assertEqual(pctl, 100.0)

    def test_valor_bajo_da_percentil_bajo(self):
        ref = date(2026, 6, 1)
        serie = self._serie_constante("MAIZE", ref, 100_000)
        pctl = percentil_estacional(serie, "MAIZE", ref, 50_000)
        # Todos los históricos son 100k > 50k → 0 valores <= 50k → 0
        self.assertEqual(pctl, 0.0)

    def test_valor_none_devuelve_none(self):
        ref = date(2026, 6, 1)
        serie = self._serie_constante("MAIZE", ref, 100_000)
        self.assertIsNone(percentil_estacional(serie, "MAIZE", ref, None))

    def test_serie_vacia_devuelve_none(self):
        ref = date(2026, 6, 1)
        self.assertIsNone(
            percentil_estacional(pd.DataFrame(), "MAIZE", ref, 100_000)
        )

    def test_historia_insuficiente_devuelve_none(self):
        # Solo 1 campaña con datos → menos que min_campanas=2 → None
        ref = date(2026, 6, 1)
        _camp, desde, hasta = fechas_estacionales("MAIZE", ref, n_campanas=5)[0]
        registros = []
        d = desde
        while d <= hasta:
            registros.append((d, "MAIZE", 100_000))
            d += timedelta(days=3)
        serie = construir_serie(registros)
        self.assertIsNone(percentil_estacional(serie, "MAIZE", ref, 120_000))

    def test_producto_inexistente_devuelve_none(self):
        ref = date(2026, 6, 1)
        serie = self._serie_constante("MAIZE", ref, 100_000)
        self.assertIsNone(percentil_estacional(serie, "WHEAT", ref, 100_000))

    def test_percentil_intermedio(self):
        # Historia con valores 0,50k,100k,150k,200k en cada campaña.
        ref = date(2026, 6, 1)
        registros = []
        for _camp, desde, _hasta in fechas_estacionales("MAIZE", ref, n_campanas=5):
            for k, v in enumerate([0, 50_000, 100_000, 150_000, 200_000]):
                registros.append((desde + timedelta(days=k), "MAIZE", v))
        serie = construir_serie(registros)
        pctl = percentil_estacional(serie, "MAIZE", ref, 100_000)
        # 3 de 5 valores por campaña <= 100k (0,50k,100k) → 60
        self.assertEqual(pctl, 60.0)


class TestConstruirSerie(unittest.TestCase):
    def test_vacio(self):
        df = construir_serie([])
        self.assertTrue(df.empty)
        self.assertListEqual(list(df.columns), ["fecha", "codigo_interno", "valor"])

    def test_construye_columnas(self):
        df = construir_serie([(date(2026, 1, 1), "MAIZE", 100.0)])
        self.assertEqual(df.iloc[0]["valor"], 100.0)
        self.assertEqual(df.iloc[0]["codigo_interno"], "MAIZE")


if __name__ == "__main__":
    unittest.main(verbosity=2)
