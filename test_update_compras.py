"""
test_update_compras.py — Tests de la transformación a filas de la tabla compras.

Sin red, sin DB. Verifica el filtrado/normalización de _df_a_filas.
"""
import unittest
from datetime import date

import pandas as pd

import update_compras


class TestDfAFilas(unittest.TestCase):
    def _df_base(self):
        return pd.DataFrame([{
            "fecha": date(2026, 6, 1), "grano_raw": "MAIZ",
            "codigo_interno": "MAIZE", "campana": "2025/26",
            "sector": "EXPORTACION", "toneladas": 100_000.0,
            "porcentaje_cosecha": 45.0,
        }])

    def test_fila_valida_pasa(self):
        filas = update_compras._df_a_filas(self._df_base())
        self.assertEqual(len(filas), 1)
        self.assertEqual(filas[0]["codigo_interno"], "MAIZE")
        self.assertEqual(filas[0]["fecha"], "2026-06-01")  # ISO str

    def test_descarta_sin_codigo(self):
        df = self._df_base()
        df.loc[0, "codigo_interno"] = None
        self.assertEqual(update_compras._df_a_filas(df), [])

    def test_descarta_sin_fecha(self):
        df = self._df_base()
        df.loc[0, "fecha"] = None
        self.assertEqual(update_compras._df_a_filas(df), [])

    def test_deriva_campana_faltante(self):
        df = self._df_base()
        df.loc[0, "campana"] = None
        filas = update_compras._df_a_filas(df)
        self.assertEqual(len(filas), 1)
        # MAIZE 1-jun-2026 → campaña 2026/27 (arranca 1-mar-2026).
        self.assertEqual(filas[0]["campana"], "2026/27")

    def test_columnas_opcionales_se_completan(self):
        filas = update_compras._df_a_filas(self._df_base())
        for col in ("toneladas_a_fijar", "precio_promedio_usd"):
            self.assertIn(col, filas[0])

    def test_nan_a_none(self):
        df = self._df_base()
        df.loc[0, "porcentaje_cosecha"] = float("nan")
        filas = update_compras._df_a_filas(df)
        self.assertIsNone(filas[0]["porcentaje_cosecha"])

    def test_df_vacio(self):
        self.assertEqual(update_compras._df_a_filas(pd.DataFrame()), [])

    def test_sector_faltante_default_exportacion(self):
        df = self._df_base().drop(columns=["sector"])
        filas = update_compras._df_a_filas(df)
        self.assertEqual(len(filas), 1)
        self.assertEqual(filas[0]["sector"], "EXPORTACION")


if __name__ == "__main__":
    unittest.main(verbosity=2)
