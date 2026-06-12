"""
Tests unitarios para compras_fas.py.
Sin red, sin DB, solo DataFrames sintéticos.
"""
from __future__ import annotations

import math
import unittest
from datetime import date, timedelta

import pandas as pd

from compras_fas import (
    GRANO_COMPRAS_A_CODIGO,
    RATIO_COMPRA_CORTO,
    RATIO_COMPRA_LARGO,
    SECTOR_EXPORTACION,
    _mapear_grano,
    compras_acumuladas_campana,
    posicion_exportadora,
    senales_presion,
)


# ---------------------------------------------------------------------------
# Helpers de fixtures
# ---------------------------------------------------------------------------

def _make_djve(
    codigo: str,
    toneladas: float,
    dias_inicio: int = 5,
    dias_fin: int = 35,
    razon_social: str = "CARGILL SACI",
    fecha_ref: date | None = None,
) -> pd.DataFrame:
    """DJVE sintética para un producto con ventana de embarque en el horizonte."""
    if fecha_ref is None:
        fecha_ref = date(2026, 6, 1)
    return pd.DataFrame([{
        "nro_djve": 1,
        "razon_social": razon_social,
        "codigo_interno": codigo,
        "producto": codigo,
        "toneladas": toneladas,
        "fecha_registro": fecha_ref,
        "fecha_inicio_embarque": fecha_ref + timedelta(days=dias_inicio),
        "fecha_fin_embarque": fecha_ref + timedelta(days=dias_fin),
    }])


def _make_lineup(
    cargo: str,
    quantity: float,
    dias_etb: int = 10,
    shipper_canon: str = "CARGILL",
    fecha_ref: date | None = None,
) -> pd.DataFrame:
    """Line-up sintético para un producto con ETB en el horizonte."""
    if fecha_ref is None:
        fecha_ref = date(2026, 6, 1)
    return pd.DataFrame([{
        "vessel": "MV TEST",
        "cargo": cargo,
        "quantity": quantity,
        "etb": fecha_ref + timedelta(days=dias_etb),
        "shipper_canon": shipper_canon,
        "origen_alt": None,
        "ops": "LOAD",
    }])


def _make_compras(
    codigo: str,
    toneladas: float,
    campana: str = "2025/26",
    sector: str = SECTOR_EXPORTACION,
    fecha: date | None = None,
) -> pd.DataFrame:
    """Compras FAS sintéticas."""
    if fecha is None:
        fecha = date(2026, 5, 1)
    return pd.DataFrame([{
        "fecha": fecha,
        "grano_raw": codigo,
        "codigo_interno": codigo,
        "campana": campana,
        "sector": sector,
        "toneladas": toneladas,
    }])


# ---------------------------------------------------------------------------
# 1. Mapeo de nombres de grano
# ---------------------------------------------------------------------------

class TestMapeoGrano(unittest.TestCase):

    def test_soja_mapea_a_sbs(self):
        self.assertEqual(_mapear_grano("SOJA"), "SBS")

    def test_maiz_con_acento(self):
        self.assertEqual(_mapear_grano("MAÍZ"), "MAIZE")

    def test_maiz_sin_acento(self):
        self.assertEqual(_mapear_grano("MAIZ"), "MAIZE")

    def test_trigo(self):
        self.assertEqual(_mapear_grano("TRIGO"), "WHEAT")

    def test_trigo_pan(self):
        self.assertEqual(_mapear_grano("TRIGO PAN"), "WHEAT")

    def test_cebada(self):
        self.assertEqual(_mapear_grano("CEBADA"), "BARLEY")

    def test_girasol(self):
        self.assertEqual(_mapear_grano("GIRASOL"), "SFSEED")

    def test_sorgo(self):
        self.assertEqual(_mapear_grano("SORGO"), "SORGHUM")

    def test_harina_soja(self):
        self.assertEqual(_mapear_grano("HARINA DE SOJA"), "SBM")

    def test_desconocido_devuelve_none(self):
        self.assertIsNone(_mapear_grano("ALPISTE"))

    def test_none_devuelve_none(self):
        self.assertIsNone(_mapear_grano(None))

    def test_minusculas_funcionan(self):
        # El mapeo debe tolerar mayúsculas/minúsculas
        self.assertEqual(_mapear_grano("soja"), "SBS")

    def test_todos_los_granos_del_mapa(self):
        for nombre in GRANO_COMPRAS_A_CODIGO:
            with self.subTest(nombre=nombre):
                self.assertIsNotNone(_mapear_grano(nombre))


# ---------------------------------------------------------------------------
# 2. compras_acumuladas_campana
# ---------------------------------------------------------------------------

class TestComprasAcumuladas(unittest.TestCase):

    def setUp(self):
        self.df = pd.concat([
            _make_compras("SBS",   500_000, campana="2025/26", sector="EXPORTACION"),
            _make_compras("SBS",   200_000, campana="2025/26", sector="INDUSTRIA"),
            _make_compras("MAIZE", 300_000, campana="2025/26", sector="EXPORTACION"),
            _make_compras("SBS",   100_000, campana="2024/25", sector="EXPORTACION"),
        ], ignore_index=True)

    def test_suma_campana_correcta(self):
        self.assertEqual(
            compras_acumuladas_campana(self.df, "SBS", "2025/26"),
            700_000,  # 500k expo + 200k industria
        )

    def test_filtra_por_sector(self):
        self.assertEqual(
            compras_acumuladas_campana(self.df, "SBS", "2025/26",
                                       sector="EXPORTACION"),
            500_000,
        )

    def test_no_mezcla_campanas(self):
        self.assertEqual(
            compras_acumuladas_campana(self.df, "SBS", "2025/26",
                                       sector="EXPORTACION"),
            500_000,
        )
        self.assertEqual(
            compras_acumuladas_campana(self.df, "SBS", "2024/25",
                                       sector="EXPORTACION"),
            100_000,
        )

    def test_producto_inexistente_devuelve_cero(self):
        self.assertEqual(
            compras_acumuladas_campana(self.df, "WHEAT", "2025/26"), 0.0
        )

    def test_df_vacio_devuelve_cero(self):
        self.assertEqual(
            compras_acumuladas_campana(pd.DataFrame(), "SBS", "2025/26"), 0.0
        )


# ---------------------------------------------------------------------------
# 3. posicion_exportadora — casos principales
# ---------------------------------------------------------------------------

class TestPosicionExportadora(unittest.TestCase):

    FECHA_REF = date(2026, 6, 1)

    def test_expo_corta(self):
        """Declaró 500k, compró 200k → falta_comprar = 300k, ratio < 1."""
        djve = _make_djve("MAIZE", 500_000, fecha_ref=self.FECHA_REF)
        # MAIZE en jun-2026 → campaña 2026/27 (arranque 1-mar-2026)
        compras = _make_compras("MAIZE", 200_000, campana="2026/27")
        lineup = _make_lineup("MAIZE", 150_000, fecha_ref=self.FECHA_REF)

        pos = posicion_exportadora(djve, compras, lineup,
                                   fecha_ref=self.FECHA_REF)
        self.assertEqual(len(pos), 1)
        row = pos.iloc[0]
        self.assertEqual(row["codigo_interno"], "MAIZE")
        self.assertAlmostEqual(row["declarado_tn"], 500_000)
        self.assertAlmostEqual(row["comprado_tn"], 200_000)
        self.assertAlmostEqual(row["falta_comprar_tn"], 300_000)
        self.assertAlmostEqual(row["embarcado_tn"], 150_000)
        self.assertAlmostEqual(row["falta_embarcar_tn"], 350_000)
        self.assertAlmostEqual(row["ratio_compra"], 0.4)
        self.assertAlmostEqual(row["ratio_embarque"], 0.3)

    def test_expo_cubierta(self):
        """Declaró 300k, compró 300k → ratio_compra = 1.0."""
        djve = _make_djve("SBS", 300_000, fecha_ref=self.FECHA_REF)
        # SBS en jun-2026 → campaña 2026/27 (arranque 1-abr-2026)
        compras = _make_compras("SBS", 300_000, campana="2026/27")
        lineup = _make_lineup("SBS", 300_000, fecha_ref=self.FECHA_REF)

        pos = posicion_exportadora(djve, compras, lineup,
                                   fecha_ref=self.FECHA_REF)
        row = pos.iloc[0]
        self.assertAlmostEqual(row["ratio_compra"], 1.0)
        self.assertAlmostEqual(row["falta_comprar_tn"], 0.0)

    def test_expo_sobre_comprada(self):
        """Compró 600k vs 400k declaradas → ratio > 1, falta_comprar negativo."""
        djve = _make_djve("WHEAT", 400_000, fecha_ref=self.FECHA_REF)
        # WHEAT en jun-2026 → campaña 2025/26 (arranque 1-dic-2025)
        compras = _make_compras("WHEAT", 600_000, campana="2025/26")
        lineup = pd.DataFrame()

        pos = posicion_exportadora(djve, compras, lineup,
                                   fecha_ref=self.FECHA_REF)
        row = pos.iloc[0]
        self.assertAlmostEqual(row["ratio_compra"], 1.5)
        self.assertTrue(row["falta_comprar_tn"] < 0)

    def test_sin_compras_ratio_nan(self):
        """df_compras vacío → comprado_tn = None, ratio_compra = NaN."""
        djve = _make_djve("MAIZE", 400_000, fecha_ref=self.FECHA_REF)
        pos = posicion_exportadora(djve, pd.DataFrame(), pd.DataFrame(),
                                   fecha_ref=self.FECHA_REF)
        row = pos.iloc[0]
        self.assertIsNone(row["comprado_tn"])
        self.assertTrue(math.isnan(row["ratio_compra"]))

    def test_sin_lineup_embarcado_cero(self):
        """df_lineup vacío → embarcado_tn = 0."""
        djve = _make_djve("SBS", 300_000, fecha_ref=self.FECHA_REF)
        compras = _make_compras("SBS", 150_000, campana="2025/26")
        pos = posicion_exportadora(djve, compras, pd.DataFrame(),
                                   fecha_ref=self.FECHA_REF)
        self.assertAlmostEqual(pos.iloc[0]["embarcado_tn"], 0.0)

    def test_djve_vacio_devuelve_vacio(self):
        pos = posicion_exportadora(
            pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
            fecha_ref=self.FECHA_REF,
        )
        self.assertTrue(pos.empty)

    def test_columnas_completas(self):
        djve = _make_djve("MAIZE", 100_000, fecha_ref=self.FECHA_REF)
        pos = posicion_exportadora(djve, pd.DataFrame(), pd.DataFrame(),
                                   fecha_ref=self.FECHA_REF)
        esperadas = {
            "codigo_interno", "producto_display", "declarado_tn",
            "comprado_tn", "embarcado_tn", "falta_comprar_tn",
            "falta_embarcar_tn", "ratio_compra", "ratio_embarque",
            "campana_actual",
        }
        self.assertTrue(esperadas.issubset(set(pos.columns)))

    def test_djve_fuera_de_horizonte_se_excluye(self):
        """DJVE cuya ventana de embarque empieza en 90 días no cae en horizonte 60d."""
        djve = _make_djve("MAIZE", 500_000, dias_inicio=91, dias_fin=110,
                          fecha_ref=self.FECHA_REF)
        pos = posicion_exportadora(djve, pd.DataFrame(), pd.DataFrame(),
                                   fecha_ref=self.FECHA_REF, horizonte_dias=60)
        self.assertTrue(pos.empty)

    def test_campana_actual_se_asigna(self):
        djve = _make_djve("SBS", 200_000, fecha_ref=self.FECHA_REF)
        pos = posicion_exportadora(djve, pd.DataFrame(), pd.DataFrame(),
                                   fecha_ref=self.FECHA_REF)
        self.assertIsNotNone(pos.iloc[0]["campana_actual"])
        # Soja: campaña abr-mar. Jun-2026 es posterior al 1-abr-2026 → campaña 2026/27
        self.assertEqual(pos.iloc[0]["campana_actual"], "2026/27")


# ---------------------------------------------------------------------------
# 4. senales_presion
# ---------------------------------------------------------------------------

class TestSenalesPresion(unittest.TestCase):

    FECHA_REF = date(2026, 6, 1)

    def _posicion(self, codigo: str, declarado: float, comprado: float,
                  embarcado: float = 0.0) -> pd.DataFrame:
        """Helper: construye un DataFrame de posición directamente."""
        import config
        ratio_c = comprado / declarado if declarado > 0 else float("nan")
        ratio_e = embarcado / declarado if declarado > 0 else float("nan")
        return pd.DataFrame([{
            "codigo_interno": codigo,
            "producto_display": config.PRODUCTO_DISPLAY.get(codigo, codigo),
            "declarado_tn": declarado,
            "comprado_tn": comprado,
            "embarcado_tn": embarcado,
            "falta_comprar_tn": declarado - comprado,
            "falta_embarcar_tn": declarado - embarcado,
            "ratio_compra": ratio_c,
            "ratio_embarque": ratio_e,
            "campana_actual": "2025/26",
        }])

    def test_emite_presion_compradora(self):
        pos = self._posicion("MAIZE", 500_000, 200_000)
        senales = senales_presion(pos)
        self.assertEqual(len(senales), 1)
        self.assertEqual(senales.iloc[0]["senal"], "PRESION COMPRADORA")

    def test_emite_presion_vendedora(self):
        pos = self._posicion("SBS", 400_000, 600_000)
        senales = senales_presion(pos)
        self.assertEqual(len(senales), 1)
        self.assertEqual(senales.iloc[0]["senal"], "PRESION VENDEDORA")

    def test_neutral_en_posicion_equilibrada(self):
        pos = self._posicion("WHEAT", 300_000, 300_000)
        senales = senales_presion(pos)
        self.assertTrue(senales.empty)  # NEUTRAL no aparece en la tabla

    def test_intensidad_crece_con_faltante(self):
        """Más Panamax faltantes → mayor intensidad."""
        pos_chico = self._posicion("MAIZE", 200_000, 50_000)   # ~2 Panamax
        pos_grande = self._posicion("MAIZE", 1_000_000, 100_000)  # ~14 Panamax → cap 5

        s_chico = senales_presion(pos_chico)
        s_grande = senales_presion(pos_grande)

        int_chico = s_chico.iloc[0]["intensidad"]
        int_grande = s_grande.iloc[0]["intensidad"]

        self.assertGreater(int_grande, int_chico)
        self.assertLessEqual(int_grande, 5)  # cap en 5

    def test_sin_datos_compras_emite_corto_embarque(self):
        """Sin dato de compras pero ratio_embarque bajo → señal proxy."""
        import config
        pos = pd.DataFrame([{
            "codigo_interno": "SBS",
            "producto_display": config.PRODUCTO_DISPLAY.get("SBS", "SBS"),
            "declarado_tn": 400_000,
            "comprado_tn": None,
            "embarcado_tn": 100_000,
            "falta_comprar_tn": None,
            "falta_embarcar_tn": 300_000,
            "ratio_compra": float("nan"),
            "ratio_embarque": 0.25,
            "campana_actual": "2025/26",
        }])
        senales = senales_presion(pos)
        self.assertEqual(len(senales), 1)
        self.assertEqual(senales.iloc[0]["senal"], "CORTO_EMBARQUE")

    def test_posicion_vacia_devuelve_vacio(self):
        senales = senales_presion(pd.DataFrame(columns=[
            "codigo_interno", "producto_display", "declarado_tn",
            "comprado_tn", "embarcado_tn", "falta_comprar_tn",
            "falta_embarcar_tn", "ratio_compra", "ratio_embarque", "campana_actual",
        ]))
        self.assertTrue(senales.empty)

    def test_declarado_pequeño_no_emite_senal(self):
        """Un declarado testimonial (<5k tn) no genera señal aunque el ratio sea bajo."""
        pos = self._posicion("BARLEY", 1_000, 0)
        senales = senales_presion(pos)
        self.assertTrue(senales.empty)

    def test_columnas_resultado(self):
        pos = self._posicion("MAIZE", 500_000, 100_000)
        senales = senales_presion(pos)
        for col in ("codigo_interno", "producto_display", "senal",
                    "intensidad", "racional"):
            self.assertIn(col, senales.columns)


if __name__ == "__main__":
    unittest.main()
