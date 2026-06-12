"""
test_fas_comprador.py — Tests unitarios para fas_comprador.py

Sin red, sin DB. Usa DataFrames sintéticos.
"""
import unittest
from datetime import date, timedelta

import pandas as pd

import fas_comprador
from fas_comprador import (
    PANAMAX_TN,
    PRODUCTOS_FAS,
    UMBRAL_AMBAR,
    UMBRAL_ROJO,
    _score_urgencia,
    perfil_historico,
    tabla_urgencia,
    urgencia_por_shipper,
)


# ---------------------------------------------------------------------------
# Helpers para construir DataFrames sintéticos
# ---------------------------------------------------------------------------

REF = date(2026, 6, 1)


def _djve(shipper_canon: str, codigo: str, tn: float,
          inicio_dias: int = 0, fin_dias: int = 30, ref: date = REF) -> dict:
    """Fila sintética de DJVE.

    IMPORTANTE: el parámetro shipper_canon debe ser el valor canónico que
    shipper_norm.canonicalizar_shipper devuelve para "<shipper_canon> SA".
    Por ejemplo: "CARGILL" funciona, "BUNGE" no (→ "VITERRA-BUNGE").
    """
    return {
        "nro_djve": 1,
        "razon_social": shipper_canon + " SA",
        "shipper_canon": shipper_canon,
        "codigo_interno": codigo,
        "producto": codigo,
        "toneladas": tn,
        "fecha_registro": ref,
        "fecha_inicio_embarque": ref + timedelta(days=inicio_dias),
        "fecha_fin_embarque": ref + timedelta(days=fin_dias),
    }


def _vessel(shipper_canon: str, cargo: str, tn: float, etb_dias: int,
            ref: date = REF) -> dict:
    """Fila sintética de line-up."""
    return {
        "vessel": f"MV_{shipper_canon}_{cargo}_{etb_dias}",
        "cargo": cargo,
        "quantity": tn,
        "etb": ref + timedelta(days=etb_dias),
        "shipper_canon": shipper_canon,
        "origen_alt": None,
        "ops": "LOAD",
    }


# ---------------------------------------------------------------------------
# TestScoreUrgencia
# ---------------------------------------------------------------------------

class TestScoreUrgencia(unittest.TestCase):
    def test_cubierto_retorna_cero(self):
        self.assertEqual(_score_urgencia(0, 5, 15), 0.0)
        self.assertEqual(_score_urgencia(-10_000, 5, 15), 0.0)

    def test_etb_cero_factor_maximo(self):
        score = _score_urgencia(PANAMAX_TN, 0, 15)
        # proximidad = max(0, 1 - 0/15) = 1 → factor = 2
        self.assertAlmostEqual(score, 2.0, places=5)

    def test_etb_igual_horizonte_sin_bonus(self):
        score = _score_urgencia(PANAMAX_TN, 15, 15)
        # proximidad = max(0, 1 - 1) = 0 → factor = 1
        self.assertAlmostEqual(score, 1.0, places=5)

    def test_etb_mayor_horizonte_no_negativo(self):
        # Días > horizonte no generan penalización (clip a 0).
        score = _score_urgencia(PANAMAX_TN, 30, 15)
        self.assertAlmostEqual(score, 1.0, places=5)

    def test_etb_cercano_mayor_score_que_lejano(self):
        score_cercano = _score_urgencia(PANAMAX_TN, 2, 15)
        score_lejano = _score_urgencia(PANAMAX_TN, 14, 15)
        self.assertGreater(score_cercano, score_lejano)


# ---------------------------------------------------------------------------
# TestUrgenciaPorShipper
# ---------------------------------------------------------------------------

class TestUrgenciaPorShipper(unittest.TestCase):
    def setUp(self):
        # CARGILL: declaró 200k tn de MAIZE; tiene 65k en line-up → corto 135k.
        # ETB en 5 días → urgencia alta.
        self.djve_cargill = pd.DataFrame([
            _djve("CARGILL", "MAIZE", 200_000, inicio_dias=0, fin_dias=30),
        ])
        self.lineup_cargill = pd.DataFrame([
            _vessel("CARGILL", "MAIZE", 65_000, etb_dias=5),
        ])

    # --- Caso básico: exportador corto ---
    def test_exportador_corto_tiene_score_positivo(self):
        res = urgencia_por_shipper(
            self.djve_cargill, self.lineup_cargill, REF, horizontes=[15]
        )
        df = res[15]
        self.assertFalse(df.empty)
        row = df.iloc[0]
        self.assertEqual(row["shipper_canon"], "CARGILL")
        self.assertGreater(row["urgencia_score"], 0)
        self.assertGreater(row["falta_cubrir_tn"], 0)

    # --- ETB real se refleja ---
    def test_dias_proximo_etb_es_correcto(self):
        res = urgencia_por_shipper(
            self.djve_cargill, self.lineup_cargill, REF, horizontes=[15]
        )
        df = res[15]
        self.assertFalse(df.empty)
        self.assertEqual(df.iloc[0]["dias_proximo_etb"], 5)

    # --- Exportador cubierto → score 0 ---
    def test_exportador_cubierto_score_cero(self):
        # ADM declaró exactamente lo que tiene en line-up → falta_cubrir=0 → score=0.
        djve = pd.DataFrame([_djve("ADM", "MAIZE", 65_000, 0, 30)])
        lineup = pd.DataFrame([_vessel("ADM", "MAIZE", 65_000, 10)])
        res = urgencia_por_shipper(djve, lineup, REF, horizontes=[15])
        df = res[15]
        if not df.empty:
            fila = df[df["shipper_canon"] == "ADM"]
            if not fila.empty:
                self.assertEqual(fila.iloc[0]["urgencia_score"], 0.0)

    # --- ETB cercano → score mayor que ETB lejano ---
    def test_etb_cercano_produce_score_mayor(self):
        # CARGILL ETB 2d vs ADM ETB 20d — mismo faltante, ETB cercano gana.
        # Note: razon_social "ADM SA" → canonicaliza a "ADM" correctamente.
        djve = pd.DataFrame([
            _djve("CARGILL", "MAIZE", 200_000, 0, 30),
            _djve("ADM",     "MAIZE", 200_000, 0, 30),
        ])
        lineup = pd.DataFrame([
            _vessel("CARGILL", "MAIZE", 65_000, etb_dias=2),
            _vessel("ADM",     "MAIZE", 65_000, etb_dias=20),
        ])
        res = urgencia_por_shipper(djve, lineup, REF, horizontes=[30])
        df = res[30]
        self.assertFalse(df.empty)
        r_cargill = df[df["shipper_canon"] == "CARGILL"]["urgencia_score"]
        r_adm     = df[df["shipper_canon"] == "ADM"]["urgencia_score"]
        self.assertFalse(r_cargill.empty)
        self.assertFalse(r_adm.empty)
        self.assertGreater(r_cargill.values[0], r_adm.values[0])

    # --- Producto fuera de PRODUCTOS_FAS no aparece ---
    def test_producto_fuera_fas_no_aparece(self):
        djve = pd.DataFrame([_djve("TRADIG", "BARLEY", 100_000, 0, 30)])
        lineup = pd.DataFrame([_vessel("TRADIG", "BARLEY", 30_000, 5)])
        res = urgencia_por_shipper(djve, lineup, REF, horizontes=[15])
        df = res[15]
        self.assertTrue(df.empty)

    # --- Retorna exactamente las 3 claves ---
    def test_retorna_tres_claves(self):
        res = urgencia_por_shipper(
            self.djve_cargill, self.lineup_cargill, REF
        )
        self.assertIn(7, res)
        self.assertIn(15, res)
        self.assertIn(30, res)

    # --- DataFrames vacíos no lanzan excepción ---
    def test_djve_vacio_sin_excepcion(self):
        res = urgencia_por_shipper(pd.DataFrame(), pd.DataFrame(), REF)
        for h in [7, 15, 30]:
            self.assertIn(h, res)
            self.assertTrue(res[h].empty)

    # --- sin DJVE → no aparece aunque haya line-up ---
    def test_sin_djve_no_aparece(self):
        lineup = pd.DataFrame([_vessel("CARGILL", "MAIZE", 65_000, 5)])
        res = urgencia_por_shipper(pd.DataFrame(), lineup, REF, horizontes=[15])
        self.assertTrue(res[15].empty)

    # --- Columnas correctas ---
    def test_columnas_presentes(self):
        res = urgencia_por_shipper(
            self.djve_cargill, self.lineup_cargill, REF, horizontes=[7]
        )
        df = res[7]
        for col in [
            "shipper_canon", "codigo_interno", "producto_display",
            "declarado_tn", "originado_tn", "falta_cubrir_tn",
            "ratio_cobertura", "n_buques", "dias_proximo_etb", "urgencia_score",
        ]:
            self.assertIn(col, df.columns, f"Columna faltante: {col}")

    # --- n_buques correcto ---
    def test_n_buques_correcto(self):
        lineup = pd.DataFrame([
            _vessel("CARGILL", "MAIZE", 65_000, 5),
            _vessel("CARGILL", "MAIZE", 65_000, 10),
        ])
        djve = pd.DataFrame([_djve("CARGILL", "MAIZE", 500_000, 0, 30)])
        res = urgencia_por_shipper(djve, lineup, REF, horizontes=[15])
        df = res[15]
        self.assertFalse(df.empty)
        # Dos buques distintos → n_buques = 2
        self.assertEqual(df.iloc[0]["n_buques"], 2)

    # --- Ordenado por score DESC ---
    def test_ordenado_por_score_desc(self):
        djve = pd.DataFrame([
            _djve("CARGILL", "MAIZE", 200_000, 0, 30),
            _djve("BUNGE",   "MAIZE", 500_000, 0, 30),
        ])
        lineup = pd.DataFrame([
            _vessel("CARGILL", "MAIZE", 65_000, 5),
            _vessel("BUNGE",   "MAIZE", 65_000, 5),
        ])
        res = urgencia_por_shipper(djve, lineup, REF, horizontes=[15])
        df = res[15]
        scores = df["urgencia_score"].tolist()
        self.assertEqual(scores, sorted(scores, reverse=True))

    # --- declarado_tn == 0 excluido ---
    def test_declarado_cero_excluido(self):
        djve = pd.DataFrame([_djve("CARGILL", "MAIZE", 0, 0, 30)])
        lineup = pd.DataFrame([_vessel("CARGILL", "MAIZE", 65_000, 5)])
        res = urgencia_por_shipper(djve, lineup, REF, horizontes=[15])
        self.assertTrue(res[15].empty)

    # --- Productos FAS aceptados ---
    def test_productos_fas_todos_aceptados(self):
        for codigo in PRODUCTOS_FAS:
            djve = pd.DataFrame([_djve("CARGILL", codigo, 200_000, 0, 30)])
            lineup = pd.DataFrame([_vessel("CARGILL", codigo, 65_000, 5)])
            res = urgencia_por_shipper(djve, lineup, REF, horizontes=[15])
            df = res[15]
            self.assertFalse(df.empty, f"Producto {codigo} debería aparecer")


# ---------------------------------------------------------------------------
# TestPerfilHistorico
# ---------------------------------------------------------------------------

class TestPerfilHistorico(unittest.TestCase):
    """Perfil histórico basado en ventanas semanales de los últimos 90 días."""

    def _djve_hist(self, shipper: str, codigo: str, tn: float,
                   base: date = None) -> list[dict]:
        """Genera DJVE históricas para cada semana de los últimos 90 días."""
        if base is None:
            base = REF
        rows = []
        for i in range(13):  # 13 semanas × 7d = 91d
            semana_ref = base - timedelta(days=i * 7)
            rows.append({
                "nro_djve": i,
                "razon_social": shipper + " SA",
                "shipper_canon": shipper,
                "codigo_interno": codigo,
                "producto": codigo,
                "toneladas": tn,
                "fecha_registro": semana_ref,
                "fecha_inicio_embarque": semana_ref - timedelta(days=7),
                "fecha_fin_embarque": semana_ref + timedelta(days=7),
            })
        return rows

    def test_siempre_corto_label_comprador_habitual(self):
        # CARGILL declaró 200k tn cada semana pero nunca tiene line-up → siempre corto.
        djve_hist = pd.DataFrame(
            self._djve_hist("CARGILL", "MAIZE", 200_000)
        )
        result = perfil_historico(
            pd.DataFrame(),  # sin line-up → originado = 0 siempre
            "CARGILL", "MAIZE",
            djve_hist, REF,
        )
        self.assertEqual(result["label"], "COMPRADOR HABITUAL")
        self.assertGreaterEqual(result["pct_periodos_corto"], 0.60)

    def test_siempre_cubierto_label_cubre_bien(self):
        # BUNGE declaró 65k tn y siempre tiene exactamente 65k en line-up.
        djve_hist = pd.DataFrame(
            self._djve_hist("BUNGE", "MAIZE", 65_000)
        )
        lineup_hist_rows = []
        for i in range(13):
            semana_ref = REF - timedelta(days=i * 7)
            lineup_hist_rows.append(
                _vessel("BUNGE", "MAIZE", 65_000, etb_dias=3, ref=semana_ref)
            )
        lineup_hist = pd.DataFrame(lineup_hist_rows)

        result = perfil_historico(
            lineup_hist, "BUNGE", "MAIZE", djve_hist, REF,
        )
        self.assertIn(result["label"], ("CUBRE BIEN", "CORTO FRECUENTE"))
        self.assertLess(result["pct_periodos_corto"], 0.60)

    def test_dfs_vacios_retorna_sin_historia(self):
        result = perfil_historico(
            pd.DataFrame(), "CARGILL", "MAIZE", pd.DataFrame(), REF
        )
        self.assertEqual(result["label"], "SIN HISTORIA")

    def test_djve_hist_vacio_retorna_sin_historia(self):
        lineup = pd.DataFrame([_vessel("CARGILL", "MAIZE", 65_000, 5)])
        result = perfil_historico(lineup, "CARGILL", "MAIZE", pd.DataFrame(), REF)
        self.assertEqual(result["label"], "SIN HISTORIA")

    def test_retorna_cuatro_claves(self):
        djve_hist = pd.DataFrame(
            self._djve_hist("CARGILL", "MAIZE", 200_000)
        )
        result = perfil_historico(pd.DataFrame(), "CARGILL", "MAIZE", djve_hist, REF)
        for key in ("pct_periodos_corto", "promedio_gap_tn", "max_gap_tn", "label"):
            self.assertIn(key, result)

    def test_pct_entre_cero_y_uno(self):
        djve_hist = pd.DataFrame(
            self._djve_hist("CARGILL", "MAIZE", 200_000)
        )
        result = perfil_historico(pd.DataFrame(), "CARGILL", "MAIZE", djve_hist, REF)
        self.assertGreaterEqual(result["pct_periodos_corto"], 0.0)
        self.assertLessEqual(result["pct_periodos_corto"], 1.0)

    def test_max_gap_mayor_o_igual_promedio(self):
        djve_hist = pd.DataFrame(
            self._djve_hist("CARGILL", "MAIZE", 200_000)
        )
        result = perfil_historico(pd.DataFrame(), "CARGILL", "MAIZE", djve_hist, REF)
        self.assertGreaterEqual(result["max_gap_tn"], result["promedio_gap_tn"])


# ---------------------------------------------------------------------------
# TestTablaUrgencia
# ---------------------------------------------------------------------------

class TestTablaUrgencia(unittest.TestCase):
    def _res_simple(self, shipper: str, etb_dias: int, falta: float,
                    horizonte: int = None) -> pd.DataFrame:
        """DataFrame de urgencia sintético para un shipper."""
        if horizonte is None:
            horizonte = 15
        score = _score_urgencia(falta, etb_dias, horizonte)
        return pd.DataFrame([{
            "shipper_canon": shipper,
            "codigo_interno": "MAIZE",
            "producto_display": "Maíz",
            "declarado_tn": falta + 65_000,
            "originado_tn": 65_000,
            "falta_cubrir_tn": falta,
            "ratio_cobertura": 65_000 / (falta + 65_000),
            "n_buques": 1,
            "dias_proximo_etb": etb_dias,
            "urgencia_score": score,
        }])

    def test_primer_fila_mayor_score(self):
        # CARGILL con ETB=3 (más urgente) vs BUNGE con ETB=20.
        df_7_cargill = self._res_simple("CARGILL", 3, 130_000, 7)
        df_7_bunge   = self._res_simple("BUNGE",  20, 130_000, 7)
        df_7 = pd.concat([df_7_cargill, df_7_bunge], ignore_index=True)

        tabla = tabla_urgencia({7: df_7, 15: pd.DataFrame(), 30: pd.DataFrame()}, {})
        self.assertFalse(tabla.empty)
        self.assertEqual(tabla.iloc[0]["shipper_canon"], "CARGILL")

    def test_etb_3_dias_es_rojo(self):
        df_7 = self._res_simple("CARGILL", 3, 130_000, 7)
        tabla = tabla_urgencia({7: df_7, 15: pd.DataFrame(), 30: pd.DataFrame()}, {})
        self.assertFalse(tabla.empty)
        row = tabla[tabla["shipper_canon"] == "CARGILL"]
        self.assertFalse(row.empty)
        self.assertEqual(row.iloc[0]["señal"], "ROJO")

    def test_etb_10_dias_es_ambar(self):
        df_7 = self._res_simple("CARGILL", 10, 130_000, 7)
        tabla = tabla_urgencia({7: df_7, 15: pd.DataFrame(), 30: pd.DataFrame()}, {})
        row = tabla[tabla["shipper_canon"] == "CARGILL"]
        self.assertFalse(row.empty)
        self.assertEqual(row.iloc[0]["señal"], "AMBAR")

    def test_etb_20_dias_es_verde(self):
        df_7 = self._res_simple("CARGILL", 20, 130_000, 7)
        tabla = tabla_urgencia({7: df_7, 15: pd.DataFrame(), 30: pd.DataFrame()}, {})
        row = tabla[tabla["shipper_canon"] == "CARGILL"]
        self.assertFalse(row.empty)
        self.assertEqual(row.iloc[0]["señal"], "VERDE")

    def test_columnas_completas(self):
        df_7 = self._res_simple("CARGILL", 5, 130_000, 7)
        tabla = tabla_urgencia({7: df_7, 15: pd.DataFrame(), 30: pd.DataFrame()}, {})
        for col in [
            "shipper_canon", "codigo_interno", "producto_display",
            "falta_7d", "falta_15d", "falta_30d",
            "dias_proximo_etb", "urgencia_score_7d", "perfil_label", "señal",
        ]:
            self.assertIn(col, tabla.columns, f"Columna faltante: {col}")

    def test_todos_horizontes_vacios_tabla_vacia_sin_excepcion(self):
        tabla = tabla_urgencia({7: pd.DataFrame(), 15: pd.DataFrame(), 30: pd.DataFrame()}, {})
        self.assertTrue(tabla.empty)

    def test_perfil_label_sin_historia_cuando_perfil_no_existe(self):
        df_7 = self._res_simple("CARGILL", 5, 130_000, 7)
        tabla = tabla_urgencia({7: df_7, 15: pd.DataFrame(), 30: pd.DataFrame()}, {})
        self.assertEqual(tabla.iloc[0]["perfil_label"], "SIN HISTORIA")

    def test_perfil_label_inyectado(self):
        df_7 = self._res_simple("CARGILL", 5, 130_000, 7)
        perfiles = {("CARGILL", "MAIZE"): {"label": "COMPRADOR HABITUAL"}}
        tabla = tabla_urgencia({7: df_7, 15: pd.DataFrame(), 30: pd.DataFrame()}, perfiles)
        self.assertEqual(tabla.iloc[0]["perfil_label"], "COMPRADOR HABITUAL")

    def test_señal_exactamente_en_umbral_rojo(self):
        df_7 = self._res_simple("CARGILL", UMBRAL_ROJO, 130_000, 7)
        tabla = tabla_urgencia({7: df_7}, {})
        self.assertEqual(tabla.iloc[0]["señal"], "ROJO")

    def test_señal_exactamente_en_umbral_ambar(self):
        df_7 = self._res_simple("CARGILL", UMBRAL_AMBAR, 130_000, 7)
        tabla = tabla_urgencia({7: df_7}, {})
        self.assertEqual(tabla.iloc[0]["señal"], "AMBAR")

    def test_ordenado_por_urgencia_score_7d_desc(self):
        df_7 = pd.concat([
            self._res_simple("CARGILL", 3, 200_000, 7),
            self._res_simple("BUNGE",   3, 100_000, 7),
            self._res_simple("ADM",     3,  50_000, 7),
        ], ignore_index=True)
        tabla = tabla_urgencia({7: df_7, 15: pd.DataFrame(), 30: pd.DataFrame()}, {})
        scores = tabla["urgencia_score_7d"].tolist()
        self.assertEqual(scores, sorted(scores, reverse=True))

    def test_falta_15d_nan_cuando_horizonte_vacio(self):
        df_7 = self._res_simple("CARGILL", 5, 130_000, 7)
        tabla = tabla_urgencia({7: df_7, 15: pd.DataFrame(), 30: pd.DataFrame()}, {})
        self.assertTrue(pd.isna(tabla.iloc[0]["falta_15d"]))
        self.assertTrue(pd.isna(tabla.iloc[0]["falta_30d"]))

    def test_falta_15d_presente_cuando_horizonte_con_datos(self):
        df_7  = self._res_simple("CARGILL", 5, 130_000,  7)
        df_15 = self._res_simple("CARGILL", 5, 120_000, 15)
        tabla = tabla_urgencia({7: df_7, 15: df_15, 30: pd.DataFrame()}, {})
        self.assertFalse(pd.isna(tabla.iloc[0]["falta_15d"]))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    unittest.main(verbosity=2)
