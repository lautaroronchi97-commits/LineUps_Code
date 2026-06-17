"""Tests del módulo puerto.py (congestión y sequía de buques por zona)."""
import unittest
from datetime import date, timedelta

import pandas as pd

import puerto


REF = date(2025, 6, 16)


def _serie(zona: str, fecha: date, buques: int) -> dict:
    return {"fecha": fecha, "zona": zona, "buques": buques}


def _vessel(zona: str, vessel: str, etb_offset: int, cargo: str = "MAIZE",
            quantity: float = 50_000) -> dict:
    return {
        "zona": zona,
        "vessel": vessel,
        "etb": REF + timedelta(days=etb_offset),
        "cargo": cargo,
        "quantity": quantity,
    }


# ---------------------------------------------------------------------------
# Q6 — congestion_por_zona
# ---------------------------------------------------------------------------

class TestCongestionPorZona(unittest.TestCase):

    def _serie_constante(self, zona: str, valor: int, dias: int = 90) -> list[dict]:
        return [_serie(zona, REF - timedelta(days=k), valor) for k in range(dias)]

    def test_devuelve_una_fila_por_zona_operativa(self):
        df = puerto.congestion_por_zona(pd.DataFrame(), REF)
        self.assertEqual(list(df["zona"]), puerto.ZONAS_OPERATIVAS)

    def test_serie_vacia_da_sin_historia(self):
        df = puerto.congestion_por_zona(pd.DataFrame(), REF)
        self.assertTrue((df["estado"] == "SIN HISTORIA").all())
        self.assertTrue((df["buques_hoy"] == 0).all())

    def test_pico_hoy_marca_sobrepoblado(self):
        # 89 días con 2 buques, hoy con 20 → percentil alto.
        registros = [_serie("Up River Norte", REF - timedelta(days=k), 2)
                     for k in range(1, 90)]
        registros.append(_serie("Up River Norte", REF, 20))
        df = puerto.congestion_por_zona(pd.DataFrame(registros), REF)
        fila = df[df["zona"] == "Up River Norte"].iloc[0]
        self.assertEqual(fila["buques_hoy"], 20)
        self.assertEqual(fila["estado"], "SOBREPOBLADO")
        self.assertGreaterEqual(fila["percentil"], puerto.PCTL_SOBREPOBLADO)

    def test_minimo_hoy_marca_vacio(self):
        registros = [_serie("Up River Sur", REF - timedelta(days=k), 15)
                     for k in range(1, 90)]
        registros.append(_serie("Up River Sur", REF, 0))
        df = puerto.congestion_por_zona(pd.DataFrame(registros), REF)
        fila = df[df["zona"] == "Up River Sur"].iloc[0]
        self.assertEqual(fila["buques_hoy"], 0)
        self.assertEqual(fila["estado"], "VACIO")
        self.assertLessEqual(fila["percentil"], puerto.PCTL_VACIO)

    def test_valor_medio_marca_normal(self):
        # Distribución uniforme 1..20, hoy = 10 → ~percentil 50.
        registros = [_serie("Bahia Blanca", REF - timedelta(days=k), (k % 20) + 1)
                     for k in range(1, 90)]
        registros.append(_serie("Bahia Blanca", REF, 10))
        df = puerto.congestion_por_zona(pd.DataFrame(registros), REF)
        fila = df[df["zona"] == "Bahia Blanca"].iloc[0]
        self.assertEqual(fila["estado"], "NORMAL")

    def test_pocos_dias_da_sin_historia(self):
        # Menos de MIN_DIAS_HISTORIA días → sin percentil.
        registros = self._serie_constante("Up River Norte", 5, dias=5)
        df = puerto.congestion_por_zona(pd.DataFrame(registros), REF)
        fila = df[df["zona"] == "Up River Norte"].iloc[0]
        self.assertEqual(fila["estado"], "SIN HISTORIA")

    def test_ignora_dias_fuera_de_ventana(self):
        # Datos viejos (más de 90 días) no deben contar.
        viejos = [_serie("Up River Norte", REF - timedelta(days=200 + k), 100)
                  for k in range(20)]
        recientes = [_serie("Up River Norte", REF - timedelta(days=k), 3)
                     for k in range(0, 30)]
        df = puerto.congestion_por_zona(pd.DataFrame(viejos + recientes), REF)
        fila = df[df["zona"] == "Up River Norte"].iloc[0]
        # max histórico debe reflejar solo los recientes (3), no los 100 viejos.
        self.assertEqual(fila["max_hist"], 3)


# ---------------------------------------------------------------------------
# Q5 — sequia_buques_por_zona
# ---------------------------------------------------------------------------

class TestSequiaBuquesPorZona(unittest.TestCase):

    def test_devuelve_una_fila_por_zona_operativa(self):
        df = puerto.sequia_buques_por_zona(pd.DataFrame(), REF)
        self.assertEqual(list(df["zona"]), puerto.ZONAS_OPERATIVAS)

    def test_lineup_vacio_todas_sin_barcos(self):
        df = puerto.sequia_buques_por_zona(pd.DataFrame(), REF)
        self.assertTrue(df["sin_barcos"].all())
        self.assertTrue((df["n_buques"] == 0).all())

    def test_cuenta_buques_en_ventana(self):
        rows = [
            _vessel("Up River Norte", "V1", 1),
            _vessel("Up River Norte", "V2", 5),
            _vessel("Up River Norte", "V3", 20),  # fuera de la ventana de 7d
        ]
        df = puerto.sequia_buques_por_zona(pd.DataFrame(rows), REF, horizonte_dias=7)
        fila = df[df["zona"] == "Up River Norte"].iloc[0]
        self.assertEqual(fila["n_buques"], 2)
        self.assertFalse(fila["sin_barcos"])

    def test_zona_sin_buques_proximos(self):
        rows = [_vessel("Up River Norte", "V1", 2)]
        df = puerto.sequia_buques_por_zona(pd.DataFrame(rows), REF)
        fila_sur = df[df["zona"] == "Up River Sur"].iloc[0]
        self.assertTrue(fila_sur["sin_barcos"])
        self.assertEqual(fila_sur["n_buques"], 0)

    def test_dedup_vessel_en_conteo(self):
        # Mismo buque en dos filas (split de destino) cuenta una vez.
        rows = [
            _vessel("Up River Norte", "V1", 1, quantity=30_000),
            _vessel("Up River Norte", "V1", 1, quantity=20_000),
        ]
        df = puerto.sequia_buques_por_zona(pd.DataFrame(rows), REF)
        fila = df[df["zona"] == "Up River Norte"].iloc[0]
        self.assertEqual(fila["n_buques"], 1)
        self.assertEqual(fila["tons"], 50_000)

    def test_productos_listados(self):
        rows = [
            _vessel("Up River Sur", "V1", 1, cargo="MAIZE"),
            _vessel("Up River Sur", "V2", 2, cargo="WHEAT"),
        ]
        df = puerto.sequia_buques_por_zona(pd.DataFrame(rows), REF)
        fila = df[df["zona"] == "Up River Sur"].iloc[0]
        self.assertEqual(fila["productos"], ["MAIZE", "WHEAT"])

    def test_excluye_etb_pasado(self):
        rows = [_vessel("Up River Norte", "V1", -3)]  # ETB ya pasó
        df = puerto.sequia_buques_por_zona(pd.DataFrame(rows), REF)
        fila = df[df["zona"] == "Up River Norte"].iloc[0]
        self.assertTrue(fila["sin_barcos"])


class TestEmojiEstado(unittest.TestCase):
    def test_estados_conocidos(self):
        self.assertEqual(puerto.emoji_estado("SOBREPOBLADO"), "🔴")
        self.assertEqual(puerto.emoji_estado("VACIO"), "⚪")
        self.assertEqual(puerto.emoji_estado("NORMAL"), "🟢")

    def test_estado_desconocido(self):
        self.assertEqual(puerto.emoji_estado("XYZ"), "·")


if __name__ == "__main__":
    unittest.main()
