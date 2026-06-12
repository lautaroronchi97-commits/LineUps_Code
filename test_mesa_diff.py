"""
test_mesa_diff.py — Tests del diff diario "qué cambió desde ayer".

Sin red, sin DB.
"""
import unittest
from datetime import date

import pandas as pd

from mesa_diff import (
    buques_nuevos,
    cambios_estado,
    construir_diff,
    djve_nuevas,
)


class TestCambiosEstado(unittest.TestCase):
    def test_cambio_direccion(self):
        hoy = {"MAIZE": {"direccion": "ESTABLE", "banda": "FIRME",
                          "calor": 65, "gap_tn": 100_000}}
        ayer = {"MAIZE": {"direccion": "ABRIENDOSE", "banda": "FIRME",
                          "calor": 65, "gap_tn": 90_000}}
        evs = cambios_estado(hoy, ayer)
        dirs = [e for e in evs if e["tipo"] == "DIR"]
        self.assertEqual(len(dirs), 1)
        self.assertEqual(dirs[0]["desde"], "ABRIENDOSE")
        self.assertEqual(dirs[0]["hasta"], "ESTABLE")

    def test_cambio_banda(self):
        hoy = {"TRIGO": {"direccion": "ESTABLE", "banda": "PESADO",
                         "calor": 36, "gap_tn": 0}}
        ayer = {"TRIGO": {"direccion": "ESTABLE", "banda": "NEUTRO",
                          "calor": 41, "gap_tn": 0}}
        evs = cambios_estado(hoy, ayer)
        bandas = [e for e in evs if e["tipo"] == "BANDA"]
        self.assertEqual(len(bandas), 1)
        self.assertEqual(bandas[0]["hasta"], "PESADO")

    def test_movimiento_gap(self):
        hoy = {"SBS": {"direccion": "ESTABLE", "banda": "FIRME",
                       "calor": 61, "gap_tn": 364_000}}
        ayer = {"SBS": {"direccion": "ESTABLE", "banda": "FIRME",
                        "calor": 61, "gap_tn": 310_000}}
        evs = cambios_estado(hoy, ayer)
        gaps = [e for e in evs if e["tipo"] == "GAP"]
        self.assertEqual(len(gaps), 1)
        self.assertAlmostEqual(gaps[0]["delta"], 54_000, places=0)

    def test_sin_cambios_sin_eventos(self):
        estado = {"MAIZE": {"direccion": "ESTABLE", "banda": "FIRME",
                            "calor": 65, "gap_tn": 100_000}}
        evs = cambios_estado(estado, estado)
        self.assertEqual(evs, [])

    def test_producto_nuevo_sin_ayer_se_ignora(self):
        hoy = {"MAIZE": {"direccion": "ABRIENDOSE", "banda": "CALIENTE",
                         "calor": 85, "gap_tn": 200_000}}
        evs = cambios_estado(hoy, {})
        self.assertEqual(evs, [])

    def test_gap_pequeno_no_emite(self):
        hoy = {"MAIZE": {"direccion": "ESTABLE", "banda": "FIRME",
                         "calor": 65, "gap_tn": 105_000}}
        ayer = {"MAIZE": {"direccion": "ESTABLE", "banda": "FIRME",
                          "calor": 65, "gap_tn": 100_000}}
        evs = cambios_estado(hoy, ayer)
        self.assertEqual([e for e in evs if e["tipo"] == "GAP"], [])


class TestBuquesNuevos(unittest.TestCase):
    def _vessel(self, vessel, cargo, tn):
        return {"vessel": vessel, "cargo": cargo, "quantity": tn,
                "shipper_canon": "COFCO", "port": "TIMBUES", "etb": date(2026, 6, 10)}

    def test_buque_nuevo_relevante(self):
        hoy = pd.DataFrame([self._vessel("MV A", "MAIZE", 66_000)])
        ayer = pd.DataFrame([self._vessel("MV B", "MAIZE", 50_000)])
        evs = buques_nuevos(hoy, ayer)
        self.assertEqual(len(evs), 1)
        self.assertEqual(evs[0]["vessel"], "MV A")

    def test_buque_existente_no_emite(self):
        v = self._vessel("MV A", "MAIZE", 66_000)
        evs = buques_nuevos(pd.DataFrame([v]), pd.DataFrame([v]))
        self.assertEqual(evs, [])

    def test_buque_pequeno_no_emite(self):
        hoy = pd.DataFrame([self._vessel("MV A", "MAIZE", 20_000)])
        evs = buques_nuevos(hoy, pd.DataFrame())
        self.assertEqual(evs, [])

    def test_lineup_hoy_vacio(self):
        self.assertEqual(buques_nuevos(pd.DataFrame(), pd.DataFrame()), [])


class TestDjveNuevas(unittest.TestCase):
    def test_djve_nueva_relevante(self):
        djve = pd.DataFrame([{
            "shipper_canon": "LDC", "codigo_interno": "WHEAT",
            "toneladas": 28_000, "fecha_registro": date(2026, 6, 11),
        }])
        evs = djve_nuevas(djve, date(2026, 6, 11))
        self.assertEqual(len(evs), 1)
        self.assertEqual(evs[0]["shipper"], "LDC")

    def test_djve_vieja_no_emite(self):
        djve = pd.DataFrame([{
            "shipper_canon": "LDC", "codigo_interno": "WHEAT",
            "toneladas": 28_000, "fecha_registro": date(2026, 6, 1),
        }])
        evs = djve_nuevas(djve, date(2026, 6, 11))
        self.assertEqual(evs, [])

    def test_djve_pequena_no_emite(self):
        djve = pd.DataFrame([{
            "shipper_canon": "LDC", "codigo_interno": "WHEAT",
            "toneladas": 5_000, "fecha_registro": date(2026, 6, 11),
        }])
        evs = djve_nuevas(djve, date(2026, 6, 11))
        self.assertEqual(evs, [])

    def test_djve_vacio(self):
        self.assertEqual(djve_nuevas(pd.DataFrame(), date(2026, 6, 11)), [])


class TestConstruirDiff(unittest.TestCase):
    def test_ordena_direccion_primero(self):
        hoy = {"MAIZE": {"direccion": "ESTABLE", "banda": "FIRME",
                         "calor": 65, "gap_tn": 100_000}}
        ayer = {"MAIZE": {"direccion": "ABRIENDOSE", "banda": "NEUTRO",
                          "calor": 55, "gap_tn": 100_000}}
        lineup_hoy = pd.DataFrame([{
            "vessel": "MV A", "cargo": "MAIZE", "quantity": 66_000,
            "shipper_canon": "COFCO", "port": "TIMBUES", "etb": date(2026, 6, 10),
        }])
        evs = construir_diff(
            hoy, ayer, lineup_hoy, pd.DataFrame(), pd.DataFrame(),
            date(2026, 6, 11),
        )
        # Primero DIR, luego BANDA, luego BUQUE.
        self.assertEqual(evs[0]["tipo"], "DIR")
        tipos = [e["tipo"] for e in evs]
        self.assertLess(tipos.index("DIR"), tipos.index("BUQUE"))

    def test_max_eventos_recorta(self):
        hoy = {f"P{i}": {"direccion": "ESTABLE", "banda": "PESADO",
                         "calor": 30, "gap_tn": 0} for i in range(10)}
        ayer = {f"P{i}": {"direccion": "ABRIENDOSE", "banda": "CALIENTE",
                          "calor": 85, "gap_tn": 0} for i in range(10)}
        evs = construir_diff(hoy, ayer, pd.DataFrame(), pd.DataFrame(),
                             pd.DataFrame(), date(2026, 6, 11), max_eventos=3)
        self.assertEqual(len(evs), 3)

    def test_sin_cambios_lista_vacia(self):
        estado = {"MAIZE": {"direccion": "ESTABLE", "banda": "FIRME",
                            "calor": 65, "gap_tn": 100_000}}
        evs = construir_diff(estado, estado, pd.DataFrame(), pd.DataFrame(),
                             pd.DataFrame(), date(2026, 6, 11))
        self.assertEqual(evs, [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
