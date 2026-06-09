"""
Tests unitarios puros para los módulos core del proyecto.

Sin red, sin Supabase, sin Streamlit.
Dependencias: unittest (stdlib) + pandas + openpyxl.

Módulos cubiertos:
  - fob_djve:    _producto_a_codigo, _parsear_xlsx, djve_diarias,
                 djve_por_producto_recientes
  - shipper_norm: canonicalizar_shipper, aplicar_a_dataframe
  - utils:       parse_fecha_corta, parse_quantity, ajustar_anio_por_rollover,
                 es_agro
"""
from __future__ import annotations

import io
import sys
import os
import unittest
from datetime import date

import pandas as pd
import openpyxl

# ---------------------------------------------------------------------------
# Asegurar que el directorio del proyecto esté en sys.path para los imports.
# ---------------------------------------------------------------------------
_WORKTREE = os.path.dirname(os.path.abspath(__file__))
if _WORKTREE not in sys.path:
    sys.path.insert(0, _WORKTREE)

import fob_djve
import shipper_norm
import utils


# ===========================================================================
# Helpers de construcción de XLSX en memoria
# ===========================================================================

def _xlsx_con_columnas(columnas: list[str], filas: list[list]) -> bytes:
    """
    Crea un XLSX en memoria con las columnas y filas indicadas.
    Devuelve los bytes listos para pasar a _parsear_xlsx().
    """
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(columnas)
    for fila in filas:
        ws.append(fila)
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def _xlsx_esquema_completo(filas: list[list] | None = None) -> bytes:
    """
    XLSX con exactamente las columnas del _ESQUEMA_COLUMNAS de fob_djve.
    Si no se pasan filas, el archivo tiene solo la cabecera.
    """
    columnas = list(fob_djve._ESQUEMA_COLUMNAS.keys())
    return _xlsx_con_columnas(columnas, filas or [])


# ===========================================================================
# Tests de fob_djve._producto_a_codigo
# ===========================================================================

class TestProductoACodigo(unittest.TestCase):

    def test_soja(self):
        self.assertEqual(fob_djve._producto_a_codigo("SOJA"), "SBS")

    def test_soja_minusculas(self):
        """La función debe normalizar a mayúsculas."""
        self.assertEqual(fob_djve._producto_a_codigo("soja"), "SBS")

    def test_maiz(self):
        self.assertEqual(fob_djve._producto_a_codigo("MAIZ"), "MAIZE")

    def test_harina_de_soja(self):
        self.assertEqual(fob_djve._producto_a_codigo("HARINA DE SOJA"), "SBM")

    def test_pellets_de_soja(self):
        self.assertEqual(fob_djve._producto_a_codigo("PELLETS DE SOJA"), "SBM")

    def test_aceite_de_soja(self):
        self.assertEqual(fob_djve._producto_a_codigo("ACEITE DE SOJA"), "SBO")

    def test_trigo(self):
        self.assertEqual(fob_djve._producto_a_codigo("TRIGO"), "WHEAT")

    def test_cebada(self):
        self.assertEqual(fob_djve._producto_a_codigo("CEBADA"), "BARLEY")

    def test_girasol(self):
        self.assertEqual(fob_djve._producto_a_codigo("GIRASOL"), "SFSEED")

    def test_desconocido(self):
        self.assertIsNone(fob_djve._producto_a_codigo("PRODUCTO INEXISTENTE XYZ"))

    def test_none(self):
        self.assertIsNone(fob_djve._producto_a_codigo(None))

    def test_cadena_vacia(self):
        self.assertIsNone(fob_djve._producto_a_codigo(""))

    def test_espacios_solo(self):
        """Una cadena con solo espacios también debe devolver None."""
        self.assertIsNone(fob_djve._producto_a_codigo("   "))

    def test_match_por_substring(self):
        """
        'SOJA VARIANTE ESPECIAL' no está en el mapa exacto, pero contiene
        'SOJA', así que el match por substring debe devolver 'SBS'.
        """
        resultado = fob_djve._producto_a_codigo("SOJA VARIANTE ESPECIAL")
        self.assertEqual(resultado, "SBS")

    def test_sorgo(self):
        self.assertEqual(fob_djve._producto_a_codigo("SORGO"), "SORGHUM")

    def test_poroto_de_soja(self):
        self.assertEqual(fob_djve._producto_a_codigo("POROTO DE SOJA"), "SBS")


# ===========================================================================
# Tests de fob_djve._parsear_xlsx
# ===========================================================================

class TestParsearXlsx(unittest.TestCase):

    def _columnas_reales(self):
        return list(fob_djve._ESQUEMA_COLUMNAS.keys())

    def test_xlsx_esquema_completo_devuelve_dataframe(self):
        """Un XLSX con todas las columnas del esquema no debe devolver vacío."""
        xlsx = _xlsx_esquema_completo()
        df = fob_djve._parsear_xlsx(xlsx)
        self.assertIsInstance(df, pd.DataFrame)

    def test_columnas_normalizadas(self):
        """Las columnas deben estar renombradas a snake_case."""
        xlsx = _xlsx_esquema_completo()
        df = fob_djve._parsear_xlsx(xlsx)
        columnas_esperadas = set(fob_djve._ESQUEMA_COLUMNAS.values())
        self.assertTrue(
            columnas_esperadas.issubset(set(df.columns)),
            f"Faltan columnas: {columnas_esperadas - set(df.columns)}",
        )

    def test_columna_codigo_interno_presente(self):
        """La columna 'codigo_interno' debe añadirse siempre."""
        xlsx = _xlsx_esquema_completo()
        df = fob_djve._parsear_xlsx(xlsx)
        self.assertIn("codigo_interno", df.columns)

    def test_parsea_filas_correctamente(self):
        """Verifica que las filas se parsean con los valores correctos."""
        cols = self._columnas_reales()
        fila = [
            "1001",                   # Nº DJVE SIM
            "2024-03-15",             # FECHA DE REGISTRO
            "2024-03-15",             # FECHA DE PRESENTACIÓN
            "SOJA",                   # PRODUCTO
            50000,                    # TN
            "2024-04-01",             # FECHA  DE INICIO PER.
            "2024-05-31",             # FECHA DE FIN PER.
            "A",                      # OPCION
            "VITERRA SA",             # RAZON SOCIAL
        ]
        xlsx = _xlsx_con_columnas(cols, [fila])
        df = fob_djve._parsear_xlsx(xlsx)

        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]["producto"], "SOJA")
        self.assertEqual(df.iloc[0]["toneladas"], 50000)
        self.assertEqual(df.iloc[0]["razon_social"], "VITERRA SA")
        self.assertEqual(df.iloc[0]["codigo_interno"], "SBS")

    def test_toneladas_no_numericas_se_convierten_a_cero(self):
        """Valores no numéricos en TN deben quedar como 0."""
        cols = self._columnas_reales()
        fila = [
            "1002", "2024-03-15", "2024-03-15", "MAIZ",
            "N/A", "2024-04-01", "2024-05-31", "A", "CARGILL",
        ]
        xlsx = _xlsx_con_columnas(cols, [fila])
        df = fob_djve._parsear_xlsx(xlsx)
        self.assertEqual(df.iloc[0]["toneladas"], 0)

    def test_xlsx_con_columnas_faltantes_devuelve_vacio(self):
        """Si faltan columnas críticas, el resultado debe ser DataFrame vacío."""
        xlsx = _xlsx_con_columnas(["COL_A", "COL_B"], [["x", "y"]])
        df = fob_djve._parsear_xlsx(xlsx)
        self.assertTrue(df.empty)

    def test_xlsx_con_doctype_rechazado(self):
        """Bytes con '<!DOCTYPE' deben devolver DataFrame vacío (defensa XXE)."""
        contenido_malicioso = b"<!DOCTYPE foo [<!ENTITY bar SYSTEM 'file:///etc/passwd'>]>"
        df = fob_djve._parsear_xlsx(contenido_malicioso)
        self.assertTrue(df.empty)

    def test_xlsx_corrupto_devuelve_vacio(self):
        """Bytes aleatorios (no es XLSX válido) deben devolver DataFrame vacío."""
        df = fob_djve._parsear_xlsx(b"\x00\x01\x02\x03 not an xlsx")
        self.assertTrue(df.empty)

    def test_producto_normalizado_a_mayusculas(self):
        """La columna 'producto' debe estar en mayúsculas y sin espacios extra."""
        cols = self._columnas_reales()
        fila = [
            "1003", "2024-03-15", "2024-03-15", "  soja  ",
            1000, "2024-04-01", "2024-05-31", "A", "EMPRESA X",
        ]
        xlsx = _xlsx_con_columnas(cols, [fila])
        df = fob_djve._parsear_xlsx(xlsx)
        self.assertEqual(df.iloc[0]["producto"], "SOJA")


# ===========================================================================
# Tests de fob_djve.djve_diarias
# ===========================================================================

class TestDjveDiarias(unittest.TestCase):

    def _df_base(self):
        """DataFrame mínimo válido para las funciones de agregación."""
        return pd.DataFrame({
            "fecha_registro": [
                date(2024, 3, 1), date(2024, 3, 1), date(2024, 3, 2),
                date(2024, 3, 2), date(2024, 3, 3),
            ],
            "codigo_interno": ["SBS", "MAIZE", "SBS", "SBM", "MAIZE"],
            "toneladas": [10000, 5000, 20000, 8000, 3000],
            "nro_djve": ["A1", "A2", "A3", "A4", "A5"],
            "razon_social": ["EMP1", "EMP2", "EMP1", "EMP3", "EMP2"],
        })

    def test_df_vacio_devuelve_vacio(self):
        df = fob_djve.djve_diarias(pd.DataFrame())
        self.assertTrue(df.empty)

    def test_columnas_resultado(self):
        """El resultado debe tener columnas 'fecha_registro' y 'toneladas'."""
        df = fob_djve.djve_diarias(self._df_base())
        self.assertIn("fecha_registro", df.columns)
        self.assertIn("toneladas", df.columns)

    def test_shape_sin_filtro(self):
        """Sin filtro de producto, debe haber una fila por fecha distinta."""
        df = fob_djve.djve_diarias(self._df_base())
        # 3 fechas únicas en los datos de prueba
        self.assertEqual(len(df), 3)

    def test_suma_por_fecha(self):
        """Las toneladas deben sumarse por fecha."""
        df = fob_djve.djve_diarias(self._df_base())
        df_sorted = df.sort_values("fecha_registro").reset_index(drop=True)
        # 2024-03-01: 10000 + 5000 = 15000
        self.assertEqual(df_sorted.iloc[0]["toneladas"], 15000)

    def test_filtro_por_codigo_interno(self):
        """Con filtro, solo aparecen las filas del código especificado."""
        df = fob_djve.djve_diarias(self._df_base(), codigo_interno="SBS")
        self.assertTrue((df["toneladas"] > 0).all())
        # Solo hay SBS en fechas 2024-03-01 y 2024-03-02
        self.assertEqual(len(df), 2)

    def test_filtro_codigo_inexistente_devuelve_vacio(self):
        df = fob_djve.djve_diarias(self._df_base(), codigo_interno="CODIGO_FALSO")
        self.assertTrue(df.empty)

    def test_ordenado_por_fecha(self):
        """Los resultados deben estar ordenados cronológicamente."""
        df = fob_djve.djve_diarias(self._df_base())
        fechas = list(df["fecha_registro"])
        self.assertEqual(fechas, sorted(fechas))


# ===========================================================================
# Tests de fob_djve.djve_por_producto_recientes
# ===========================================================================

class TestDjvePorProductoRecientes(unittest.TestCase):

    def _df_base(self, fecha_base: date | None = None):
        if fecha_base is None:
            fecha_base = date(2024, 3, 15)
        return pd.DataFrame({
            "fecha_registro": [
                fecha_base, fecha_base,
                date(fecha_base.year, fecha_base.month, fecha_base.day),
            ],
            "codigo_interno": ["SBS", "SBS", "MAIZE"],
            "toneladas": [10000, 5000, 8000],
            "nro_djve": ["A1", "A2", "A3"],
            "razon_social": ["VITERRA", "CARGILL", "ADM"],
        })

    def test_df_vacio_devuelve_vacio(self):
        df = fob_djve.djve_por_producto_recientes(pd.DataFrame())
        self.assertTrue(df.empty)

    def test_columnas_resultado(self):
        """Debe tener las columnas esperadas."""
        df = fob_djve.djve_por_producto_recientes(
            self._df_base(), dias=30, hasta=date(2024, 3, 15)
        )
        for col in ("codigo_interno", "toneladas", "n_djve", "razon_social_top"):
            self.assertIn(col, df.columns, f"Falta columna: {col}")

    def test_toneladas_correctas(self):
        """Las toneladas de SBS deben ser 15000 (10000+5000)."""
        df = fob_djve.djve_por_producto_recientes(
            self._df_base(), dias=30, hasta=date(2024, 3, 15)
        )
        row_sbs = df[df["codigo_interno"] == "SBS"]
        self.assertEqual(len(row_sbs), 1)
        self.assertEqual(row_sbs.iloc[0]["toneladas"], 15000)

    def test_n_djve(self):
        """El conteo de declaraciones debe ser correcto."""
        df = fob_djve.djve_por_producto_recientes(
            self._df_base(), dias=30, hasta=date(2024, 3, 15)
        )
        row_sbs = df[df["codigo_interno"] == "SBS"]
        self.assertEqual(row_sbs.iloc[0]["n_djve"], 2)

    def test_razon_social_top(self):
        """El exportador top de SBS es VITERRA (10000 > 5000)."""
        df = fob_djve.djve_por_producto_recientes(
            self._df_base(), dias=30, hasta=date(2024, 3, 15)
        )
        row_sbs = df[df["codigo_interno"] == "SBS"]
        self.assertEqual(row_sbs.iloc[0]["razon_social_top"], "VITERRA")

    def test_filtra_fuera_del_rango(self):
        """Registros fuera del rango no deben aparecer."""
        df_test = pd.DataFrame({
            "fecha_registro": [date(2024, 1, 1), date(2024, 3, 15)],
            "codigo_interno": ["SBS", "SBS"],
            "toneladas": [99999, 1000],
            "nro_djve": ["OLD", "NEW"],
            "razon_social": ["VIEJA", "NUEVA"],
        })
        df = fob_djve.djve_por_producto_recientes(
            df_test, dias=30, hasta=date(2024, 3, 15)
        )
        row_sbs = df[df["codigo_interno"] == "SBS"]
        # Solo debe contar la fila del 15-mar (dentro del rango)
        self.assertEqual(row_sbs.iloc[0]["toneladas"], 1000)

    def test_excluye_codigo_interno_nulo(self):
        """Filas con codigo_interno=None deben ignorarse."""
        df_test = pd.DataFrame({
            "fecha_registro": [date(2024, 3, 15), date(2024, 3, 15)],
            "codigo_interno": [None, "MAIZE"],
            "toneladas": [50000, 3000],
            "nro_djve": ["X1", "X2"],
            "razon_social": ["SIN_MAP", "ADM"],
        })
        df = fob_djve.djve_por_producto_recientes(
            df_test, dias=30, hasta=date(2024, 3, 15)
        )
        # No debe aparecer ninguna fila con codigo_interno == None
        self.assertFalse(df["codigo_interno"].isna().any())
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]["codigo_interno"], "MAIZE")


# ===========================================================================
# Tests de shipper_norm.canonicalizar_shipper
# ===========================================================================

class TestCanonicalizarShipper(unittest.TestCase):

    # --- Viterra / Bunge / OMHSA ---
    def test_viterra_sa(self):
        self.assertEqual(shipper_norm.canonicalizar_shipper("VITERRA SA"), ("VITERRA-BUNGE", None))

    def test_bunge_argentina(self):
        self.assertEqual(shipper_norm.canonicalizar_shipper("BUNGE ARGENTINA"), ("VITERRA-BUNGE", None))

    def test_omhsa(self):
        self.assertEqual(shipper_norm.canonicalizar_shipper("OMHSA"), ("VITERRA-BUNGE", None))

    def test_oleaginosa_moreno(self):
        self.assertEqual(
            shipper_norm.canonicalizar_shipper("OLEAGINOSA MORENO HNOS S.A."),
            ("VITERRA-BUNGE", None),
        )

    # --- Cargill ---
    def test_cargill_saci(self):
        self.assertEqual(shipper_norm.canonicalizar_shipper("CARGILL SACI"), ("CARGILL", None))

    def test_cargill_sin_sufijo(self):
        self.assertEqual(shipper_norm.canonicalizar_shipper("CARGILL"), ("CARGILL", None))

    # --- COFCO / Nidera ---
    def test_cofco(self):
        self.assertEqual(
            shipper_norm.canonicalizar_shipper("COFCO INTERNATIONAL ARGENTINA S.A."),
            ("COFCO", None),
        )

    def test_nidera(self):
        self.assertEqual(shipper_norm.canonicalizar_shipper("NIDERA S.A."), ("COFCO", None))

    # --- LDC ---
    def test_ldc_argentina(self):
        self.assertEqual(shipper_norm.canonicalizar_shipper("LDC ARGENTINA S.A."), ("LDC", None))

    def test_louis_dreyfus(self):
        self.assertEqual(
            shipper_norm.canonicalizar_shipper("LOUIS DREYFUS COMMODITIES"),
            ("LDC", None),
        )

    # --- ADM ---
    def test_adm_agro(self):
        self.assertEqual(shipper_norm.canonicalizar_shipper("ADM AGRO S.A."), ("ADM", None))

    def test_archer_daniels(self):
        self.assertEqual(
            shipper_norm.canonicalizar_shipper("ARCHER DANIELS MIDLAND"),
            ("ADM", None),
        )

    # --- AGD ---
    def test_agd(self):
        self.assertEqual(shipper_norm.canonicalizar_shipper("AGD S.A."), ("AGD", None))

    def test_aceitera_general_deheza(self):
        self.assertEqual(
            shipper_norm.canonicalizar_shipper("ACEITERA GENERAL DEHEZA S.A."),
            ("AGD", None),
        )

    # --- ACA ---
    def test_aca(self):
        self.assertEqual(
            shipper_norm.canonicalizar_shipper("ACA - ASOC COOPERATIVAS ARGENTINAS"),
            ("ACA", None),
        )

    # --- Molinos ---
    def test_molinos_agro(self):
        self.assertEqual(shipper_norm.canonicalizar_shipper("MOLINOS AGRO"), ("MOLINOS", None))

    def test_molinos_rio_de_la_plata(self):
        self.assertEqual(
            shipper_norm.canonicalizar_shipper("MOLINOS RIO DE LA PLATA"),
            ("MOLINOS", None),
        )

    # --- Quilmes / Maltería ---
    def test_malteria_quilmes(self):
        self.assertEqual(
            shipper_norm.canonicalizar_shipper("CERVECERIA Y MALTERIA QUILMES"),
            ("QUILMES", None),
        )

    # --- Empresa desconocida ---
    def test_empresa_desconocida(self):
        canon, origen = shipper_norm.canonicalizar_shipper("EMPRESA CHICA XYZ S.A.")
        self.assertEqual(canon, "OTROS")
        self.assertIsNone(origen)

    # --- Entradas inválidas ---
    def test_none(self):
        self.assertEqual(shipper_norm.canonicalizar_shipper(None), ("OTROS", None))

    def test_cadena_vacia(self):
        self.assertEqual(shipper_norm.canonicalizar_shipper(""), ("OTROS", None))

    def test_solo_espacios(self):
        self.assertEqual(shipper_norm.canonicalizar_shipper("   "), ("OTROS", None))


# ===========================================================================
# Tests de flag origen_alt (PY / UY)
# ===========================================================================

class TestOrigenAlt(unittest.TestCase):

    def test_ldc_py(self):
        """'LDC PY' debe mapearse a LDC con origen PY."""
        self.assertEqual(shipper_norm.canonicalizar_shipper("LDC PY"), ("LDC", "PY"))

    def test_bunge_py(self):
        self.assertEqual(
            shipper_norm.canonicalizar_shipper("BUNGE PY"),
            ("VITERRA-BUNGE", "PY"),
        )

    def test_cargill_uy(self):
        self.assertEqual(shipper_norm.canonicalizar_shipper("CARGILL UY"), ("CARGILL", "UY"))

    def test_cofco_uy(self):
        self.assertEqual(shipper_norm.canonicalizar_shipper("COFCO UY"), ("COFCO", "UY"))

    def test_adm_py(self):
        self.assertEqual(shipper_norm.canonicalizar_shipper("ADM PY"), ("ADM", "PY"))

    def test_palabra_paraguay_completa(self):
        """La palabra PARAGUAY completa también activa el flag."""
        canon, origen = shipper_norm.canonicalizar_shipper("LDC PARAGUAY S.A.")
        self.assertEqual(canon, "LDC")
        self.assertEqual(origen, "PY")

    def test_palabra_uruguay_completa(self):
        canon, origen = shipper_norm.canonicalizar_shipper("CARGILL URUGUAY S.A.")
        self.assertEqual(canon, "CARGILL")
        self.assertEqual(origen, "UY")

    def test_otros_con_py(self):
        """Un shipper desconocido con PY debe ser OTROS pero con origen PY."""
        canon, origen = shipper_norm.canonicalizar_shipper("EMPRESA RARA PY S.A.")
        self.assertEqual(canon, "OTROS")
        self.assertEqual(origen, "PY")


# ===========================================================================
# Tests de shipper_norm.aplicar_a_dataframe
# ===========================================================================

class TestAplicarADataframe(unittest.TestCase):

    def _df_base(self):
        return pd.DataFrame({
            "shipper": [
                "VITERRA SA",
                "CARGILL S.A.C.I.",
                "LDC PY",
                "EMPRESA CHICA",
                None,
            ]
        })

    def test_agrega_columna_shipper_canon(self):
        df = shipper_norm.aplicar_a_dataframe(self._df_base())
        self.assertIn("shipper_canon", df.columns)

    def test_agrega_columna_origen_alt(self):
        df = shipper_norm.aplicar_a_dataframe(self._df_base())
        self.assertIn("origen_alt", df.columns)

    def test_valores_shipper_canon(self):
        df = shipper_norm.aplicar_a_dataframe(self._df_base())
        self.assertEqual(df.iloc[0]["shipper_canon"], "VITERRA-BUNGE")
        self.assertEqual(df.iloc[1]["shipper_canon"], "CARGILL")
        self.assertEqual(df.iloc[2]["shipper_canon"], "LDC")
        self.assertEqual(df.iloc[3]["shipper_canon"], "OTROS")
        self.assertEqual(df.iloc[4]["shipper_canon"], "OTROS")

    def test_valores_origen_alt(self):
        df = shipper_norm.aplicar_a_dataframe(self._df_base())
        # LDC PY debe tener origen_alt = "PY"
        self.assertEqual(df.iloc[2]["origen_alt"], "PY")
        # El resto sin PY/UY debe ser None o NaN (pandas almacena None como NaN
        # en columnas de objetos; ambos son valores "sin origen alternativo").
        self.assertTrue(
            pd.isna(df.iloc[0]["origen_alt"]) or df.iloc[0]["origen_alt"] is None
        )
        self.assertTrue(
            pd.isna(df.iloc[1]["origen_alt"]) or df.iloc[1]["origen_alt"] is None
        )

    def test_df_vacio_agrega_columnas(self):
        """Un DataFrame vacío debe recibir las columnas de salida igualmente."""
        df_vacio = pd.DataFrame()
        resultado = shipper_norm.aplicar_a_dataframe(df_vacio)
        self.assertIn("shipper_canon", resultado.columns)
        self.assertIn("origen_alt", resultado.columns)

    def test_columna_entrada_personalizada(self):
        """Se puede especificar una columna de entrada distinta a 'shipper'."""
        df = pd.DataFrame({"exportador": ["CARGILL", "BUNGE ARGENTINA"]})
        resultado = shipper_norm.aplicar_a_dataframe(df, col_in="exportador")
        self.assertIn("shipper_canon", resultado.columns)
        self.assertEqual(resultado.iloc[0]["shipper_canon"], "CARGILL")
        self.assertEqual(resultado.iloc[1]["shipper_canon"], "VITERRA-BUNGE")

    def test_df_sin_columna_shipper_agrega_nulos(self):
        """Si falta la columna de entrada, las columnas de salida deben ser None."""
        df = pd.DataFrame({"otra_col": [1, 2]})
        resultado = shipper_norm.aplicar_a_dataframe(df)
        self.assertTrue(resultado["shipper_canon"].isna().all())
        self.assertTrue(resultado["origen_alt"].isna().all())


# ===========================================================================
# Tests de utils.parse_fecha_corta
# ===========================================================================

class TestParseFechaCorta(unittest.TestCase):

    def test_fecha_valida(self):
        resultado = utils.parse_fecha_corta("14-abr", 2024)
        self.assertEqual(resultado, date(2024, 4, 14))

    def test_dia_uno_digito(self):
        resultado = utils.parse_fecha_corta("5-ene", 2024)
        self.assertEqual(resultado, date(2024, 1, 5))

    def test_separador_barra(self):
        """También debe aceptar '/' como separador."""
        resultado = utils.parse_fecha_corta("14/abr", 2024)
        self.assertEqual(resultado, date(2024, 4, 14))

    def test_minusculas(self):
        resultado = utils.parse_fecha_corta("3-mar", 2025)
        self.assertEqual(resultado, date(2025, 3, 3))

    def test_todos_los_meses(self):
        """Verificar que todos los meses en español están mapeados."""
        from config import MESES_ES
        for abrev, num in MESES_ES.items():
            with self.subTest(mes=abrev):
                resultado = utils.parse_fecha_corta(f"1-{abrev}", 2024)
                self.assertIsNotNone(resultado, f"Falló para mes '{abrev}'")
                self.assertEqual(resultado.month, num)

    def test_none_devuelve_none(self):
        self.assertIsNone(utils.parse_fecha_corta(None, 2024))

    def test_cadena_vacia_devuelve_none(self):
        self.assertIsNone(utils.parse_fecha_corta("", 2024))

    def test_formato_invalido_devuelve_none(self):
        self.assertIsNone(utils.parse_fecha_corta("2024-04-14", 2024))

    def test_mes_inexistente_devuelve_none(self):
        self.assertIsNone(utils.parse_fecha_corta("10-xyz", 2024))

    def test_fecha_invalida_devuelve_none(self):
        """31 de febrero no existe; debe devolver None en lugar de pinchar."""
        self.assertIsNone(utils.parse_fecha_corta("31-feb", 2024))

    def test_solo_guion_devuelve_none(self):
        self.assertIsNone(utils.parse_fecha_corta("-", 2024))


# ===========================================================================
# Tests de utils.parse_quantity
# ===========================================================================

class TestParseQuantity(unittest.TestCase):

    def test_entero_simple(self):
        self.assertEqual(utils.parse_quantity("46000"), 46000)

    def test_separador_coma(self):
        """'46,000' es formato anglosajón con coma como separador de miles."""
        self.assertEqual(utils.parse_quantity("46,000"), 46000)

    def test_separador_punto(self):
        """'46.000' es formato europeo con punto como separador de miles."""
        self.assertEqual(utils.parse_quantity("46.000"), 46000)

    def test_con_espacios(self):
        """Los espacios también se eliminan."""
        self.assertEqual(utils.parse_quantity("46 000"), 46000)

    def test_none_devuelve_none(self):
        self.assertIsNone(utils.parse_quantity(None))

    def test_cadena_vacia_devuelve_none(self):
        self.assertIsNone(utils.parse_quantity(""))

    def test_solo_guion_devuelve_none(self):
        """'-' es un valor de celda vacía en la tabla fuente."""
        self.assertIsNone(utils.parse_quantity("-"))

    def test_texto_no_numerico_devuelve_none(self):
        self.assertIsNone(utils.parse_quantity("N/A"))

    def test_valor_grande(self):
        self.assertEqual(utils.parse_quantity("1.000.000"), 1000000)

    def test_solo_ceros(self):
        self.assertEqual(utils.parse_quantity("0"), 0)


# ===========================================================================
# Tests de utils.ajustar_anio_por_rollover
# ===========================================================================

class TestAjustarAnioPorRollover(unittest.TestCase):

    def test_sin_rollover(self):
        """Fechas cercanas no deben cambiar de año."""
        fecha_c = date(2024, 4, 15)
        fecha_p = date(2024, 4, 18)
        self.assertEqual(utils.ajustar_anio_por_rollover(fecha_p, fecha_c), date(2024, 4, 18))

    def test_rollover_diciembre_a_enero(self):
        """
        Consultando en diciembre, una fecha en enero del mismo año
        debe pasarse al año siguiente.

        fecha_consulta=2024-12-28, fecha_parseada=2024-01-05
        diff_meses = (2024-2024)*12 + (1-12) = -11  -> < -6
        Resultado esperado: 2025-01-05
        """
        fecha_consulta = date(2024, 12, 28)
        fecha_parseada = date(2024, 1, 5)
        resultado = utils.ajustar_anio_por_rollover(fecha_parseada, fecha_consulta)
        self.assertEqual(resultado, date(2025, 1, 5))

    def test_rollover_enero_a_diciembre(self):
        """
        Consultando en enero, una fecha en diciembre del mismo año
        debe pasarse al año anterior.

        fecha_consulta=2025-01-03, fecha_parseada=2025-12-28
        diff_meses = (2025-2025)*12 + (12-1) = 11  -> > 6
        Resultado esperado: 2024-12-28
        """
        fecha_consulta = date(2025, 1, 3)
        fecha_parseada = date(2025, 12, 28)
        resultado = utils.ajustar_anio_por_rollover(fecha_parseada, fecha_consulta)
        self.assertEqual(resultado, date(2024, 12, 28))

    def test_none_devuelve_none(self):
        self.assertIsNone(
            utils.ajustar_anio_por_rollover(None, date(2024, 6, 1))
        )

    def test_diferencia_exacta_de_6_meses_no_ajusta(self):
        """
        La heurística ajusta solo si |diff_meses| > 6.
        Con exactamente 6 meses de diferencia, no debe ajustar.
        """
        fecha_consulta = date(2024, 1, 1)
        fecha_parseada = date(2024, 7, 1)  # diff = +6, no > 6
        resultado = utils.ajustar_anio_por_rollover(fecha_parseada, fecha_consulta)
        self.assertEqual(resultado, date(2024, 7, 1))

    def test_diferencia_mayor_a_6_meses_ajusta(self):
        """diff = +7 -> debe ajustar el año hacia atrás."""
        fecha_consulta = date(2024, 1, 1)
        fecha_parseada = date(2024, 8, 1)  # diff = +7 > 6
        resultado = utils.ajustar_anio_por_rollover(fecha_parseada, fecha_consulta)
        self.assertEqual(resultado, date(2023, 8, 1))


# ===========================================================================
# Tests de utils.es_agro
# ===========================================================================

class TestEsAgro(unittest.TestCase):

    def test_grains_es_agro(self):
        self.assertTrue(utils.es_agro("GRAINS"))

    def test_by_products_es_agro(self):
        self.assertTrue(utils.es_agro("BY PRODUCTS"))

    def test_vegoil_es_agro(self):
        self.assertTrue(utils.es_agro("VEGOIL"))

    def test_fertilizantes_no_es_agro(self):
        """FERTILIZERS no está en AGRO_CATEGORIES."""
        self.assertFalse(utils.es_agro("FERTILIZERS"))

    def test_none_no_es_agro(self):
        self.assertFalse(utils.es_agro(None))

    def test_cadena_vacia_no_es_agro(self):
        self.assertFalse(utils.es_agro(""))

    def test_minusculas_es_agro(self):
        """La comparación debe ser case-insensitive."""
        self.assertTrue(utils.es_agro("grains"))

    def test_con_espacios_es_agro(self):
        """Espacios al inicio/final deben ignorarse."""
        self.assertTrue(utils.es_agro("  VEGOIL  "))

    def test_categoria_desconocida_no_es_agro(self):
        self.assertFalse(utils.es_agro("CHEMICALS"))

    def test_steel_no_es_agro(self):
        self.assertFalse(utils.es_agro("STEEL"))


# ===========================================================================
# Entry point
# ===========================================================================

if __name__ == "__main__":
    unittest.main(verbosity=2)
