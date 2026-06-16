"""
Carga manual de comercializacion de granos (farmer-selling) a Supabase.

Por que existe
--------------
El MAGyP bloquea con 403 las IPs de datacenter (GitHub Actions, Supabase Edge
Functions), asi que el cron automatico no puede bajar el dato. La solucion:
vos bajas el archivo desde tu navegador en Argentina (sin bloqueo) y corres
este script para subirlo a la DB. Tarda ~30 segundos una vez por semana.

Uso
---
    python cargar_compras.py ARCHIVO

    ARCHIVO: ruta al CSV o XLSX descargado del MAGyP
             (planilla "Comercializacion de Granos" / SIO-Granos).

Donde bajar el archivo
----------------------
    https://datos.magyp.gob.ar/dataset/compras-de-granos
    → Descargar el recurso CSV o XLSX mas reciente.

El script es idempotente: correrlo dos veces con el mismo archivo no duplica
datos (la tabla tiene UNIQUE constraint por campana + codigo_interno + sector + fecha).
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

import compras_fas
from update_compras import _df_a_filas
from db import upsert_compras
from utils import setup_logging

logger = setup_logging(__name__)


def _leer_bytes(content: bytes, suffix: str) -> pd.DataFrame:
    """
    Parsea el contenido binario de un archivo MAGyP (CSV o Excel) y devuelve
    un DataFrame normalizado identico al de `compras_fas.descargar_compras()`.

    Args:
        content: bytes del archivo (de Path.read_bytes() o UploadedFile.getvalue()).
        suffix:  extension sin punto, en minusculas ("csv", "xlsx", "xls").

    Raises:
        ValueError: si el formato no es reconocido o las columnas no coinciden.
    """
    import io as _io
    buf = _io.BytesIO(content)

    if suffix in ("xlsx", "xls"):
        try:
            raw = pd.read_excel(buf, dtype=str)
        except Exception as exc:
            raise ValueError(f"No se pudo leer el archivo Excel: {exc}") from exc
    elif suffix == "csv":
        try:
            raw = pd.read_csv(buf, encoding="utf-8", dtype=str, on_bad_lines="skip")
        except UnicodeDecodeError:
            buf.seek(0)
            raw = pd.read_csv(buf, encoding="latin-1", dtype=str, on_bad_lines="skip")
        except Exception as exc:
            raise ValueError(f"No se pudo leer el CSV: {exc}") from exc
    else:
        raise ValueError(
            f"Formato no soportado: '{suffix}'. Usar .csv, .xlsx o .xls."
        )

    if raw.empty:
        raise ValueError("El archivo está vacío.")

    # Normalizar nombres de columna igual que descargar_compras().
    raw.columns = [c.lower().strip().replace(" ", "_") for c in raw.columns]

    # Detectar columnas clave con tolerancia a variantes de nombre.
    col_map: dict[str, str] = {}
    for col in raw.columns:
        if any(k in col for k in ("grano", "cultivo", "producto")):
            col_map.setdefault("grano_raw", col)
        elif "sector" in col:
            col_map.setdefault("sector", col)
        elif any(k in col for k in ("campa", "zafra")):
            col_map.setdefault("campana", col)
        elif any(k in col for k in ("semana", "fecha", "periodo")):
            col_map.setdefault("fecha", col)
        elif col in ("tn", "toneladas", "comprado_tn", "total_tn"):
            col_map.setdefault("toneladas", col)

    if not {"grano_raw", "toneladas"}.issubset(col_map):
        raise ValueError(
            "El archivo no tiene las columnas esperadas.\n"
            f"  Columnas encontradas: {list(raw.columns)}\n"
            "  Se necesita al menos: columna de grano y columna de toneladas.\n"
            "  Verificar que sea la planilla 'Comercializacion de Granos' del MAGyP."
        )

    df = raw.rename(columns={v: k for k, v in col_map.items()})

    # Normalizar tipos.
    df["toneladas"] = pd.to_numeric(df["toneladas"], errors="coerce").fillna(0)
    df["grano_raw"] = df["grano_raw"].astype(str).str.strip().str.upper()

    if "fecha" in df.columns:
        df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce").dt.date
    else:
        df["fecha"] = None

    if "sector" in df.columns:
        df["sector"] = df["sector"].astype(str).str.strip().str.upper()
    else:
        df["sector"] = compras_fas.SECTOR_EXPORTACION

    if "campana" not in df.columns:
        df["campana"] = None

    # Mapear a codigo_interno usando la misma tabla que el modulo automatico.
    df["codigo_interno"] = df["grano_raw"].apply(compras_fas._mapear_grano)

    # Columnas opcionales si existen en el archivo.
    for col_opt in ("toneladas_a_fijar", "precio_promedio_usd", "porcentaje_cosecha"):
        candidates = [c for c in df.columns if col_opt.split("_")[0] in c]
        if candidates and col_opt not in df.columns:
            df[col_opt] = pd.to_numeric(df[candidates[0]], errors="coerce")

    return df


def _leer_archivo(path: Path) -> pd.DataFrame:
    """Wrapper sobre `_leer_bytes` para leer desde una ruta local."""
    suffix = path.suffix.lstrip(".").lower()
    return _leer_bytes(path.read_bytes(), suffix)


def resumen_granos_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Devuelve un DataFrame con el resumen de granos encontrados en el archivo.
    Columnas: grano_raw, codigo_interno, toneladas_kt, mapeado.
    Usado por el dashboard para mostrar el preview antes de confirmar.
    """
    if "grano_raw" not in df.columns or df.empty:
        return pd.DataFrame(columns=["Grano", "Codigo", "Toneladas (kt)", "Estado"])
    grp = (
        df.groupby(["grano_raw", "codigo_interno"], dropna=False)["toneladas"]
        .sum()
        .reset_index()
    )
    grp["Estado"] = grp["codigo_interno"].apply(
        lambda c: f"✅ {c}" if pd.notna(c) else "⚠️ sin mapeo (se omite)"
    )
    grp["Toneladas (kt)"] = (grp["toneladas"] / 1000).round(1)
    return grp.rename(columns={"grano_raw": "Grano", "codigo_interno": "Codigo"})[
        ["Grano", "Codigo", "Toneladas (kt)", "Estado"]
    ]


def cargar(ruta: str) -> tuple[int, int]:
    """
    Lee el archivo local, normaliza y sube a Supabase.

    Returns:
        (filas_leidas, filas_upserted)
    """
    path = Path(ruta)
    if not path.exists():
        raise FileNotFoundError(f"No se encontro el archivo: {path}")

    logger.info("Leyendo %s ...", path)
    df = _leer_archivo(path)
    leidas = len(df)
    logger.info("Filas leidas: %d", leidas)

    if df.empty:
        logger.warning("El archivo no tenia filas utiles.")
        return 0, 0

    filas = _df_a_filas(df)
    if not filas:
        _resumen_granos(df)
        logger.warning(
            "Ninguna fila paso la normalizacion (falta codigo_interno / sector / fecha). "
            "Verificar que el archivo sea la planilla correcta del MAGyP."
        )
        return leidas, 0

    _resumen_granos(df)
    logger.info("Upsertando %d filas en Supabase...", len(filas))
    upsert_compras(filas)
    return leidas, len(filas)


def _resumen_granos(df: pd.DataFrame) -> None:
    """Imprime un resumen de los granos encontrados en el archivo."""
    if "grano_raw" not in df.columns:
        return
    conteo = df.groupby(["grano_raw", "codigo_interno"])["toneladas"].sum()
    print("\nGranos encontrados en el archivo:")
    for (grano, codigo), tn in conteo.items():
        estado = f"→ {codigo}" if codigo else "  (sin mapeo, se omite)"
        print(f"  {grano:<30} {estado}   {tn/1e3:>10.1f} kt")
    print()


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__)
        print("ERROR: falta el argumento ARCHIVO.")
        print("  Uso: python cargar_compras.py ruta/al/archivo.csv")
        return 2

    ruta = sys.argv[1]
    try:
        leidas, upserted = cargar(ruta)
    except (FileNotFoundError, ValueError) as exc:
        print(f"ERROR: {exc}")
        return 1
    except Exception as exc:
        logger.error("Fallo inesperado: %s", exc, exc_info=True)
        return 1

    print(f"Listo. {leidas} filas leidas, {upserted} filas upserted en Supabase.")
    if upserted == 0 and leidas > 0:
        print("  Advertencia: ninguna fila fue subida. Revisar el resumen de granos arriba.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
