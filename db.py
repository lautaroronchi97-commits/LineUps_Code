"""
Capa de acceso a Supabase.

Encapsula TODA la interaccion con la base:
- Cliente autenticado via .env.
- Upsert masivo con deduplicacion automatica.
- Queries paginadas (Supabase devuelve max 1000 filas por request).
- Consulta de que fechas ya fueron cargadas (para resume del backfill).

El resto del proyecto no deberia importar `supabase` directamente; debe usar
las funciones expuestas aca.
"""

from __future__ import annotations

import os
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
from dotenv import load_dotenv
from supabase import Client, create_client

from config import (
    FETCH_PAGE_SIZE,
    TABLA_LINEUP,
    UPSERT_BATCH_SIZE,
    UPSERT_CONFLICT_COLUMNS,
)
from utils import setup_logging

logger = setup_logging(__name__)


# ---------------------------------------------------------------------------
# Cliente
# ---------------------------------------------------------------------------

# Cacheamos el cliente como variable de modulo: crear un cliente es barato
# pero innecesario repetirlo. Si en el futuro queres forzar reconexion,
# simplemente reiniciar el script.
_client_cache: Client | None = None


def get_client() -> Client:
    """
    Devuelve un cliente Supabase autenticado con las credenciales de .env.

    Busca SUPABASE_URL y SUPABASE_SERVICE_ROLE_KEY. Si falta alguna, levanta
    RuntimeError con un mensaje claro explicando como resolverlo.
    """
    global _client_cache
    if _client_cache is not None:
        return _client_cache

    # Cargamos .env desde la raiz del proyecto (mismo directorio que este archivo).
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)
    else:
        # En GitHub Actions o Streamlit Cloud no hay .env: las vars vienen del entorno.
        logger.info("No se encontro .env local; uso variables de entorno del sistema.")

    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    if not url or not key:
        raise RuntimeError(
            "Faltan credenciales de Supabase.\n"
            f"  Esperaba SUPABASE_URL y SUPABASE_SERVICE_ROLE_KEY en {env_path} "
            "o en variables de entorno.\n"
            "  Copia .env.example a .env y completalo con los valores de "
            "tu proyecto Supabase (Settings > API)."
        )

    _client_cache = create_client(url, key)
    return _client_cache


# ---------------------------------------------------------------------------
# Upsert
# ---------------------------------------------------------------------------

def upsert_lineup(filas: list[dict[str, Any]], batch_size: int = UPSERT_BATCH_SIZE) -> int:
    """
    Inserta (o actualiza si ya existen) filas en la tabla `lineup`.

    Usa la UNIQUE CONSTRAINT (fecha_consulta, port, berth, vessel, cargo,
    quantity, eta) para detectar duplicados: si corres el backfill dos veces
    para el mismo dia, no se duplican filas.

    Divide en lotes de `batch_size` para evitar timeouts del API.
    Devuelve la cantidad total de filas upserted.
    """
    if not filas:
        return 0

    client = get_client()
    total = 0

    for inicio in range(0, len(filas), batch_size):
        lote = filas[inicio:inicio + batch_size]
        try:
            resp = (
                client.table(TABLA_LINEUP)
                .upsert(lote, on_conflict=UPSERT_CONFLICT_COLUMNS)
                .execute()
            )
        except Exception as exc:
            logger.error(
                "Upsert fallo en el lote %d-%d (%d filas): %s",
                inicio, inicio + len(lote), len(lote), exc,
            )
            raise

        total += len(resp.data) if resp.data else len(lote)
        logger.info("Upsert OK: lote de %d filas (acumulado %d/%d).",
                    len(lote), total, len(filas))

    return total


# ---------------------------------------------------------------------------
# Lectura (con paginacion)
# ---------------------------------------------------------------------------

def _fetch_all(query_builder) -> list[dict[str, Any]]:
    """
    Consume una query de supabase-py paginando hasta traer todas las filas.

    supabase-py devuelve como maximo FETCH_PAGE_SIZE filas por llamada. Esta
    funcion ejecuta la misma query con `.range(start, end)` incrementando
    hasta que no vuelven mas filas.

    El `query_builder` debe ser el builder ANTES de llamar .execute(). Ojo:
    la instancia se reusa, entonces no hay que haberle llamado .range() antes.
    """
    filas: list[dict[str, Any]] = []
    inicio = 0
    while True:
        fin = inicio + FETCH_PAGE_SIZE - 1
        resp = query_builder.range(inicio, fin).execute()
        batch = resp.data or []
        filas.extend(batch)
        if len(batch) < FETCH_PAGE_SIZE:
            break
        inicio += FETCH_PAGE_SIZE
    return filas


def get_fechas_ya_cargadas() -> set[date]:
    """
    Devuelve el set de `fecha_consulta` que ya tienen al menos 1 fila en la DB.

    Lo usa backfill.py para saltar fechas procesadas y permitir reanudar.

    Implementacion: trae la columna `fecha_consulta` paginada y la deduplica
    en Python. Con ~6 anos de data diaria son pocos miles de filas distintas,
    asi que traerla en memoria es trivial.
    """
    client = get_client()
    query = client.table(TABLA_LINEUP).select("fecha_consulta")
    filas = _fetch_all(query)

    # Parseamos a date y dedup-amos.
    fechas: set[date] = set()
    for fila in filas:
        valor = fila.get("fecha_consulta")
        if isinstance(valor, str):
            fechas.add(datetime.strptime(valor, "%Y-%m-%d").date())
        elif isinstance(valor, date):
            fechas.add(valor)
    logger.info("Fechas ya cargadas en DB: %d distintas.", len(fechas))
    return fechas


# ---------------------------------------------------------------------------
# Query helper para el dashboard
# ---------------------------------------------------------------------------

def query_lineup(
    fecha_desde: date | None = None,
    fecha_hasta: date | None = None,
    ports: Iterable[str] | None = None,
    cats: Iterable[str] | None = None,
    cargos: Iterable[str] | None = None,
    shippers: Iterable[str] | None = None,
    solo_agro: bool = False,
) -> pd.DataFrame:
    """
    Consulta la tabla `lineup` con filtros opcionales y devuelve un DataFrame.

    Todos los filtros son opcionales y se combinan con AND. Si ninguno pasa,
    trae TODA la tabla (cuidado con la memoria si la base ya esta grande).

    Ejemplo:
        df = query_lineup(
            fecha_desde=date(2024, 1, 1),
            fecha_hasta=date(2024, 12, 31),
            cargos=["MAIZE", "WHEAT"],
            solo_agro=True,
        )
    """
    client = get_client()
    query = client.table(TABLA_LINEUP).select("*")

    if fecha_desde is not None:
        query = query.gte("fecha_consulta", fecha_desde.isoformat())
    if fecha_hasta is not None:
        query = query.lte("fecha_consulta", fecha_hasta.isoformat())
    if ports:
        query = query.in_("port", list(ports))
    if cats:
        query = query.in_("cat", list(cats))
    if cargos:
        query = query.in_("cargo", list(cargos))
    if shippers:
        query = query.in_("shipper", list(shippers))
    if solo_agro:
        query = query.eq("es_agro", True)

    # Ordenamos por fecha_consulta DESC por default (mas util en el dashboard).
    query = query.order("fecha_consulta", desc=True)

    filas = _fetch_all(query)
    df = pd.DataFrame(filas)

    # Convertir columnas de fecha a datetime para que pandas/plotly las traten bien.
    if not df.empty:
        for col in ("fecha_consulta", "eta", "etb", "ets"):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    return df


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

def ping() -> dict[str, Any]:
    """
    Verifica que la conexion a Supabase funcione y la tabla este accesible.

    Devuelve un dict con {conectado: bool, cantidad_filas: int, error: str}.
    Usado por test_end_to_end.py.
    """
    try:
        client = get_client()
        resp = client.table(TABLA_LINEUP).select("id", count="exact").limit(1).execute()
        return {"conectado": True, "cantidad_filas": resp.count or 0, "error": None}
    except Exception as exc:  # noqa: BLE001 - queremos capturar cualquier cosa
        return {"conectado": False, "cantidad_filas": 0, "error": str(exc)}


if __name__ == "__main__":
    # Uso: python db.py  -> prueba de conexion
    print("Probando conexion a Supabase...")
    resultado = ping()
    if resultado["conectado"]:
        print(f"OK. Conectado. La tabla '{TABLA_LINEUP}' tiene {resultado['cantidad_filas']} filas.")
    else:
        print(f"ERROR de conexion: {resultado['error']}")
