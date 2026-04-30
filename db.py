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
    TABLA_DJVE,
    TABLA_LINEUP,
    UPSERT_BATCH_SIZE,
    UPSERT_CONFLICT_COLUMNS,
    UPSERT_CONFLICT_DJVE,
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
    Devuelve un cliente Supabase autenticado con las credenciales del entorno.

    Estrategia de key (primera que este definida gana):
      1. SUPABASE_ANON_KEY  → clave publica de solo-lectura; segura para el
                             dashboard en Streamlit Cloud (RLS bloquea escrituras).
      2. SUPABASE_SERVICE_ROLE_KEY → clave con permisos totales; usada por los
                             scripts de cron (backfill, update_today, update_djve)
                             y por el desarrollo local.

    Esto permite que Streamlit Cloud tenga SOLO la anon_key (read-only) y que
    GitHub Actions tenga SOLO la service_role_key (lectura + escritura), sin
    cambios en los scripts de cron.
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
    # Preferir anon_key (solo-lectura, dashboard) sobre service_role (admin, cron).
    key = os.getenv("SUPABASE_ANON_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    if not url or not key:
        raise RuntimeError(
            "Faltan credenciales de Supabase.\n"
            f"  Esperaba SUPABASE_URL y (SUPABASE_ANON_KEY o SUPABASE_SERVICE_ROLE_KEY) "
            f"en {env_path} o en variables de entorno.\n"
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

_FETCH_MAX_ROWS = 2_000_000  # techo de seguridad: aborta si la tabla crece inesperadamente


def _fetch_all(query_builder) -> list[dict[str, Any]]:
    """
    Consume una query de supabase-py paginando hasta traer todas las filas.

    supabase-py devuelve como maximo FETCH_PAGE_SIZE filas por llamada. Esta
    funcion ejecuta la misma query con `.range(start, end)` incrementando
    hasta que no vuelven mas filas.

    El `query_builder` debe ser el builder ANTES de llamar .execute(). Ojo:
    la instancia se reusa, entonces no hay que haberle llamado .range() antes.

    Corta en _FETCH_MAX_ROWS como circuit breaker ante crecimiento inesperado
    de la tabla (bug de dedup, ingesta accidental de duplicados masivos).
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
        # Progreso visible cada 10 paginas (10K filas) en queries pesadas.
        if inicio % (FETCH_PAGE_SIZE * 10) == 0:
            logger.debug("_fetch_all: %d filas acumuladas...", len(filas))
        if len(filas) >= _FETCH_MAX_ROWS:
            logger.error(
                "_fetch_all: superado el limite de %d filas. Abortando paginacion. "
                "Verificar si hay duplicados masivos en la DB.",
                _FETCH_MAX_ROWS,
            )
            break
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
    columns: str = "*",
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
    query = client.table(TABLA_LINEUP).select(columns)

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


# ---------------------------------------------------------------------------
# Helpers especificos para el dashboard v2 (Bloomberg redesign)
# ---------------------------------------------------------------------------
# Estos wrappers enriquecen query_lineup con:
#  - Filtrado a productos prioritarios (8 productos del trading desk).
#  - Solo exportaciones (ops = "LOAD") porque el usuario no quiere imports.
#  - Normalizacion de shippers (agrega columnas shipper_canon y origen_alt).
#  - Agregaciones comunes para KPIs.

def query_exports_prioritarios(
    fecha_desde: date | None = None,
    fecha_hasta: date | None = None,
) -> pd.DataFrame:
    """
    Trae solo exportaciones (LOAD) de los 8 productos prioritarios.
    Agrega las columnas `shipper_canon` y `origen_alt` ya normalizadas.

    Es la base para Panorama, Shippers y Productos.
    """
    from config import CODIGOS_PRIORITARIOS
    from shipper_norm import aplicar_a_dataframe

    df = query_lineup(
        fecha_desde=fecha_desde,
        fecha_hasta=fecha_hasta,
        cargos=list(CODIGOS_PRIORITARIOS),
    )
    if df.empty:
        return df

    # Solo exportaciones (LOAD). El usuario no quiere imports en el dashboard.
    df = df[df["ops"] == "LOAD"].copy()

    # Normalizar shippers (agrega shipper_canon y origen_alt).
    df = aplicar_a_dataframe(df)
    return df


def query_en_puerto_ahora(fecha_ref: date) -> pd.DataFrame:
    """
    Buques actualmente en puerto: etb <= fecha_ref AND ets >= fecha_ref.

    Si etb o ets son NULL, se excluyen (no podemos saber si esta operando).
    Usa el line-up mas reciente disponible en DB (la query `lineup` tiene
    una fila por dia-consulta-buque; para "ahora" usamos el ultimo snapshot).
    """
    client = get_client()
    # Tomamos el snapshot del ultimo dia de consulta (fecha_ref).
    # Los buques que aparecen ese dia con ETB ya pasado y ETS futuro = en puerto.
    query = (
        client.table(TABLA_LINEUP)
        .select("*")
        .eq("fecha_consulta", fecha_ref.isoformat())
        .not_.is_("etb", "null")
        .not_.is_("ets", "null")
        .lte("etb", fecha_ref.isoformat())
        .gte("ets", fecha_ref.isoformat())
    )
    filas = _fetch_all(query)
    df = pd.DataFrame(filas)
    if df.empty:
        return df

    for col in ("fecha_consulta", "eta", "etb", "ets"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    return df


def ultima_fecha_cargada() -> date | None:
    """
    Devuelve la fecha_consulta mas reciente en la tabla.
    Util como default del selector de fecha en el dashboard.
    """
    client = get_client()
    resp = (
        client.table(TABLA_LINEUP)
        .select("fecha_consulta")
        .order("fecha_consulta", desc=True)
        .limit(1)
        .execute()
    )
    if not resp.data:
        return None
    valor = resp.data[0]["fecha_consulta"]
    if isinstance(valor, str):
        return datetime.strptime(valor, "%Y-%m-%d").date()
    return valor


def primera_fecha_cargada() -> date | None:
    """Primera fecha_consulta en DB (borde del histograma)."""
    client = get_client()
    resp = (
        client.table(TABLA_LINEUP)
        .select("fecha_consulta")
        .order("fecha_consulta", desc=False)
        .limit(1)
        .execute()
    )
    if not resp.data:
        return None
    valor = resp.data[0]["fecha_consulta"]
    if isinstance(valor, str):
        return datetime.strptime(valor, "%Y-%m-%d").date()
    return valor


def ultima_actualizacion_lineup() -> datetime | None:
    """
    Timestamp de la ultima fila insertada en la tabla `lineup`. Refleja
    cuando corrio el ultimo update exitoso (cron diario o manual). Util
    para mostrarle al usuario "Ultima actualizacion: dd-mm hh:mm".
    """
    client = get_client()
    resp = (
        client.table(TABLA_LINEUP)
        .select("created_at")
        .order("created_at", desc=True)
        .limit(1)
        .execute()
    )
    if not resp.data:
        return None
    valor = resp.data[0]["created_at"]
    if isinstance(valor, str):
        # Postgres devuelve timestamp con TZ ("...+00:00"). datetime.fromisoformat
        # de Python 3.11+ lo parsea bien.
        return datetime.fromisoformat(valor.replace("Z", "+00:00"))
    return valor


# ---------------------------------------------------------------------------
# DJVE: persistencia y lectura
# ---------------------------------------------------------------------------
# Las DJVE (Declaraciones Juradas de Ventas al Exterior) las baja update_djve.py
# del MAGyP cada dia. Se persisten en la tabla `djve` para que el dashboard
# pueda leerlas rapido (en lugar de descargar el XLSX cada vez que el usuario
# entra a la pestana Productos).

def upsert_djve(filas: list[dict[str, Any]],
                batch_size: int = UPSERT_BATCH_SIZE) -> int:
    """
    Inserta/actualiza filas en la tabla `djve`.

    Idempotente: si la misma DJVE (anio, nro_djve) viene de nuevo en una
    corrida posterior, se sobreescriben los campos en lugar de duplicar.

    Args:
        filas: lista de dicts con las keys de la tabla djve. Las filas deben
            traer al menos `anio` y `nro_djve` (la unique constraint).
        batch_size: tamano del lote para el upsert. Default 500 evita timeouts.

    Returns:
        Cantidad total de filas upserted.
    """
    if not filas:
        return 0

    client = get_client()
    total = 0

    for inicio in range(0, len(filas), batch_size):
        lote = filas[inicio:inicio + batch_size]
        try:
            resp = (
                client.table(TABLA_DJVE)
                .upsert(lote, on_conflict=UPSERT_CONFLICT_DJVE)
                .execute()
            )
        except Exception as exc:
            logger.error(
                "Upsert DJVE fallo en el lote %d-%d (%d filas): %s",
                inicio, inicio + len(lote), len(lote), exc,
            )
            raise

        total += len(resp.data) if resp.data else len(lote)
        logger.info("Upsert DJVE OK: lote de %d filas (acumulado %d/%d).",
                    len(lote), total, len(filas))

    return total


def query_djve(anio: int | None = None) -> pd.DataFrame:
    """
    Lee la tabla djve. Devuelve DataFrame con las mismas columnas que produce
    fob_djve.descargar_djve_acumuladas (sin `id`, sin `actualizado_en`).

    Args:
        anio: si se pasa, filtra a ese ano. Si es None, trae todo.

    Returns:
        DataFrame vacio si no hay data.
    """
    client = get_client()
    query = client.table(TABLA_DJVE).select(
        "nro_djve, fecha_registro, fecha_presentacion, producto, toneladas, "
        "fecha_inicio_embarque, fecha_fin_embarque, opcion, razon_social, "
        "codigo_interno, anio"
    )
    if anio is not None:
        query = query.eq("anio", anio)
    query = query.order("fecha_registro", desc=False)

    filas = _fetch_all(query)
    df = pd.DataFrame(filas)
    if df.empty:
        return df

    # Normalizar tipos para que matcheen con la salida de fob_djve.
    for col in ("fecha_registro", "fecha_presentacion",
                "fecha_inicio_embarque", "fecha_fin_embarque"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
    if "toneladas" in df.columns:
        df["toneladas"] = pd.to_numeric(df["toneladas"], errors="coerce").fillna(0)

    return df


def djve_ultima_actualizacion(anio: int | None = None) -> datetime | None:
    """
    Devuelve el timestamp mas reciente del campo `actualizado_en` en la tabla
    djve. Sirve para mostrar en el dashboard "ultima sync DJVE: hace X minutos".
    """
    client = get_client()
    query = client.table(TABLA_DJVE).select("actualizado_en")
    if anio is not None:
        query = query.eq("anio", anio)
    resp = query.order("actualizado_en", desc=True).limit(1).execute()
    if not resp.data:
        return None
    valor = resp.data[0]["actualizado_en"]
    if isinstance(valor, str):
        return datetime.fromisoformat(valor.replace("Z", "+00:00"))
    return valor


if __name__ == "__main__":
    # Uso: python db.py  -> prueba de conexion
    print("Probando conexion a Supabase...")
    resultado = ping()
    if resultado["conectado"]:
        print(f"OK. Conectado. La tabla '{TABLA_LINEUP}' tiene {resultado['cantidad_filas']} filas.")
    else:
        print(f"ERROR de conexion: {resultado['error']}")
