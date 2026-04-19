"""
Constantes y configuracion del proyecto.

Todo lo que no cambia entre ejecuciones vive aca: URLs, listas de productos,
mapas de meses, etc. Si manana aparece un producto nuevo o queres agregar
una categoria al tablero, este es el unico archivo que tenes que tocar.
"""

# ---------------------------------------------------------------------------
# Fuente de datos (ISA Agents)
# ---------------------------------------------------------------------------

# URL base del Line Up. El scraper le agrega los parametros via querystring:
#   ?lang=es&select_day=DD&select_month=MM&select_year=YYYY&mode=Search
BASE_URL = "https://www.isa-agents.com.ar/info/line_up_mndrn.php"

# User-Agent identificable (buena practica: decir quien sos y para que).
# Si algun dia ISA cambia la pagina y quieren contactarte, pueden.
USER_AGENT = "LineUpDashboard/1.0 (agro trading research; personal use)"

# Timeout por request (en segundos). 30s es generoso pero la pagina a veces tarda.
REQUEST_TIMEOUT = 30

# Reintentos por fecha fallida y backoff entre intentos.
MAX_RETRIES = 2
RETRY_BACKOFF_SECONDS = 3

# Delay entre requests sucesivos en el backfill. 2s es respetuoso con ISA.
DEFAULT_DELAY_SECONDS = 2.0


# ---------------------------------------------------------------------------
# Parseo de fechas
# ---------------------------------------------------------------------------

# La tabla devuelve fechas como "14-abr" (dia-mes abreviado en espanol).
# Este mapa convierte la abreviatura al numero de mes.
MESES_ES = {
    "ene": 1,
    "feb": 2,
    "mar": 3,
    "abr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "ago": 8,
    "sep": 9,
    "set": 9,   # variante que a veces aparece
    "oct": 10,
    "nov": 11,
    "dic": 12,
}


# ---------------------------------------------------------------------------
# Categorias y productos AGRO
# ---------------------------------------------------------------------------

# Categorias 100% agro. Si la columna `Cat` de la tabla cae en una de estas,
# es_agro == True. Fertilizantes es "agro adyacente" (indicador de siembra),
# lo tratamos aparte cuando queremos incluirlo.
AGRO_CATEGORIES = {"GRAINS", "BY PRODUCTS", "VEGOIL"}

# Productos top por familia. Los usa el dashboard como opciones default
# en los filtros de la pestana "Comparativa historica".
PRODUCTOS_GRANOS = ["MAIZE", "WHEAT", "SBS", "SORGHUM", "BARLEY", "SFSEED", "MALT"]
PRODUCTOS_HARINAS = ["SBM", "SFMP", "SHULLS", "CORN GLTN", "WBP"]
PRODUCTOS_ACEITES = ["SBO", "SFO", "NSBO", "LECITHIN"]
PRODUCTOS_FERTILIZANTES = ["UREA", "MAP", "DAP", "MOP", "UAN"]

# Todos los productos relevantes, en un solo lugar.
PRODUCTOS_AGRO_TODOS = (
    PRODUCTOS_GRANOS
    + PRODUCTOS_HARINAS
    + PRODUCTOS_ACEITES
    + PRODUCTOS_FERTILIZANTES
)


# ---------------------------------------------------------------------------
# Schema esperado de la tabla HTML (sanity check)
# ---------------------------------------------------------------------------

# Si ISA cambia el orden o nombre de columnas, queremos fallar rapido con un
# error claro, no silenciosamente con datos desalineados. El scraper compara
# el <thead> contra esta lista.
EXPECTED_HEADERS = [
    "Port",
    "Berth",
    "Vessel",
    "Ops.",
    "Cat",
    "Cargo",
    "Quantity",
    "Dest/Orig.",
    "Area",
    "Shipper",
    "ETA",
    "ETB",
    "ETS",
    "Remarks",
]

# Columnas que mapean a la tabla `lineup` en Supabase (mismo orden).
DB_COLUMNS = [
    "port",
    "berth",
    "vessel",
    "ops",
    "cat",
    "cargo",
    "quantity",
    "dest_orig",
    "area",
    "shipper",
    "eta",
    "etb",
    "ets",
    "remarks",
]


# ---------------------------------------------------------------------------
# Base de datos (Supabase)
# ---------------------------------------------------------------------------

TABLA_LINEUP = "lineup"

# Upsert en lotes de 500 filas para evitar timeouts del API de Supabase.
UPSERT_BATCH_SIZE = 500

# Paginacion: Supabase devuelve max 1000 filas por query. Este es el tamano
# de cada "pagina" cuando traemos data historica.
FETCH_PAGE_SIZE = 1000

# Clave unica logica usada en el upsert para evitar duplicados.
# Debe coincidir con la UNIQUE CONSTRAINT definida en el DDL de la tabla.
#
# Incluimos dest_orig, shipper y ops porque un mismo buque a veces aparece
# con filas identicas en port/berth/vessel/cargo/qty/eta pero splitea la carga
# a varios destinos o shippers. Cada fila de la fuente debe ser unica.
UPSERT_CONFLICT_COLUMNS = (
    "fecha_consulta,port,berth,vessel,cargo,quantity,eta,dest_orig,shipper,ops"
)
