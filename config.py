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
# Rediseno v2: productos PRIORITARIOS para el trading desk
# ---------------------------------------------------------------------------
# El usuario definio que solo le importan 8 productos (los 4 del complejo soja,
# maiz, trigo, cebada, sorgo y girasol). El resto se filtra por default.
#
# Cada entrada es (codigo_interno_en_DB, nombre_display, familia).
# La familia se usa para agrupar en el tab Panorama.

PRODUCTOS_PRIORITARIOS = [
    # (codigo, display, familia)
    ("SBS",     "Soja",          "Soja"),
    ("SBM",     "Harina soja",   "Soja"),
    ("SBO",     "Aceite soja",   "Soja"),
    ("MAIZE",   "Maiz",          "Maiz"),
    ("WHEAT",   "Trigo",         "Trigo"),
    ("BARLEY",  "Cebada",        "Cebada"),
    ("SORGHUM", "Sorgo",         "Sorgo"),
    ("SFSEED",  "Girasol",       "Girasol"),
]

# Set de codigos para filtros rapidos.
CODIGOS_PRIORITARIOS = {codigo for codigo, _, _ in PRODUCTOS_PRIORITARIOS}

# Display label por codigo (ej "SBM" -> "Harina soja").
PRODUCTO_DISPLAY = {codigo: display for codigo, display, _ in PRODUCTOS_PRIORITARIOS}


# ---------------------------------------------------------------------------
# Paleta de colores fija por shipper canonico (Bloomberg dark theme)
# ---------------------------------------------------------------------------
# Colores altamente saturados sobre fondo oscuro. Cada shipper mantiene
# SIEMPRE el mismo color en todos los graficos del dashboard (facilita lectura
# rapida cuando uno esta acostumbrado a la vista).

SHIPPER_COLORS: dict[str, str] = {
    "VITERRA-BUNGE": "#FF9900",  # amber (dominante en soja)
    "CARGILL":       "#00D4FF",  # cyan
    "COFCO":         "#FF3333",  # red (marca china)
    "LDC":           "#CC66FF",  # violeta
    "ADM":           "#FF66CC",  # rosa
    "AGD":           "#33FF99",  # verde menta
    "ACA":           "#FFD700",  # dorado (cooperativas)
    "MOLINOS":       "#66FF66",  # verde
    "QUILMES":       "#FF6600",  # naranja (cerveza)
    "GLENCORE":      "#9999FF",  # celeste
    "OLAM":          "#FFCC33",  # mostaza
    "OTROS":         "#808080",  # gris neutro
}

# Paleta Bloomberg (para elementos generales del dashboard).
BLOOMBERG_PALETTE = {
    "bg_primary":   "#0A0E1A",   # fondo principal
    "bg_card":      "#141B2B",   # cards/containers
    "bg_hover":     "#1E2738",
    "accent":       "#FF9900",   # amber signature Bloomberg
    "accent_blue":  "#00D4FF",
    "positive":     "#00FF88",
    "negative":     "#FF3B3B",
    "warning":      "#FFCC00",
    "text_primary": "#E1E5EE",
    "text_muted":   "#8899AA",
    "grid":         "#1A2332",
}


# ---------------------------------------------------------------------------
# Zonas climaticas (FASE 2: pronostico Open-Meteo 7 dias)
# ---------------------------------------------------------------------------
# Coordenadas centroides de los 4 nodos portuarios mas importantes.
# Gran Rosario se divide en Norte y Sur segun pedido del usuario.

ZONAS_CLIMA: dict[str, dict[str, float | str]] = {
    "Gran Rosario Norte": {
        "lat": -32.833, "lon": -60.733,
        "descripcion": "San Lorenzo, Timbues, San Martin, Rosario",
    },
    "Gran Rosario Sur": {
        "lat": -33.017, "lon": -60.633,
        "descripcion": "General Lagos, Alvear",
    },
    "Bahia Blanca": {
        "lat": -38.717, "lon": -62.267,
        "descripcion": "Puerto Galvan, Ingeniero White",
    },
    "Necochea/Quequen": {
        "lat": -38.583, "lon": -58.700,
        "descripcion": "Puerto Quequen",
    },
}


# ---------------------------------------------------------------------------
# Agrupacion de puertos por zona (para tab Congestion)
# ---------------------------------------------------------------------------
# Mapea cada puerto (como aparece en la columna `port` de la DB) a su zona.
# Los puertos del Gran Rosario se dividen Norte/Sur. Los no listados caen
# en "Otros".

PUERTOS_GRAN_ROSARIO_SUR = {
    "GENERAL LAGOS",
    "PUERTO GRAL. LAGOS",
    "ARROYO SECO",
    "PUNTA ALVEAR",
    "ALVEAR",
}

PUERTOS_GRAN_ROSARIO_NORTE = {
    "ROSARIO",
    "SAN LORENZO",
    "SAN MARTIN",
    "PUERTO SAN MARTIN",
    "TIMBUES",
    "PUERTO GENERAL SAN MARTIN",
    "RICARDONE",
}

PUERTOS_BAHIA_BLANCA = {
    "BAHIA BLANCA",
    "PUERTO GALVAN",
    "INGENIERO WHITE",
    "CARGILL BAHIA",
}

PUERTOS_NECOCHEA = {
    "NECOCHEA",
    "QUEQUEN",
    "PUERTO QUEQUEN",
}

# Alto Parana: puertos argentinos al norte del Gran Rosario (rio arriba).
# San Nicolas y Ramallo estan cerca pero no son parte del nodo Rosario.
PUERTOS_ALTO_PARANA = {
    "SAN NICOLAS",
    "CAMPANA",
    "RAMALLO",
    "ZARATE",
    "LIMA",
    "DEL GUAZU",
    "GUAZU",
}

# Buenos Aires / La Plata (estuario del Rio de la Plata).
PUERTOS_BUENOS_AIRES = {
    "LA PLATA",
    "DOCK SUD",
    "BUENOS AIRES",
    "PUERTO NUEVO",
}

# Uruguay (casi siempre transbordo o corta de soja PY/BR).
PUERTOS_URUGUAY = {
    "MONTEVIDEO",
    "NUEVA PALMIRA",
    "PAYSANDU",
    "FRAY BENTOS",
    "PUERTO NUEVA PALMIRA",
}

# Patagonia argentina.
PUERTOS_PATAGONIA = {
    "PUERTO MADRYN",
    "COMODORO RIVADAVIA",
    "PUERTO ROSALES",
    "PUNTA COLORADA",
    "CALETA PAULA",
    "USHUAIA",
}


def zona_de_puerto(port: str | None) -> str:
    """
    Devuelve la zona portuaria a la que pertenece un puerto.

    Zonas: Gran Rosario Norte/Sur, Bahia Blanca, Necochea/Quequen,
    Alto Parana, Buenos Aires/La Plata, Uruguay, Patagonia, Otros.
    Comparacion case-insensitive con match exacto primero, despues substring
    como fallback para variantes ortograficas.
    """
    if not port:
        return "Otros"
    p = port.upper().strip()
    # Match exacto primero.
    if p in PUERTOS_GRAN_ROSARIO_NORTE:
        return "Gran Rosario Norte"
    if p in PUERTOS_GRAN_ROSARIO_SUR:
        return "Gran Rosario Sur"
    if p in PUERTOS_BAHIA_BLANCA:
        return "Bahia Blanca"
    if p in PUERTOS_NECOCHEA:
        return "Necochea/Quequen"
    if p in PUERTOS_ALTO_PARANA:
        return "Alto Parana"
    if p in PUERTOS_BUENOS_AIRES:
        return "Buenos Aires/La Plata"
    if p in PUERTOS_URUGUAY:
        return "Uruguay"
    if p in PUERTOS_PATAGONIA:
        return "Patagonia"
    # Fallback por substring.
    if "LAGOS" in p or "ALVEAR" in p:
        return "Gran Rosario Sur"
    if "ROSARIO" in p or "SAN LORENZO" in p or "SAN MARTIN" in p or "TIMBUES" in p or "RICARDONE" in p:
        return "Gran Rosario Norte"
    if "BAHIA" in p or "GALVAN" in p or "WHITE" in p:
        return "Bahia Blanca"
    if "QUEQUEN" in p or "NECOCHEA" in p:
        return "Necochea/Quequen"
    if "MONTEVIDEO" in p or "PALMIRA" in p or "PAYSANDU" in p or "FRAY BENTOS" in p:
        return "Uruguay"
    if "MADRYN" in p or "COMODORO" in p or "ROSALES" in p or "USHUAIA" in p:
        return "Patagonia"
    if "CAMPANA" in p or "NICOLAS" in p or "RAMALLO" in p or "ZARATE" in p or "GUAZU" in p:
        return "Alto Parana"
    if "LA PLATA" in p or "DOCK SUD" in p or "BUENOS AIRES" in p:
        return "Buenos Aires/La Plata"
    return "Otros"


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
