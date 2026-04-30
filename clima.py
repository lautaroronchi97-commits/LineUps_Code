"""
Pronostico climatico para las 4 zonas portuarias argentinas principales.

Usa Open-Meteo (gratuito, sin API key, sin limites agresivos) para obtener
pronostico de 7 dias. El clima es relevante para el trading agro porque:
- Lluvias fuertes en el nodo Rosario paralizan cargas (elevadores no operan
  bajo lluvia por riesgo de humedad en el grano).
- Vientos altos en Bahia Blanca cierran el puerto.
- Nieblas en Necochea retrasan entradas.

Zonas cubiertas (definidas en config.ZONAS_CLIMA):
- Gran Rosario Norte, Gran Rosario Sur, Bahia Blanca, Necochea/Quequen.

API: https://open-meteo.com/en/docs
Endpoint: https://api.open-meteo.com/v1/forecast

Funcion principal:
- obtener_pronostico(zona) -> DataFrame con dia, t_max, t_min, precipitacion,
  viento_max, prob_lluvia, codigo_clima.
"""
from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd
import requests

from config import ZONAS_CLIMA

API_URL = "https://api.open-meteo.com/v1/forecast"

# Mapa de codigos WMO del World Meteorological Organization a descripcion + emoji.
# https://open-meteo.com/en/docs#weather_variable_documentation
CODIGO_CLIMA: dict[int, tuple[str, str]] = {
    0:  ("Despejado",            "☀️"),
    1:  ("Mayormente despejado", "🌤️"),
    2:  ("Parcialmente nublado", "⛅"),
    3:  ("Nublado",              "☁️"),
    45: ("Niebla",               "🌫️"),
    48: ("Niebla con escarcha",  "🌫️"),
    51: ("Llovizna ligera",      "🌦️"),
    53: ("Llovizna moderada",    "🌦️"),
    55: ("Llovizna fuerte",      "🌧️"),
    61: ("Lluvia ligera",        "🌦️"),
    63: ("Lluvia moderada",      "🌧️"),
    65: ("Lluvia fuerte",        "🌧️"),
    71: ("Nieve ligera",         "🌨️"),
    73: ("Nieve moderada",       "🌨️"),
    75: ("Nieve fuerte",         "❄️"),
    77: ("Nieve granular",       "❄️"),
    80: ("Chaparrones ligeros",  "🌦️"),
    81: ("Chaparrones moderados","🌧️"),
    82: ("Chaparrones violentos","⛈️"),
    85: ("Chaparrones nieve",    "🌨️"),
    86: ("Chaparrones nieve fuertes", "❄️"),
    95: ("Tormenta",             "⛈️"),
    96: ("Tormenta con granizo ligero",  "⛈️"),
    99: ("Tormenta con granizo fuerte",  "⛈️"),
}


def _descripcion_clima(codigo: int) -> tuple[str, str]:
    """Devuelve (descripcion, emoji) para un codigo WMO."""
    return CODIGO_CLIMA.get(codigo, ("Desconocido", "❓"))


def _consultar_api(lat: float, lon: float, timeout: int = 15) -> dict[str, Any]:
    """
    Pega a Open-Meteo y devuelve el JSON crudo. Levanta requests exception
    si falla; el caller debe manejarlo.
    """
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": ",".join([
            "weather_code",
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "precipitation_probability_max",
            "wind_speed_10m_max",
            "wind_gusts_10m_max",
        ]),
        "timezone": "America/Argentina/Buenos_Aires",
        "forecast_days": 7,
    }
    resp = requests.get(API_URL, params=params, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def obtener_pronostico(zona: str) -> pd.DataFrame:
    """
    Devuelve el pronostico de 7 dias para una zona portuaria.

    Columnas del DataFrame:
        fecha (date)
        t_max (float, °C)
        t_min (float, °C)
        lluvia_mm (float)
        prob_lluvia (int, %)
        viento_kmh (float)
        rafaga_kmh (float)
        codigo (int, WMO)
        descripcion (str)
        emoji (str)

    Si la API falla devuelve un DataFrame vacio (el dashboard lo detecta y
    muestra un mensaje amigable en vez de romperse).

    Args:
        zona: clave de ZONAS_CLIMA (ej "Gran Rosario Norte")

    Raises:
        KeyError si la zona no existe.
    """
    info = ZONAS_CLIMA[zona]
    try:
        data = _consultar_api(float(info["lat"]), float(info["lon"]))
    except requests.exceptions.RequestException:
        return pd.DataFrame()

    daily = data.get("daily", {})
    if not daily:
        return pd.DataFrame()

    df = pd.DataFrame({
        "fecha":       pd.to_datetime(daily["time"]).date,
        "t_max":       daily["temperature_2m_max"],
        "t_min":       daily["temperature_2m_min"],
        "lluvia_mm":   daily["precipitation_sum"],
        "prob_lluvia": daily["precipitation_probability_max"],
        "viento_kmh":  daily["wind_speed_10m_max"],
        "rafaga_kmh":  daily["wind_gusts_10m_max"],
        "codigo":      daily["weather_code"],
    })
    descripciones = df["codigo"].map(_descripcion_clima)
    df["descripcion"] = [d[0] for d in descripciones]
    df["emoji"]       = [d[1] for d in descripciones]
    return df


def pronostico_todas_zonas() -> dict[str, pd.DataFrame]:
    """
    Devuelve un dict {zona: DataFrame_pronostico} con las 4 zonas.
    Si alguna zona falla, su valor es un DataFrame vacio.
    """
    return {zona: obtener_pronostico(zona) for zona in ZONAS_CLIMA}


def clasificar_riesgo(row: pd.Series) -> str:
    """
    Clasifica el riesgo operativo portuario del dia en ALTO/MEDIO/BAJO/OK.

    Reglas heuristicas:
    - ALTO: tormenta (cod 95+) o lluvia >20mm o rafaga >60 km/h.
    - MEDIO: lluvia 5-20mm o rafaga 40-60 km/h o prob_lluvia >70%.
    - BAJO: lluvia 1-5mm o prob_lluvia 40-70%.
    - OK: el resto (condiciones operativas normales).

    Los umbrales son aproximaciones razonables; afinar con el usuario.
    """
    codigo = row.get("codigo", 0)
    lluvia = row.get("lluvia_mm", 0) or 0
    rafaga = row.get("rafaga_kmh", 0) or 0
    prob = row.get("prob_lluvia", 0) or 0

    if codigo >= 95 or lluvia > 20 or rafaga > 60:
        return "🔴 ALTO"
    if lluvia > 5 or rafaga > 40 or prob > 70:
        return "🟡 MEDIO"
    if lluvia > 1 or prob > 40:
        return "🟢 BAJO"
    return "⚪ OK"


if __name__ == "__main__":
    # Test manual: trae el pronostico de una zona y lo imprime.
    print("Probando Open-Meteo...\n")
    for zona in ZONAS_CLIMA:
        print(f"--- {zona} ---")
        df = obtener_pronostico(zona)
        if df.empty:
            print("  (API fallo o sin datos)\n")
            continue
        df["riesgo"] = df.apply(clasificar_riesgo, axis=1)
        for _, row in df.iterrows():
            print(
                f"  {row['fecha']} {row['emoji']} {row['descripcion']:25} "
                f"T {row['t_min']:.0f}-{row['t_max']:.0f}°C | "
                f"lluvia {row['lluvia_mm']:.1f}mm ({row['prob_lluvia']:.0f}%) | "
                f"viento {row['viento_kmh']:.0f} (rafaga {row['rafaga_kmh']:.0f}) | "
                f"{row['riesgo']}"
            )
        print()
