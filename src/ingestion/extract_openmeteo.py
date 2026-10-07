import json
import time
from pathlib import Path
import datetime as dt

import requests

WEATHER_BASE = "https://archive-api.open-meteo.com/v1/archive"
RAW_DIR = Path("data/raw/openmeteo")

LOCATIONS = {
    "hamburg": (53.55, 9.99),
    "muenchen": (48.14, 11.58),
    "berlin": (52.52, 13.41),
    "karlsruhe": (49.00, 8.41)
}

VARIABLES = ["wind_speed_100m", "shortwave_radiation", "temperature_2m"]

START_DATE = dt.date.today() - dt.timedelta(weeks=52)
END_DATE = dt.date.today() - dt.timedelta(days=1)

def fetch_weather(lat, lon, start, end):
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start,
        "end_date": end,
        "hourly": ",".join(VARIABLES),
        "timezone": "UTC"
    }
    r = requests.get(WEATHER_BASE, params=params, timeout=60)
    r.raise_for_status()
    return r.json()

def extract_location(name, lat, lon):
    out_dir = RAW_DIR / f"location={name}"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{START_DATE}_{END_DATE}.json"

    if path.exists():
        print(f"{name}: already exists, skipping")
        return

    data = fetch_weather(lat, lon, START_DATE, END_DATE)
    path.write_text(json.dumps(data), encoding="utf-8")
    print(f"{name}: saved ({len(data["hourly"]["time"])} hours)")

def run():
    for name, (lat, lon) in LOCATIONS.items():
        extract_location(name, lat, lon)
        time.sleep(0.5)

if __name__ == "__main__":
    run()