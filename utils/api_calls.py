from retry_requests import retry
from datetime import datetime, timedelta
import openmeteo_requests
import pandas as pd
import requests_cache
import yaml
import requests

import os

_CONFIG_PATHS = [
    "/opt/airflow/config/config.yaml",
    "/home/makos/dev/weather_dag/config/config.yaml",
    os.path.join(os.path.dirname(__file__), "..", "config", "config.yaml"),
]
_CONFIG_PATH = next((p for p in _CONFIG_PATHS if os.path.exists(p)), None)
if _CONFIG_PATH is None:
    raise FileNotFoundError(f"config.yaml not found in {_CONFIG_PATHS}")
with open(_CONFIG_PATH, "r") as f:
    CONFIG = yaml.safe_load(f)

yesterday = (datetime.today().date() - timedelta(days = 1)).strftime("%Y-%m-%d")
CITY = CONFIG["city"]
URL = CONFIG["api_url"]
FEATURES = CONFIG["features"]
TIME_ZONE = CONFIG["time_zone"]
CONVERTION_RATE = 111_111
OUTPUT_FOLDER = CONFIG["output_folder"]
LOCALITY_RADIUS = CONFIG["locality_radius"]

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

def get_coordinates(city):
    city_info = requests.get(
        "https://nominatim.openstreetmap.org/search",
        params={"q": city, "format": "json", "limit": 1},
        headers={"User-Agent": "my-app/1.0"}  # required by Nominatim
    )

    try:
        city_data = city_info.json()
    except Exception as e:
        print("Couldn't get the City Coordinates")
        raise e

    lat = float(city_data[0]["lat"])
    lon = float(city_data[0]["lon"])

    print(f"City: {city}")
    print(f"Latitude: {lat}, Longitude: {lon}")

    return lat, lon

def build_params(lat, lon, features, time_zone):
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": "",
        "end_date": "",
        "hourly": features,
        "timezone": time_zone,
    }

    return params

def get_df(responses) -> pd.DataFrame:
    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]
    print(f"Coordinates: {response.Latitude()}°N {response.Longitude()}°E")
    print(f"Elevation: {response.Elevation()} m asl")
    print(f"Timezone: {response.Timezone()}{response.TimezoneAbbreviation()}")
    print(f"Timezone difference to GMT+0: {response.UtcOffsetSeconds()}s")

    # Process hourly data. The order of variables needs to be the same as requested.
    hourly = response.Hourly()
    
    variables = {}
    
    for i, f in enumerate(FEATURES):
        variables[f] = hourly.Variables(i).ValuesAsNumpy()

    hourly_data = {"date": pd.date_range(
        start = pd.to_datetime(hourly.Time() + response.UtcOffsetSeconds(), unit = "s", utc = True),
        end =  pd.to_datetime(hourly.TimeEnd() + response.UtcOffsetSeconds(), unit = "s", utc = True),
        freq = pd.Timedelta(seconds = hourly.Interval()),
        inclusive = "left"
    )}

    for f in variables:
        hourly_data[f] = variables[f]

    hourly_dataframe = pd.DataFrame(data = hourly_data)
    return hourly_dataframe

def get_archive_data(params: dict, start_date : str, end_date : str) -> pd.DataFrame:
    params = params.copy()
    
    params["start_date"] = start_date
    params["end_date"] = end_date
    
    df = None
    
    try:
        responses = openmeteo.weather_api(URL, params = params)
        df = get_df(responses=responses)
        return df
    except Exception as e:
        print("Some Error Has acured: ", e)
        raise e

def get_data(start_date=yesterday, end_date=yesterday) -> pd.DataFrame:
    lat, lon = get_coordinates(CITY)
    params = build_params(lat, lon, features=FEATURES, time_zone=TIME_ZONE)
    return(get_archive_data(params=params, start_date = start_date, end_date = end_date))
    
def get_local_data(lat, lon, start_date, end_date):
    delta = LOCALITY_RADIUS / CONVERTION_RATE
    
    west_lon = ((lon - delta + 180) % 360) - 180
    east_lon = ((lon + delta + 180) % 360) - 180
    longs = [west_lon, east_lon]

    south_lat = max(-90, lat - delta)
    north_lat = min(90, lat + delta)
    lats = [south_lat, north_lat]
    
    labels = ["south_west", "north_west", "south_east", "north_east"]
    res = {}
    
    counter = 0
    for lon_i in longs: 
        for lat_j in lats:
            label = labels[counter]
            params = build_params(lat_j, lon_i, features=FEATURES, time_zone=TIME_ZONE)
            res[label] = get_archive_data(params=params, start_date=start_date, end_date=end_date)
            counter += 1
            
    return res

def load_data(start_date, end_date):
    
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    
    lat,lon = get_coordinates(CITY)
    center = get_data(start_date=start_date, end_date=end_date)
    local_data = get_local_data(lat, lon, start_date=start_date, end_date=end_date)
    center.to_csv(f"{OUTPUT_FOLDER}/city_data_{timestamp}.csv")
    
    for point in local_data:
        local_data[point].to_csv(f"{OUTPUT_FOLDER}/{point}_data_{timestamp}.csv")

def load_archive(start_date, end_date):
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    
    lat, lon = get_coordinates(CITY)
    params = build_params(lat, lon, features=FEATURES, time_zone=TIME_ZONE)
    df = get_archive_data(params, start_date=start_date, end_date=end_date)
    df.to_csv(f"{OUTPUT_FOLDER}/archive_{timestamp}.csv")
    
    
if __name__ == "__main__":
    # get_data()
    load_data("2024-04-14", "2026-04-14")
    # load_data("2025-04-12", yesterday)
    # print(yesterday)
