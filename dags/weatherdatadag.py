from airflow import DAG
from airflow.decorators import task
from pendulum import datetime, now
from datetime import timedelta, date
import logging
import time
import posixpath
import requests
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from utilities.cityfetcher import fetch_cities, flatten_cities
import io
import pandas as pd

# Airflow Variables
url = Variable.get("INSIDEAIRBNB_URL")
country_list = Variable.get("COUNTRY_LIST", default_var='["United Kingdom","United States","Canada","Germany"]', deserialize_json=True)
bucket_name = Variable.get("GCS_BUCKET_NAME")
geocode_url = Variable.get("GEOCODE_URL", default_var="https://geocoding-api.open-meteo.com/v1/search")
weather_url = Variable.get("WEATHER_URL", default_var="https://archive-api.open-meteo.com/v1/archive")

# GEOCODE_URL = "https://geocoding-api.open-meteo.com/v1/search"
# WEATHER_URL = "https://archive-api.open-meteo.com/v1/archive"


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=30)
}

@task(multiple_outputs=False)
def resolve_city_coordinates(city_dict: dict, geocode_url: str) -> dict:
    """
    Resolves latitude, longitude, and timezone for a given city and country using the Open-Meteo Geocoding API.
    If no result is found, returns None (so the city is skipped).
    
    Parameters:
        city_dict (dict): Dictionary containing city and country information.
    
    Returns:
        dict: Dictionary containing city name, country name, latitude, longitude, and timezone.
    """
    city_name = city_dict["city_name"]
    country_name = city_dict["country_name"]
    query_string = f"{city_name}, {country_name}"

    logging.info(f"Resolving coordinates for: {query_string}")

    try:
        geocode_response = requests.get(
            geocode_url,
            params={"name": query_string, "count": 5, "language": "en", "format": "json"},
            timeout=30,
        )
        geocode_response.raise_for_status()
        results_json = (geocode_response.json() or {}).get("results") or []

        if not results_json:
            logging.warning(f"No results found for '{query_string}', skipping city.")
            return None

        country_lower = country_name.strip().lower()
        city_lower = city_name.strip().lower()

        def calculate_match_score(result_item):
            score = 0
            if result_item.get("country", "").lower() == country_lower:
                score += 2
            if result_item.get("name", "").lower() == city_lower:
                score += 2
            if result_item.get("feature_code") in ("PPLC", "PPLA", "PPLA2"):
                score += 1
            return score

        # Get the best matching city based on the match score
        best_match = sorted(results_json, key=calculate_match_score, reverse=True)[0]

        resolved_info = {
            "city_name": city_name,
            "country_name": country_name,
            "latitude": best_match["latitude"],
            "longitude": best_match["longitude"],
            "timezone": best_match.get("timezone", "auto"),
        }

        logging.info(f'Resolved info for {query_string}: {resolved_info}')
        return resolved_info

    except Exception as error:
        logging.error(f"Error resolving '{query_string}': {error}")
        return None


@task(multiple_outputs=False)
def fetch_and_upload_weather_daily(city_info: dict, bucket_name: str, weather_url: str, max_retries: int = 3, backoff_factor: float = 1.0) -> None:
    """
    Fetches daily weather data (current month) for a city and uploads it to GCS as a Parquet file.

    Parameters:
        city_info (dict): {'city_name','country_name','latitude','longitude','timezone'}
        bucket_name (str): Target GCS bucket
        
    Returns:
        None
    """

    city_name = city_info["city_name"]
    country_name = city_info["country_name"]
    latitude = city_info["latitude"]
    longitude = city_info["longitude"]

    run_date = now("UTC")
    month_str_prefix = run_date.format("YYYYMM")
    country_prefix = country_name.lower().replace(" ", "_")
    city_prefix = city_name.lower().replace(" ", "_")

    object_prefix = (f"openmeteo/extract_month={month_str_prefix}/country={country_prefix}/city={city_prefix}/")
    object_name = posixpath.join(object_prefix, "weather_daily.parquet")
    logging.info(f"Object prefix: {object_prefix}")

    start_date = date.today().replace(day=1).isoformat()
    end_date = date.today().isoformat()

    query_params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "daily": "temperature_2m_max,temperature_2m_min,temperature_2m_mean,precipitation_sum",
        "timezone": "auto",
        "temperature_unit": "celsius",
        "precipitation_unit": "mm",
    }

    conn_id = "GCS_CONN_ID"
    gcs_hook = GCSHook(gcp_conn_id=conn_id)

    if gcs_hook.exists(bucket_name=bucket_name, object_name=object_name):
        logging.info(f"Exists, skipping: gs://{bucket_name}/{object_name}")
        return None

    for attempt in range(1, max_retries + 1):
        try:
            logging.info(f"Fetching daily weather for {city_name}, {country_name} from {start_date} to {end_date}")

            weather_response = requests.get(weather_url, params=query_params, timeout=60)
            weather_response.raise_for_status()

            weather_json = weather_response.json()
            if "daily" not in weather_json or not weather_json["daily"].get("time"):
                logging.warning(f"No data for {city_name}, skipping.")

            weather_df = pd.DataFrame(weather_json["daily"]).copy()
            weather_df.rename(columns={"time": "date"}, inplace=True)
            weather_df["date"] = pd.to_datetime(weather_df["date"]).dt.strftime("%Y-%m-%d")
            
            logging.info(f"Fetched {len(weather_df)} rows of weather data for {city_name}.")
            logging.info(weather_df.head().to_string())

            # Convert to Parquet bytes
            parquet_buffer = io.BytesIO()
            weather_df.to_parquet(parquet_buffer, index=False)
            parquet_bytes = parquet_buffer.getvalue()

            gcs_hook.upload(
                bucket_name=bucket_name,
                object_name=object_name,
                data=parquet_bytes,
                mime_type="application/octet-stream",
            )

            logging.info(f"Uploaded Parquet file to GCS: gs://{bucket_name}/{object_name}")
            break

        except Exception as error:
            logging.error(f"Error fetching/uploading: {error}")
            if attempt < max_retries:
                delay = backoff_factor * (2 ** (attempt - 1))
                logging.info(f"Retrying in {delay:.1f} seconds...")
                time.sleep(delay)
            else:
                logging.error("Max retries exceeded.")


with DAG(
    dag_id="weather_monthly_scraper",
    default_args=default_args,
    schedule='@monthly',
    catchup=False,
    tags=["openmeteo", "weather", "gcs"],
) as dag:

    # Map over countries
    city_lists_per_country = fetch_cities.partial(url=url).expand(country=country_list)
    
    # Flatten the list of lists into a single list
    city_lists = flatten_cities(city_lists_per_country)
    
    # Resolve city coordinates
    city_coordinates = resolve_city_coordinates.partial(geocode_url=geocode_url).expand(city_dict=city_lists)

    # Fetch and upload weather data
    weather_daily = fetch_and_upload_weather_daily.partial(weather_url=weather_url, bucket_name=bucket_name).expand(city_info=city_coordinates)