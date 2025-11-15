from airflow import DAG
from airflow.decorators import task
from pendulum import datetime, now
from datetime import timedelta
import logging
import time
import io
import posixpath
import pandas as pd
from pytrends.request import TrendReq
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from utilities.cityfetcher import fetch_cities, flatten_cities


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

# Airflow Variables
url = Variable.get("INSIDEAIRBNB_URL")
bucket_name = Variable.get("GCS_BUCKET_NAME")
country_list = Variable.get("COUNTRY_LIST", default_var='["United Kingdom","United States","Canada","Germany"]', deserialize_json=True)


@task(multiple_outputs=False)
def fetch_and_upload_travel_interest(city_info: dict, bucket_name: str, max_retries: int = 3, backoff_factor: float = 1.0) -> None:
    """
    Fetches Google Trends interest-over-time for simple travel-intent keywords for a city and uploads to GCS (Parquet).

    Parameters:
        city_info (dict): {'city_name': str, 'country_name': str}
        bucket_name (str): Target GCS bucket
        max_retries (int): Maximum number of retries for fetching the data.
        backoff_factor (float): Backoff factor for retries.
    
    Returns:
        None
    """
    city_name = city_info["city_name"]
    country_name = city_info["country_name"]

    # Prefixes and object path
    run_date = now("UTC")
    month_str_prefix = run_date.format("YYYYMM")
    country_prefix = country_name.lower().replace(" ", "_")
    city_prefix = city_name.lower().replace(" ", "_")
    object_prefix = f"googletrends/extract_month={month_str_prefix}/country={country_prefix}/city={city_prefix}/"
    object_name = posixpath.join(object_prefix, "interest_over_time.parquet")

    logging.info(f"Object prefix: {object_prefix}")

    # Initialize GCS hook
    conn_id = "GCS_CONN_ID"
    gcs_hook = GCSHook(gcp_conn_id=conn_id)
    
    # Skip if already exists
    if gcs_hook.exists(bucket_name=bucket_name, object_name=object_name):
        logging.info(f"Exists, skipping: gs://{bucket_name}/{object_name}")
        return None

    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"} #Mozilla/5.0 (+requests)
    timeframe = "today 3-m"
    geo = ""

    # Build the keyword list
    city_clean = city_name.strip()
    kw_list = [
        f"visit {city_clean}",
        f"things to do in {city_clean}",
        f"{city_clean} airbnb",
    ]

    # Configure pytrends
    pytrends = TrendReq(
        hl="en-US",
        tz=0,
        retries=3,
        backoff_factor=1,
        requests_args={"headers": headers},
    )

    try:
        pytrends.build_payload(
            kw_list=kw_list,
            cat=0,
            timeframe=timeframe,  
            geo=geo,              
            gprop="",
        )

        df = pytrends.interest_over_time()
        if df is None or df.empty:
            logging.error(f"Error fetching for {city_name}: No data returned.")
        
            return None

        interest_cols = [c for c in df.columns if c in kw_list]

        # Rename keyword columns to standardized format
        rename_map = {col: col.replace(f'{city_clean}','city').lower().replace(" ", "_") for col in interest_cols}
        df = df.rename(columns=rename_map)

        logging.info(f"Fetched {len(df)} rows for {city_name}. Sample:\n{df.head(5).to_string()}")
        logging.info(df.dtypes)

        try:
            # Upload as Parquet
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=True)
            parquet_bytes = parquet_buffer.getvalue()

            gcs_hook.upload(
                bucket_name=bucket_name,
                object_name=object_name,
                data=parquet_bytes,
                mime_type="application/octet-stream",
            )

            logging.info(f"Uploaded Parquet file to GCS: gs://{bucket_name}/{object_name}")
        
        except Exception as e:
            logging.error(f"Error uploading Parquet to GCS for {city_name}: {e}")

    except Exception as e:
        logging.error(f"Error fetching/uploading Trends for {city_name}: {e}")


with DAG(
    dag_id="travel_interest_trends_monthly",
    default_args=default_args,
    schedule="@monthly",
    catchup=False,
    tags=["googletrends", "travel", "gcs"],
) as dag:
    
    # Map over countries
    city_lists_per_country = fetch_cities.partial(url=url).expand(country=country_list)
    
    # Flatten the list of lists into a single list
    city_lists = flatten_cities(city_lists_per_country)

    # Fetch and upload travel interest trends
    fetch_and_upload_travel_interest.partial(bucket_name=bucket_name).expand(city_info=city_lists)
