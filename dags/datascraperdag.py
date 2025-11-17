from airflow import DAG
from airflow.decorators import task
from pendulum import datetime, now
from datetime import timedelta
import logging
import time
import posixpath
import mimetypes
import requests
import httpx
from bs4 import BeautifulSoup
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from utilities.cityfetcher import fetch_cities, flatten_cities

# Airflow Variables
url = Variable.get("INSIDEAIRBNB_URL")
country_list = Variable.get("COUNTRY_LIST", default_var='["United Kingdom","United States","Canada","Germany"]', deserialize_json=True)
bucket_name = Variable.get("GCS_BUCKET_NAME")

# country_list = ['United Kingdom', 'United States', 'Canada', 'Germany']
# url = "https://insideairbnb.com/get-the-data/"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(seconds=300)
}
    
@task(multiple_outputs=False)
def fetch_and_upload_files(url: str, city_dict: dict, bucket_name: str, max_retries: int = 3, backoff_factor: float = 1.0) -> None:
    """
    Fetches the HTML content of a given URL. Uses BeautifulSoup to parse the HTML and extract data file links. Then downloads each file and uploads it to Google Cloud Storage (GCS).
    
    Parameters:
        url (str): The URL to fetch.
        city_dict (dict): Dictionary containing city and country information.
        max_retries (int): Maximum number of retries for downloading the file.
        backoff_factor (float): Backoff factor for retries.
        
    Returns:
        None
    """
    country_prefix = city_dict["country_name"].lower().replace(" ", "_")
    city_prefix = city_dict["city_name"].lower().replace(" ", "_")
    run_date = now("UTC")
    month_str_prefix = run_date.format("YYYYMM")

    name_prefix = (f"insideairbnb/extract_month={month_str_prefix}/country={country_prefix}/city={city_prefix}/")
    
    logging.info(f"Object prefix: {name_prefix}")
    
    headers = {"User-Agent": "Mozilla/5.0 (+requests)"}

    conn_id = "GCS_CONN_ID"
    hook = GCSHook(gcp_conn_id=conn_id)
    
    for attempt in range(1, max_retries + 1):
        try:
            # Get the HTML content from the URL
            
            response = httpx.get(url, headers=headers)
            
            if response.status_code == 200 and response.content:
                # Parse the HTML content
                html_source = response.text
                logging.info("Request was successful.")
                soup = BeautifulSoup(html_source, 'lxml')
                soup = BeautifulSoup(soup.prettify(), 'lxml')
                
                try:
                    city = soup.find('table',class_=f'data table table-hover table-striped {city_dict["city_name"].lower().replace(" ","-")}')
                    table_rows = city.find('tbody').find_all('tr')
                    logging.info(f"Found {len(table_rows)} rows in the table.")
                    
                    for row in table_rows:
                        try:
                            link_item = row.find('a', href=True)
                            link = link_item['href']
                            if '/data/' in link:
                                logging.info(f"Downloading file from link: {link}")
                                try:
                                    file_response = requests.get(link, timeout=60, headers=headers)
                                    file_response.raise_for_status()
                                    
                                    link_filename = link.split('/')[-1]
                                    object_name = posixpath.join(name_prefix, link_filename)
                                    content_type, _ = mimetypes.guess_type(link_filename)
                                    
                                    try:
                                        if hook.exists(bucket_name=bucket_name, object_name=object_name):
                                            logging.info(f"Object Exists in GCS, skipping: gs://{bucket_name}/{object_name}")
                                            continue
                                        
                                        hook.upload(
                                            bucket_name=bucket_name,
                                            object_name=object_name,
                                            data=file_response.content,
                                            mime_type=content_type,
                                        )
                                        
                                        logging.info(f"File downloaded from insideairbnb and uploaded to GCS with path: gs://{bucket_name}/{object_name}")
                                    
                                    except Exception as e:
                                        raise RuntimeError(f"Error: {e}")
                                    
                                except Exception as e:
                                    logging.error(f"Error downloading file from link {link}: {e}")
                                    continue
                                    
                                
                        except Exception as e:
                            logging.error(f"Error: Error extracting link: {e}")
                        
                except Exception as e:
                    logging.error(f"Error: Error parsing HTML: {e}")
                
                break
                        
        except Exception as e:
            logging.info(f"Error fetching the URL: {e}")
        
            # Backoff before next retry
            if attempt < max_retries:
                # Delay with exponential backoff
                delay = backoff_factor * (2 ** (attempt - 1))
                logging.info(f"Retrying in {delay:.1f} seconds...")
                time.sleep(delay)
                
            else:
                logging.error("Error: Max retries exceeded. Failed to fetch URL.")


                
with DAG(
    dag_id="globalbnb_monthly_scraper",
    default_args=default_args,
    schedule='@monthly',
    catchup=False,
    tags=["insideairbnb", "scraper", "gcs"],
) as dag:

    # Map over countries
    city_lists_per_country = fetch_cities.partial(url=url).expand(country=country_list)
    
    # Flatten the list of lists into a single list
    city_lists = flatten_cities(city_lists_per_country)

    # Map over cities, Each mapped task handles all files for that city.
    fetch_and_upload_files.partial(url=url, bucket_name=bucket_name).expand(city_dict=city_lists)