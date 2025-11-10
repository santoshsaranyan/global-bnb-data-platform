import httpx
from bs4 import BeautifulSoup
from io import BytesIO
import os
import logging
import time
import requests
from dotenv import load_dotenv
from google.oauth2 import service_account
from google.cloud import storage
import posixpath
import mimetypes

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

"""
This script scrapes data files from the InsideAirbnb website for listings.
It downloads the relevant gzipped CSV files and saves them to Google Cloud Storage (GCS).
"""

# Load environment variables from .env file
load_dotenv()

country_list = ['United Kingdom', 'United States', 'Canada', 'Germany']

def scrape_data():
    
    logging.info("Step 1: Scraping Data")
    # Define the URL to scrape
    url = "https://insideairbnb.com/get-the-data/"
    for country in country_list:
        cities = fetch_cities(url, country)
        for city in cities:
            fetch_html(url, city)
    

def fetch_cities(url: str, country: str, max_retries: int = 3, backoff_factor: float = 1.0) -> dict:
    """
    Returns a list of cities to scrape data for.
    
    Parameters:
        url (str): The URL to fetch.
        country (str): The country to filter cities by.
        max_retries (int): Maximum number of retries for fetching the URL.
        backoff_factor (float): Backoff factor for retries.
        
    Returns:
        list: A list of city names.
    """
    
    city_list = []
    
    for attempt in range(1, max_retries + 1):
        try:
            # Get the HTML content from the URL
            headers = {"User-Agent": "Mozilla/5.0 (+requests)"}
            response = httpx.get(url, headers=headers)
            
            if response.status_code == 200 and response.content:
                # Parse the HTML content
                html_source = response.text
                logging.info("Request was successful.")
                soup = BeautifulSoup(html_source, 'lxml')
                soup = BeautifulSoup(soup.prettify(), 'lxml')
                
                try:
                    city_section = soup.find('div', class_='contentcontainer-module--content-container--cf14f common-module--iab-content-container--cae9b').find_all('h3')
                    for section in city_section:
                        if country in section.text:
                            city_dict = {}
                            city_dict['country_name'] = country
                            city_dict['city_name'] = section.text.split(',')[0].strip()
                            
                            city_list.append(city_dict)
                            
                            logging.info(f"Found city: {city_dict['city_name']} in country: {country}")
                            
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
    
        
    return city_list
    
def download_file(link: str, city_dict) -> None:
    """
    Downloads a file from the given URL. Uploads the file to Google Cloud Storage (GCS).
    
    Parameters:
        link (str): The URL of the file to download.
        city_dict (dict): Dictionary containing city and country information.
        
    Returns:
        None
    """

    # Load credentials from environment variables
    credentials_path = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
    project_id = os.environ['GCP_PROJECT_ID']
    bucket_name = os.environ['GCP_BUCKET_NAME']

    # Create a credentials object
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    
    client = storage.Client(credentials=credentials, project=project_id)
    bucket = client.bucket(bucket_name)
    
    country_name = city_dict['country_name'].lower().replace(" ","_")
    city_name = city_dict['city_name'].lower().replace(" ","_")
    
    # Create destination folder if it doesn't exist
    save_folder = f'insideairbnb/country={country_name}/city={city_name}/'
    link_filename = link.split('/')[-1]
    #os.makedirs(save_folder, exist_ok=True)

    filename = posixpath.join(save_folder, link_filename)
    
    # Download the file
    try:
        headers = {"User-Agent": "Mozilla/5.0 (+requests)"}
        response = requests.get(link, timeout=60, headers=headers)
        response.raise_for_status()
        
        blob = bucket.blob(filename)
        #with open(filename, 'wb') as f:
            #f.write(response.content)
            
        content_type, _ = mimetypes.guess_type(link_filename)
        
        blob.upload_from_string(response.content, content_type=content_type)
            
        logging.info(f"File downloaded from insideairbnb and uploaded to GCS with path: gs://{bucket_name}/{filename}")
        
    except Exception as e:
        logging.error(f"Error: Error downloading file: {e}")  
    
    
def fetch_html(url: str, city_dict: dict, max_retries: int = 3, backoff_factor: float = 1.0) -> None:
    """
    Fetches the HTML content of a given URL. Uses BeautifulSoup to parse the HTML and extract data file links.
    
    Parameters:
        url (str): The URL to fetch.
        city_dict (dict): Dictionary containing city and country information.
        max_retries (int): Maximum number of retries for downloading the file.
        backoff_factor (float): Backoff factor for retries.
        
    Returns:
        None
    """
    for attempt in range(1, max_retries + 1):
        try:
            # Get the HTML content from the URL
            headers = {"User-Agent": "Mozilla/5.0 (+requests)"}
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
                                download_file(link, city_dict)
                                
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

    
if __name__ == "__main__":
    scrape_data()