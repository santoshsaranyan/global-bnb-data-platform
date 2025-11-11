from airflow.decorators import task
import logging
import time
import httpx
from bs4 import BeautifulSoup

@task(multiple_outputs=False)
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


@task(multiple_outputs=False)
def flatten_cities(list_of_lists: list[list[dict]]) -> list[dict]:
    """
    Flattens a list of lists into a single list.
    
    Parameters:
        list_of_lists (list of list of dict): The list of lists to flatten.
        
    Returns:
        list of dict: The flattened list. 
    """
    return [item for sublist in list_of_lists for item in sublist]