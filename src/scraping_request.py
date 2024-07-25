from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
import pandas as pd
from src.db_connection import IMDBDBTable  # Import the DB class
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def time_to_minutes(time_str):
    total_minutes = 0
    parts = time_str.split()
    for part in parts:
        if 'h' in part:
            total_minutes += int(part.strip('h')) * 60
        elif 'm' in part:
            total_minutes += int(part.strip('m'))
    return total_minutes


def scrape_imdb_data(url):
    # Setup Chrome WebDriver
    driver_service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=driver_service)

    try:
        # Navigate to the IMDb movie chart page
        driver.get(url)
        time.sleep(5)  # Add a short delay to ensure page loads completely (adjust as needed)

        # Find elements for movie details
        movie_titles = driver.find_elements(By.XPATH, '//*[@id="__next"]/main/div/div[3]/section/div/div[2]/div/ul/li/div[2]/div/div/div[2]/a/h3')
        movie_years = driver.find_elements(By.XPATH, '//*[@id="__next"]/main/div/div[3]/section/div/div[2]/div/ul/li/div[2]/div/div/div[3]/span[1]')
        movie_lengths = driver.find_elements(By.XPATH, '//*[@id="__next"]/main/div/div[3]/section/div/div[2]/div/ul/li/div[2]/div/div/div[3]/span[2]')
        movie_rates = driver.find_elements(By.XPATH, '//*[@id="__next"]/main/div/div[3]/section/div/div[2]/div/ul/li/div[2]/div/div/span/div/span')
        movie_group = driver.find_elements(By.XPATH, '//*[@id="__next"]/main/div/div[3]/section/div/div[2]/div/ul/li/div[2]/div/div/div[3]/span[3]')

        # Ensure lists are of the same length
        min_length = min(len(movie_titles), len(movie_years), len(movie_rates), len(movie_lengths), len(movie_group))

        data = []
        for i in range(min_length):
            title = movie_titles[i].text
            year = movie_years[i].text
            rate_text = movie_rates[i].text.split('\n')[0]
            length = time_to_minutes(movie_lengths[i].text)
            group = movie_group[i].text.strip()

            data.append([title, year, rate_text, length, group])

        # Convert list to DataFrame for easier manipulation
        df = pd.DataFrame(data, columns=['Title', 'Year', 'Rating', 'Duration_minutes', 'Group_Category'])

        logger.info("DataFrame created with %d rows", len(df))
        logger.info(df.head())

        return df

    except Exception as e:
        logger.error(f"Error during scraping: {e}")
        return None

    finally:
        # Quit the WebDriver session
        driver.quit()

def main():
    url = 'https://www.imdb.com/chart/moviemeter/?ref_=nv_mv_mpm'
    imdb_data = scrape_imdb_data(url)

    # Initialize the database table class
    IMDB_table = IMDBDBTable()

    if imdb_data is not None and not imdb_data.empty:
        try:
            # Insert the data into the database
            IMDB_table.insert_data(imdb_data)
        except Exception as e:
            logger.error(f"Error inserting data into database: {e}")
    else:
        logger.warning("No data to insert into the database.")

    try:
        # Retrieve and print all data from the database to verify
        result = IMDB_table.select_all()
        logger.info("Data retrieved from database:")
        for row in result:
            logger.info(row)
    except Exception as e:
        logger.error(f"Error retrieving data from database: {e}")

if __name__ == "__main__":
    main()
