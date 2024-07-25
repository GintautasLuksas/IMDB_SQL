# main.py

import pandas as pd
from db_connection import IMDBDBTable, DBEngine
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_sample_dataframe():
    """Create a sample DataFrame with movie data."""
    data = {
        'Title': ['Movie 1', 'Movie 2', 'Movie 3'],
        'Year': ['2020', '2021', '2022'],
        'Rating': ['8.5', '7.4', '9.0'],
        'Duration_minutes': [120, 150, 90],
        'Group_Category': ['Action', 'Drama', 'Comedy']
    }
    return pd.DataFrame(data)

def main():
    # Create a sample DataFrame
    df = create_sample_dataframe()

    # Initialize the IMDBDBTable class
    imdb_table = IMDBDBTable()

    # Insert sample data into the database
    imdb_table.insert_data(df)

    # Retrieve and print all data from the database
    data = imdb_table.select_all()
    for row in data:
        logger.info(row)

    # 7. Duomenų atnaujinimas duomenų bazėje per python naudojant UPDATE komandą.
    imdb_table.update_data('Movie 1', '2020', {'Rating': '9.1', 'Duration_minutes': 125})

    # 8. Duomenų pašalinimas duomenų bazėje per python naudojant DELETE komandą
    imdb_table.delete_data('Movie 2', '2021')

    # 9. Lentelių trynimas iš duomenų bazės per pyhton naudojant DROP komandą.
    imdb_table.drop_table()

if __name__ == "__main__":
    main()
