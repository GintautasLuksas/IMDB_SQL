import os
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import logging

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#### 1. Prisijungimas prie DB, kaip atskira klasė
class DBEngine:
    def __init__(self):
        self.connection = None
        self.cursor = None
        self.connect()

    def connect(self):
        """Establish a connection to the PostgreSQL database."""
        try:
            self.connection = psycopg2.connect(
                dbname=os.getenv('DB_NAME'),
                user=os.getenv('DB_USERNAME'),
                password=os.getenv('DB_PASSWORD'),
                host=os.getenv('HOST'),
                port=os.getenv('PORT')
            )
            self.cursor = self.connection.cursor()
            logger.info('Database connection established.')
        except (Exception, psycopg2.Error) as error:
            logger.error(f"Error connecting to the database: {error}")
            raise

    def __del__(self):
        """Close the database connection and cursor."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info('Database connection closed.')

    def check_table_schema(self, table_name):
        """Print the schema of a given table."""
        try:
            query = """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = %s;
            """
            self.cursor.execute(query, (table_name,))
            columns = self.cursor.fetchall()
            for column in columns:
                logger.info(f"Column: {column[0]}, Type: {column[1]}")
        except psycopg2.Error as e:
            logger.error(f"Error retrieving table schema: {e}")

### 2. Klasės kūrimas kiekvienai lentelei
class IMDBDBTable:
    table_name = "IMDB"
    columns = ('Title', 'Year', 'Rating', 'Duration_minutes', 'Group_Category')

    def __init__(self):
        self.db_connection = DBEngine()
        self.create_table()

### 3. Duomenų bazės lentelių kūrimas su Python
    def create_table(self):
        """Create the IMDB table if it doesn't exist."""
        try:
            logger.info('Creating table...')
            create_query = f"""
               CREATE TABLE IF NOT EXISTS "{self.table_name}" (
                   "id" SERIAL PRIMARY KEY,
                   "Title" VARCHAR(255),
                   "Year" VARCHAR(4),
                   "Rating" VARCHAR(255),
                   "Duration_minutes" INT,
                   "Group_Category" VARCHAR(10),
                   UNIQUE ("Title", "Year")
               )
               """
            self.db_connection.cursor.execute(create_query)
            self.db_connection.connection.commit()
            logger.info('Table IMDB created or already exists!')
        except psycopg2.Error as e:
            logger.error(f"Error creating table: {e}")
            self.db_connection.connection.rollback()

### 4. Duomenų įrašymas į lenteles
    def insert_data(self, df):
        """Insert data into the IMDB table."""
        if not self.check_table_exists():
            logger.error("Table does not exist, cannot insert data.")
            return

        columns = sql.SQL(', ').join(map(sql.Identifier, self.columns))
        values = sql.SQL(', ').join(sql.Placeholder() * len(self.columns))
        table_name = sql.Identifier(self.table_name)
###5. Dublikatų panaikinimas. ON CONFLICT DO NOTHING.
        query = sql.SQL("INSERT INTO {} ({}) VALUES ({}) ON CONFLICT (\"Title\", \"Year\") DO NOTHING").format(
            table_name, columns, values)

        # Convert DataFrame rows to tuples
        data = [tuple(row) for row in df.to_numpy()]
        try:
            with self.db_connection.connection.cursor() as cursor:
                cursor.executemany(query, data)
                self.db_connection.connection.commit()
            logger.info('Data inserted successfully!')
        except psycopg2.Error as e:
            logger.error(f"Error inserting data: {e}")
            self.db_connection.connection.rollback()

    def check_table_exists(self):
        """Check if the table exists in the database."""
        try:
            query = f"SELECT to_regclass('public.\"{self.table_name}\"')"
            self.db_connection.cursor.execute(query)
            result = self.db_connection.cursor.fetchone()
            return result[0] is not None
        except psycopg2.Error as e:
            logger.error(f"Error checking table existence: {e}")
            return False

    def select_all(self):
        """Retrieve all data from the IMDB table."""
        query = f"SELECT DISTINCT * FROM \"{self.table_name}\""
        try:
            self.db_connection.cursor.execute(query)
            return self.db_connection.cursor.fetchall()
        except psycopg2.Error as e:
            logger.error(f"Error retrieving data: {e}")
            return []
### 7. Duomenų atnaujinimas duomenų bazėje per python naudojant UPDATE komandą.
    def update_data(self, title, year, new_values):
        """Update data in the IMDB table based on title and year."""
        try:
            set_clause = ', '.join([f"{key} = %s" for key in new_values.keys()])
            query = sql.SQL(f"UPDATE {self.table_name} SET {set_clause} WHERE \"Title\" = %s AND \"Year\" = %s")
            self.db_connection.cursor.execute(query, (*new_values.values(), title, year))
            self.db_connection.connection.commit()
            logger.info('Data updated successfully!')
        except psycopg2.Error as e:
            logger.error(f"Error updating data: {e}")
            self.db_connection.connection.rollback()
###8. Duomenų pašalinimas duomenų bazėje per python naudojant DELETE komandą
    def delete_data(self, title, year):
        """Delete data from the IMDB table based on title and year."""
        try:
            query = sql.SQL(f"DELETE FROM {self.table_name} WHERE \"Title\" = %s AND \"Year\" = %s")
            self.db_connection.cursor.execute(query, (title, year))
            self.db_connection.connection.commit()
            logger.info('Data deleted successfully!')
        except psycopg2.Error as e:
            logger.error(f"Error deleting data: {e}")
            self.db_connection.connection.rollback()
###9. Lentelių trynimas iš duomenų bazės per python naudojant DROP komandą
    def drop_table(self):
        """Drop the IMDB table."""
        try:
            query = sql.SQL(f"DROP TABLE IF EXISTS {self.table_name}")
            self.db_connection.cursor.execute(query)
            self.db_connection.connection.commit()
            logger.info('Table dropped successfully!')
        except psycopg2.Error as e:
            logger.error(f"Error dropping table: {e}")
            self.db_connection.connection.rollback()
