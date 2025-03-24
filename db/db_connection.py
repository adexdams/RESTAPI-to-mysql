import os
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

# Load the environment variables from .env file
load_dotenv()

# Load MySQL connection settings
HOST = os.getenv('HOST')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE')
MYSQL_USERNAME = os.getenv('MYSQL_USERNAME')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')


# This function will create a connection with the database
def connect_to_db():
    try:
        connection = mysql.connector.connect(
            host=HOST,
            user=MYSQL_USERNAME,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )
        if connection.is_connected():
            print("✅ Connected to MySQL")
            return connection
    except mysql.connector.Error as e:
        print(f"❌ Error connecting to MySQL: {e}")
        return None


# This function will create a table inside the database
def create_table(db_connection):
    """
    Create a table if it does not exist in the MySQL database
    """
    CREATE_TABLE_SQL_QUERY = """
    CREATE TABLE IF NOT EXISTS top_scorers (
        `position` INT,
        `player` VARCHAR(255),
        `club` VARCHAR(255),
        `total_goals` INT,
        `penalty_goals` INT,
        `assists` INT,
        `matches` INT,
        `mins` INT,
        `age` INT,
        PRIMARY KEY (`player`, `club`)
    );
    """
    try:
        cursor = db_connection.cursor()
        cursor.execute(CREATE_TABLE_SQL_QUERY)
        db_connection.commit()
        print("Table created successfully ✅")

    except Exception as e:
        print(f"❌ [CREATING TABLE ERROR]: '{e}'")


# This function will insert the contents of the dataframe into the table
def insert_into_table(db_connection, df):
    """
    Insert or update the top scorers data in the database from the dataframe
    """
    cursor = db_connection.cursor()

    INSERT_DATA_SQL_QUERY = """
    INSERT INTO top_scorers (`position`, `player`, `club`, `total_goals`, `penalty_goals`, `assists`, `matches`, `mins`, `age`)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        `total_goals` = VALUES(`total_goals`),
        `penalty_goals` = VALUES(`penalty_goals`),
        `assists` = VALUES(`assists`),
        `matches` = VALUES(`matches`),
        `mins` = VALUES(`mins`),
        `age` = VALUES(`age`)
    """
    # Create a list of tuples from the dataframe values
    data_values_as_tuples = [tuple(x) for x in df.to_numpy()]

    # Execute the query
    cursor.executemany(INSERT_DATA_SQL_QUERY, data_values_as_tuples)
    db_connection.commit()
    print("Data inserted or updated successfully ✅")
