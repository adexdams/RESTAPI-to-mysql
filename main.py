import os
from dotenv import load_dotenv
from functions import get_top_scorers, process_top_scorers, create_dataframe
from db.db_connection import create_table, insert_into_table, connect_to_db


# Load the environment variables from .env file
load_dotenv()

# Load the API Key to make API requests
RAPIDAPI_KEY = os.getenv('RAPIDAPI_KEY')

# Setup API request headers to authenticate requests
headers = {
    "x-rapidapi-host": "api-football-v1.p.rapidapi.com",
	"x-rapidapi-key": RAPIDAPI_KEY
}

# Setup API URL and parameters
url = "https://api-football-v1.p.rapidapi.com/v3/players/topscorers"
params = {"league":"39",
          "season": "2024"}


# Load the ETL pipeline
def run_data_pipeline():
    """
    Execute the ETL pipeline
    """
    #check_rate_limits()

    data = get_top_scorers(url, headers, params)

    if data and 'response' in data and data['response']:
        top_scorers = process_top_scorers(data)
        df = create_dataframe(top_scorers)
        print(df.to_string(index=False))

    else:
        print("No data available or an error occurred ❌")

    db_connection = connect_to_db()


    # If connection is successful, proceed with creating table and inserting data
    if db_connection is not None:
        create_table(db_connection)
        insert_into_table(db_connection, df)

if __name__ == "__main__":
    run_data_pipeline()



