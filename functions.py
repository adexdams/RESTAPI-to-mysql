import requests
from datetime import datetime
import pandas as pd
import json

# This function will EXTRACT contents from the API link
def get_top_scorers(url, headers, params):
    """
    Fetch the top scorers using the API
    """
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        return data

    except requests.exceptions.HTTPError as http_error_message:
        print (f"❌ [HTTP ERROR]: {http_error_message}")

    except requests.exceptions.ConnectionError as connection_error_message:
        print (f"❌ [CONNECTION ERROR]: {connection_error_message}")

    except requests.exceptions.Timeout as timeout_error_message:
        print (f"❌ [TIMEOUT ERROR]: {timeout_error_message}")

    except requests.exceptions.RequestException as other_error_message:
        print (f"❌ [UNKNOWN ERROR]: {other_error_message}")


# This function will TRANSFORM the output of the API to a list
def process_top_scorers(data):
    """
    Parse the JSON data required for the top scorers
    :param data:
    :return:
    """

    top_scorers = []
    for scorer_data in data['response']:
        statistics = scorer_data['statistics'][0]

        # Set up constants for processing data
        player = scorer_data['player']
        player_name = player['name']
        club_name = statistics['team']['name']
        total_goals = int(statistics['goals']['total'])
        penalty_goals = int(statistics['penalty']['scored'])
        assists = int(statistics['goals']['assists']) if statistics['goals']['assists'] else 0
        matches_played = int(statistics['games']['appearences'])
        minutes_played = int(statistics['games']['minutes'])
        dob = datetime.strptime(player['birth']['date'], '%Y-%m-%d')
        age = (datetime.now() - dob).days// 365

        #Append data
        top_scorers.append({
            'player': player_name,
            'club': club_name,
            'total_goals': total_goals,
            'penalty_goals': penalty_goals,
            'assists': assists,
            'matches': matches_played,
            'mins': minutes_played,
            'age': age
        })
    return top_scorers


# This function will LOAD the output from the last function into a dataframe
def create_dataframe(top_scorers):
    """
    Convert list of dictionaries into a Pandas dataframe and process it.
    :param top_scorers:
    :return:
    """
    df = pd.DataFrame(top_scorers)

    # Sort dataframe first by 'total_gaols' in desc order, then by 'assists' in desc order
    df.sort_values(by=['total_goals', 'assists'], ascending=[False, False], inplace=True)

    # Reset index after sorting to reflect new order
    df.reset_index(drop=True, inplace=True)

    # Recalculate the ranks based on the sorted order
    df['position'] = df['total_goals'].rank(method='dense', ascending=False).astype(int)

    # Specify the columns to include in the final dataframe in the desired order
    df = df[['position', 'player', 'club', 'total_goals', 'penalty_goals', 'assists', 'matches', 'mins', 'age']]

    return df