import os
import sqlite3
import pandas as pd

BASE_PATH = os.path.abspath(os.path.dirname(__file__))


def get_connection(path, db_name):
    conn = sqlite3.connect(f'{path}{db_name}')
    return conn


def select_from_weather_data(conn, dates, locations, latitudes, longitudes):
    # Create a list of single string key values combining all parameters
    string_keys = [f'{date}{location}{latitude}{longitude}'
                   for date, location, latitude, longitude in zip(dates, locations, latitudes, longitudes)]

    cursor = conn.cursor()
    query = """
            SELECT * 
            FROM weather_daily_data
            WHERE date || location || latitude || longitude IN ({seq})
            """.format(seq=','.join(['?']*len(string_keys)))

    res = cursor.execute(query, string_keys)

    columns = list(map(lambda x: x[0], cursor.description))
    data = res.fetchall()

    return pd.DataFrame(data, columns=columns)


