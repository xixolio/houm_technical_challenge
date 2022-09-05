import os
import sqlite3
import pandas as pd
from sqlalchemy import create_engine


BASE_PATH = os.path.abspath(os.path.dirname(__file__))


def get_connection(path, db_name):
    conn = sqlite3.connect(f'{path}{db_name}')
    return conn


#def get_connection(path, db_name):
#    engine = create_engine(f'sqlite:///{path}{db_name}', echo=False)
#    return engine.connect()


def select_from_daily_weather_data(conn, dates, latitudes, longitudes):
    # Create a list of single string key values combining all parameters
    string_keys = [f'{date}{latitude}{longitude}'
                   for date, latitude, longitude in zip(dates, latitudes, longitudes)]

    cursor = conn.cursor()
    query = """
            SELECT * 
            FROM weather_daily_data
            WHERE date || latitude || longitude IN ({seq})
            """.format(seq=','.join(['?']*len(string_keys)))

    res = cursor.execute(query, string_keys)

    columns = list(map(lambda x: x[0], cursor.description))
    data = res.fetchall()

    return pd.DataFrame(data, columns=columns)


def select_from_hourly_weather_data(conn, dates, hours, latitudes, longitudes):
    # Create a list of single string key values combining all parameters
    string_keys = [f'{date}{hour}{latitude}{longitude}'
                   for date, hour, latitude, longitude in zip(dates, hours, latitudes, longitudes)]

    cursor = conn.cursor()
    query = """
            SELECT * 
            FROM weather_hourly_data
            WHERE date || hour || latitude || longitude IN ({seq})
            """.format(seq=','.join(['?']*len(string_keys)))

    res = cursor.execute(query, string_keys)

    columns = list(map(lambda x: x[0], cursor.description))
    data = res.fetchall()

    return pd.DataFrame(data, columns=columns)


def insert_into_weather_daily_data(conn, df):
    cursor = conn.cursor()
    cursor.executemany('INSERT INTO weather_daily_data VALUES(?, ?, ?, ?, ?)', df.values)


def insert_into_weather_hourly_data(conn, df):
    cursor = conn.cursor()
    cursor.executemany('INSERT INTO weather_hourly_data VALUES(?, ?, ?, ?, ?, ?)', df.values)



