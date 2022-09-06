import os
import sqlite3
import pandas as pd
from sqlalchemy import create_engine


BASE_PATH = os.path.abspath(os.path.dirname(__file__))


def get_connection(path, db_name):
    """
    Gets the connector object connected to the sqlite DB

    Parameters
    ----------
    :param path: DB folder path
    :param db_name: DB name
    :return: sqlite3 connector object connected to the DB
    """
    conn = sqlite3.connect(f'{path}{db_name}')
    return conn


def select_from_daily_weather_data(conn, dates, latitudes, longitudes):
    """
    Selects daily weather data from the DB

    The DB is queried for entries with key values dates[i], latitudes[i], longitudes[i] in the
    weather_daily_data table. The key values are concatenated as strings to search more easily multiple values
    in the DB. Returns a DataFrame with the entries found.

    Parameters
    ----------
    :param connector conn: sqlite connector to the DB
    :param list string dates: list of dates in 'YYYY-mm-dd' string format for each location
    :param list float latitudes: list of latitude values for each location
    :param list float longitudes: list of longitude values for each location
    :return: DataFrame(date, latitude, longitude, conditions, temperature) with all the entries found
    """
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


def select_from_hourly_weather_data(conn, dates, latitudes, longitudes):
    """
    Selects daily weather data from the DB

    The DB is queried for entries with key values dates[i], latitudes[i], longitudes[i] in the
    weather_hourly_data table. The key values are concatenated as strings to search more easily multiple values
    in the DB. In this case, multiple entries might be returned for each key values combination, as we query
    using the date information only, and not the specific hour value.  Returns a DataFrame with the entries found.

    Parameters
    ----------
    :param connector conn: sqlite connector to the DB
    :param list string dates: list of dates in 'YYYY-mm-dd' string format for each location
    :param list float latitudes: list of latitude values for each location
    :param list float longitudes: list of longitude values for each location
    :return: DataFrame(date, latitude, longitude, conditions, temperature) with all the entries found
    """
    # Create a list of single string key values combining all parameters
    string_keys = [f'{date}{latitude}{longitude}'
                   for date, latitude, longitude in zip(dates,  latitudes, longitudes)]

    cursor = conn.cursor()
    query = """
            SELECT * 
            FROM weather_hourly_data
            WHERE date || latitude || longitude IN ({seq})
            """.format(seq=','.join(['?']*len(string_keys)))

    res = cursor.execute(query, string_keys)

    columns = list(map(lambda x: x[0], cursor.description))
    data = res.fetchall()

    return pd.DataFrame(data, columns=columns)


def insert_into_weather_daily_data(conn, df):
    """
    Inserts weather daily data coming from the API in the DB

    Parameters
    ----------
    :param sqlite connector conn: sqlite DB connector
    :param DataFrame df: data with the following columns in order [date, latitude, longitude, conditions, temperature ]
    :return:
    """
    cursor = conn.cursor()
    cursor.executemany('INSERT INTO weather_daily_data VALUES(?, ?, ?, ?, ?)', df.values)
    conn.commit()


def insert_into_weather_hourly_data(conn, df):
    """
    Inserts weather hourly data coming from the API in the DB

    Parameters
    ----------
    :param sqlite connector conn: sqlite DB connector
    :param DataFrame df: data with the following columns in order [date, hour, latitude, longitude, conditions, temperature ]
    :return:
    """
    cursor = conn.cursor()
    cursor.executemany('INSERT INTO weather_hourly_data VALUES(?, ?, ?, ?, ?, ?)', df.values)
    conn.commit()



