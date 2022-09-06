import os
import sqlite3

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
    if not os.path.exists(f'{path}{db_name}'):
        conn = sqlite3.connect(f'{path}{db_name}')
        query_1 = open(f'{BASE_PATH}/../sql_queries/create_table_weather_daily_data.sql').read()
        query_2 = open(f'{BASE_PATH}/../sql_queries/create_table_weather_hourly_data.sql').read()

        conn.cursor().execute(query_1)
        conn.cursor().execute(query_2)

    else:
        conn = sqlite3.connect(f'{path}{db_name}')

    return conn
