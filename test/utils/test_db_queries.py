import os
import sys
sys.path.append('../..')
import pytest
import pandas as pd
import numpy as np
from src.utils.db_queries import select_from_daily_weather_data, get_connection, select_from_hourly_weather_data, \
    insert_into_weather_daily_data, insert_into_weather_hourly_data

BASE_PATH = os.path.abspath(os.path.dirname(__file__))


@pytest.fixture
def weather_data_sqlite(tmpdir, scope='session'):
    db_name = 'weather_data.db'
    conn = get_connection(tmpdir, db_name)
    cursor = conn.cursor()
    sql_query = open(f'{BASE_PATH}/../../src/sql_queries/create_table_weather_daily_data.sql').read()
    cursor.execute(sql_query)

    test_data = [
        ('2022-01-10', 70.3, 30.1, 'Rain', 30.1),
        ('2022-01-10', 80.3, 30.1, 'Rain', 30.1),
        ('2022-01-11', 70.3, 30.1, 'Rain', 30.1),
        ('2022-01-12', 70.3, 30.1, 'Rain', 30.1)
    ]

    cursor.executemany('INSERT INTO weather_daily_data VALUES(?, ?, ?, ?, ?)', test_data)

    sql_query = open(f'{BASE_PATH}/../../src/sql_queries/create_table_weather_hourly_data.sql').read()
    cursor.execute(sql_query)

    test_data = [
        ('2022-01-10', '00:00:00', 70.3, 30.1, 'Rain', 30.1),
        ('2022-01-10', '20:00:00', 80.3, 30.1, 'Rain', 30.1),
        ('2022-01-10', '21:00:00', 70.3, 30.1, 'Rain', 30.1),
        ('2022-01-12', '20:00:00', 70.3, 30.1, 'Rain', 30.1)
    ]

    cursor.executemany('INSERT INTO weather_hourly_data VALUES(?, ?, ?, ?, ?, ?)', test_data)

    yield conn


def test_select_from_daily_weather_data_some_rows(weather_data_sqlite):
    # Third row doesn't exist in the db
    dates = ['2022-01-10', '2022-01-11', '2021-01-11']
    #locations = ['Valdivia', 'Santiago', 'Valdivia']
    latitudes = [70.3, 70.3, 0]
    longitudes = [30.1, 30.1, 0]

    db_name = 'weather_data'
    conn = weather_data_sqlite
    df = select_from_daily_weather_data(conn, dates, latitudes, longitudes)

    assert len(df) == 2


def test_select_from_hourly_weather_data_some_rows(weather_data_sqlite):
    # Third row doesn't exist in the db
    dates = ['2022-01-10', '2022-01-12', '2021-01-11']
    hours = ['00:00:00', '20:00:00', '21:00:00']
    #locations = ['Valdivia', 'Santiago', 'Valdivia']
    latitudes = [70.3, 70.3, 0]
    longitudes = [30.1, 30.1, 0]

    db_name = 'weather_data'
    conn = weather_data_sqlite
    df = select_from_hourly_weather_data(conn, dates, latitudes, longitudes)

    assert len(df) == 3


def test_insert_into_weather_daily_data(weather_data_sqlite):
    insert_data = [
        ('2022-01-13', 70.3, 30.1, 30.1, 'Rain'),
        ('2022-01-15', 80.3, 30.1, 30.1, 'Rain'),
    ]
    insert_df = pd.DataFrame(insert_data, columns=['date', 'latitude', 'longitude', 'temp', 'conditions'])
    conn = weather_data_sqlite
    insert_into_weather_daily_data(conn, insert_df)

    db_data = conn.execute('SELECT * FROM weather_daily_data').fetchall()

    assert len(db_data) == 6


def test_insert_into_weather_hourly_data(weather_data_sqlite):
    insert_data = [
        ('2022-01-13', '00:00:00', 70.3, 30.1, 30.1, 'Rain'),
        ('2022-01-15', '01:00:00', 80.3, 30.1, 30.1, 'Rain'),
    ]
    insert_df = pd.DataFrame(insert_data, columns=['date', 'hour', 'latitude', 'longitude', 'temp', 'conditions'])
    conn = weather_data_sqlite
    insert_into_weather_hourly_data(conn, insert_df)

    db_data = conn.execute('SELECT * FROM weather_hourly_data').fetchall()

    assert len(db_data) == 6





