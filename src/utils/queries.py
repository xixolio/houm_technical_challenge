import os
import sys

sys.path.append('../..')
import pandas as pd
import numpy as np

from src.utils.api_queries import query_weather_api_multiple_times
from src.utils.db_queries import select_from_daily_weather_data, select_from_hourly_weather_data, get_connection

BASE_PATH = os.path.abspath(os.path.dirname(__file__))


def get_data_from_db_or_api(conn, dates, latitudes, longitudes, columns, include='days'):
    if include == 'days':
        key_columns = ['date', 'latitude', 'longitude']
        db_df = select_from_daily_weather_data(conn, dates, latitudes, longitudes)
        print('DB DF ACA')
        print(db_df)
        # Look for values not returned by the db dataframe using a key_string created from all the
        # key columns
        key_strings = [f'{date}{latitude}{longitude}'
                       for date, latitude, longitude in
                       zip(dates, latitudes, longitudes)]

        db_key_strings = db_df[key_columns].astype(str).sum(1).values

        # Select all locations, dates, latitudes and longitudes not found in db.
        # Remember that in the API locations is latitudes,longitudes
        keys_not_present_in_db_index = [key in db_key_strings for key in key_strings]
        dates = list(np.array(dates)[keys_not_present_in_db_index])
        latitudes = list(np.array(latitudes)[keys_not_present_in_db_index])
        longitudes = list(np.array(longitudes)[keys_not_present_in_db_index])
        locations = [f'{latitude},{longitude}, '
                     for latitude, longitude in zip(latitudes, longitudes)]

        # Search remaining in the API
        api_df = query_weather_api_multiple_times(locations, start_dates=dates, include=include, verbose=True)

        # Return the final df with only the relevant columns to the problem
        return pd.concat([api_df[columns], db_df[columns]])

