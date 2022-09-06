import os
import sys

sys.path.append('../..')
import pandas as pd
import numpy as np

from src.utils.api_queries import query_weather_api_multiple_times
from src.utils.db_queries import select_from_daily_weather_data, select_from_hourly_weather_data, get_connection,\
    insert_into_weather_daily_data, insert_into_weather_hourly_data

BASE_PATH = os.path.abspath(os.path.dirname(__file__))


def get_data_from_db_or_api(conn, dates, latitudes, longitudes, columns, include='days', hours=[], verbose=False):
    """
    Get weather API data either from the DB or the API

    Given the list of parameters dates, latitudes and longitudes, searches first if the data has been stored
    in the DB. If not it is instead retreived from the API. When retrieving hourly data (include='hours') the
    search in the DB is conducted using the date, and not the time, since the API retreives 24 hours batches
    anytime a date is requested, thus, each time a new date-location pair is requested, the DB also stores
     24 hours of data corresponding to that date-location pair. Even though the list of dates and hours is
     asked in string format, the returned dates in the DataFrame are all in datetime format. If the DB has
     all the required information, that dataframe is returned without querying the API. Otherwise, the
     API is queried and the DB dataframe and API dataframe are concatenated and returned, after selecting the
     import columns from the API dataframe.

    Parameters
    ----------
    :param connector conn: sqlite connector to the DB
    :param list string dates: list of dates in 'YYYY-mm-dd' string format for each location
    :param list float latitudes: list of latitude values for each location
    :param list float longitudes: list of longitude values for each location
    :param list string columns: list of relevant columns to consider coming from the API data
    :param str include: either 'days', if we want daily information for each date, or 'hours', if we want
                hourly data for each date.
    :param list string hours: (optional) if include = 'hours', then a list of hours in 'HH:MM:ss' string format
                have to be provided.
    :return: DataFrame with the results from querying the weather data
    """
    if include == 'days':
        key_columns = ['date', 'latitude', 'longitude']
        db_df = select_from_daily_weather_data(conn, dates, latitudes, longitudes)
        db_df['date'] = pd.to_datetime(db_df['date'])

        # Look for values not returned by the db dataframe using a key_string created from all the
        # key columns
        key_strings = [f'{date}{latitude}{longitude}'
                       for date, latitude, longitude in
                       zip(dates, latitudes, longitudes)]

        db_key_strings = db_df[key_columns].astype(str).sum(1).values

        # Select all locations, dates, latitudes and longitudes not found in db.
        # Remember that in the API locations is latitudes,longitudes
        keys_not_present_in_db_index = [key not in db_key_strings for key in key_strings]

        if verbose:
            print("Found", len(key_strings) - sum(keys_not_present_in_db_index), "out of ", len(key_strings),
                  " entries in the DB.")

        dates = list(np.array(dates)[keys_not_present_in_db_index])
        latitudes = list(np.array(latitudes)[keys_not_present_in_db_index])
        longitudes = list(np.array(longitudes)[keys_not_present_in_db_index])
        locations = [f'{latitude},{longitude}'
                     for latitude, longitude in zip(latitudes, longitudes)]

        # Search remaining in the API and save to the db if any
        if len(locations) > 0:
            api_df = query_weather_api_multiple_times(locations, start_dates=dates, include=include, verbose=verbose)
            api_df = api_df[columns]

            # To save in the db, we create a str_date column
            api_df['str_date'] = api_df['date'].astype(str)
            columns_to_save = [col for col in columns if col != 'date']
            columns_to_save = ['str_date'] + columns_to_save

            insert_into_weather_daily_data(conn, api_df[columns_to_save])

            # Return the final df with only the relevant columns to the problem
            return pd.concat([api_df, db_df[columns]])

        else:
            return db_df[columns]

    elif include == 'hours':
        key_columns = ['date', 'hour', 'latitude', 'longitude']
        db_df = select_from_hourly_weather_data(conn, dates, latitudes, longitudes)

        # Look for values not returned by the db dataframe using a key_string created from all the
        # key columns
        key_strings = [f'{date}{hour}{latitude}{longitude}'
                       for date, hour, latitude, longitude in
                       zip(dates, hours, latitudes, longitudes)]

        db_key_strings = db_df[key_columns].astype(str).sum(1).values

        # Select all locations, dates, hours, latitudes and longitudes not found in db.
        # Remember that in the API locations is latitudes,longitudes
        keys_not_present_in_db_index = [key not in db_key_strings for key in key_strings]

        if verbose:
            print("Found", len(key_strings) - sum(keys_not_present_in_db_index), "out of ", len(key_strings),
                  " entries in the DB.")

        dates = list(np.array(dates)[keys_not_present_in_db_index])
        latitudes = list(np.array(latitudes)[keys_not_present_in_db_index])
        longitudes = list(np.array(longitudes)[keys_not_present_in_db_index])
        locations = [f'{latitude},{longitude}'
                     for latitude, longitude in zip(latitudes, longitudes)]

        # Remember to recover the whole datetime from the db data and convert to UTC to avoid problems
        db_df['date'] = pd.to_datetime(db_df['date'].astype('str') + ' ' + db_df['hour'].astype('str'))

        # Search remaining in the API and save to the db, if any
        if len(locations) > 0:
            api_df = query_weather_api_multiple_times(locations, start_dates=dates, include=include, verbose=verbose)
            api_df = api_df[columns]

            # To save the data in the DB, we split date and hour
            api_df['date_only'] = api_df['date'].dt.date.astype(str)
            api_df['hour'] = api_df['date'].dt.time.astype(str)

            columns_to_save = [col for col in columns if col != 'date']
            columns_to_save = ['date_only', 'hour'] + columns_to_save

            insert_into_weather_hourly_data(conn, api_df[columns_to_save])

            return pd.concat([api_df, db_df[columns]])

        else:
            return db_df[columns]




