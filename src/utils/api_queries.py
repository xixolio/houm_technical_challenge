"""
Module that deals with queries to the Visualcrossing Weather API.
For more information about the API, refer to https://www.visualcrossing.com/
"""

import os
import codecs
import urllib.request
import urllib.error
import pandas as pd
from src.utils.config_utils import read_yaml

BASE_PATH = os.path.abspath(os.path.dirname(__file__))

CONFIG = read_yaml(f'{BASE_PATH}/../config.yaml')
API_KEY = CONFIG['API']['API_KEY']
BASE_URL = CONFIG['API']['BASE_URL']


def query_weather_api(location, start_date, end_date=None, include='days', verbose=False):
    """
    Queries the Visualcrossing Weather API for weather information

    Takes a location, date, and whether you want daily summary or hourly data and returns the weather
    data associated to it.

    Parameters
    ----------
    :param str location: 'latitude,longitud' string
    :param str start_date: date to query in 'yyyy-mm-dd' format
    :param str end_date: not used
    :param str include: days or hours, whether to return a daily summary or hourly information from the date
    :param bool verbose: whether to print the query url or not

    :return: DataFrame(name, datetime, temp, conditions,...): df with response
    """
    api_query = f'{BASE_URL}{location}/{start_date}'

    if end_date is not None:
        api_query += f'/{end_date}'

    # Adding query parameters and api key
    api_query += f'?&contentType=csv&include={include}&unitGroup=metric&timezone=Z&key={API_KEY}'

    if verbose:
        print(' - Running query URL: ', api_query)

    try:
        response = urllib.request.urlopen(api_query)
        response_df = pd.read_csv(response)
        return response_df

    except urllib.error.HTTPError as e:
        error_info = e.read().decode()
        print('Error code: ', e.code, error_info)
        raise e

    except urllib.error.URLError as e:
        error_info = e.read().decode()
        print('Error code: ', e.code, error_info)
        raise e


def query_weather_api_multiple_times(locations, start_dates, end_dates=None, include='days', verbose=False):
    """
    Queries the Visual Crossing weather api for each tuple of values

    Receives a list of locations and start_dates and for each pair calls query_weather_api, concatenating
    the results in a DataFrame and reformating its columns.

    Parameters
    ----------
    :param list str locations: list of 'latitude,longitud' strings
    :param list str start_dates: list of dates
    :param list end_dates: unused
    :param str include:  days or hours, whether to return a daily summary or hourly information from the date
    :param bool verbose: whether to print the query url or not
    :return: DataFrame(date, {hour}, latitude, longitude, temp, conditions): df with concatenated responses.
        If include == 'hours' the df has the hour column as well.
    """
    if len(locations) != len(start_dates):
        raise Exception("locations and start_dates should have same number of elements.")

    if end_dates is not None:
        if len(end_dates) != len(start_dates):
            raise Exception("start_dates and end_dates should have same number of elements.")

    # Eliminate duplicate locations, start_dates pairs if present
    df = pd.DataFrame({'locations': locations,
                       'start_dates': start_dates})
    df = df.drop_duplicates()

    locations = df['locations'].values
    start_dates = df['start_dates'].values

    response_df_list = []

    if end_dates is None:
        end_dates = [None for i in range(len(locations))]

    for location, start_date, end_date in zip(locations, start_dates, end_dates):
        response_df = query_weather_api(location, start_date, end_date=end_date, include=include,
                                        verbose=verbose)
        response_df_list.append(response_df)

    api_df = pd.concat(response_df_list)

    # The api column "name" returns the latitude,longitud pair that we need for the actual dataframe.
    api_df['latitude'] = api_df['name'].apply(lambda x: float(x.split(',')[0]))
    api_df['longitude'] = api_df['name'].apply(lambda x: float(x.split(',')[1]))
    api_df = api_df.rename(columns={'datetime': 'date', 'temp': 'temperature'})
    api_df['date'] = pd.to_datetime(api_df['date'])

    #if include == 'hours':
    #    # Extract the time information from date
    #    api_df['hour'] = api_df['date'].dt.time
    #    api_df['date'] = api_df['date'].dt.date

    return api_df




