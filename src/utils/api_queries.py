import codecs
import urllib.request
import urllib.error
import pandas as pd

# TODO: move to config file later
BASE_URL = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/'
API_KEY = 'NQJUZHVSAXKBCBZAC7AVE8ZCC'


def query_weather_api(location, start_date, end_date=None, include='days', verbose=False):
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
    if len(locations) != len(start_dates):
        raise Exception("locations and start_dates should have same number of elements.")

    if end_dates is not None:
        if len(end_dates) != len(start_dates):
            raise Exception("start_dates and end_dates should have same number of elements.")

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
    api_df = api_df.rename(columns={'datetime': 'date'})
    api_df['date'] = pd.to_datetime(api_df['date'])

    if include == 'hours':
        # Extract the time information from date
        api_df['hour'] = api_df['date'].dt.time
        api_df['date'] = api_df['date'].dt.date

    print('ACA', api_df)
    return api_df




