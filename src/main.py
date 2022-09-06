import os
import sys
import inspect

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)

from properties_info.properties_info import PropertiesInfo


if __name__ == '__main__':
    """
    Execute this code to get all the answers from the Challenge
    """
    properties_info = PropertiesInfo()

    # Question 1
    print('Pregunta 1: Número total de visitas en la data:')
    print(properties_info.get_total_visits())
    print('')

    # Question 2
    print('Pregunta 2: Promedio de propiedades por usuario:')
    print(properties_info.get_average_properties_by_user())
    print('')

    # Question 3
    user_id = 2
    print('Pregunta 3: Temperatura promedio a las visitas de las propiedades del usuario 2: ')
    print(round(properties_info.get_average_temperature_in_visits_by_user(user_id), 2), '°C')
    print('')

    # Question 4
    weather_condition = 'Rain'
    print('Pregunta 4: Temperatura promedio a las visitas de las propiedades en días con lluvia: ')
    print(round(properties_info.get_average_temperature_in_visits_by_weather_condition('Rain'), 2), '°C')
    print('')

    # Question 5
    print('Pregunta 5: Temperatura promedio a las visitas de las propiedades de la localidad de Suba: ')
    print(round(properties_info.get_average_temperature_in_visits_by_location('Suba'), 2), '°C')
    print('')

