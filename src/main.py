from properties_info.properties_info import PropertiesInfo


if __name__ == '__main__':
    """
    Execute this code to get all the answers from the Challenge
    """
    properties_info = PropertiesInfo()

    # Question 1
    print(properties_info.get_total_visits())

    # Question 2
    print(properties_info.get_average_properties_by_user())

    # Question 3
    print(properties_info.get_average_temperature_in_visits_by_user(2))