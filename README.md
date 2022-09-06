# Houm Data Engineer Technical Challenge

Small project built to apply to the position of Data Engineer in Houm. The challenge involved manipulating csv data and a weather API to answer specific questions about visits carried out to properties managed by Houm. In particular, it answers the 5 following questions:

- How many visits were carried out in total, given the data?
- What's the average number of owned properties by user?
- What was the average temperature in visits to properties owned by a specific user?
- What was the average temperature in visits done in rainy days?
- What was the average temperature in visits done in a specific locality?

The challenge was solved using mainly numpy and pandas, as the provided data was small and did not require the use of distributed libraries for computing. However, the design of the solution was thought to be scalable, hence for each question the data is loaded independently, filtering as much as possible as early as possible. In the future, the csv files could be uploaded to AWS S3 storage and be requested from there.

## API
For this challenge, weather information at particular locations was requested using the https://www.visualcrossing.com/ Weather API, which receives a location string, a date string and returns historical weather information at hourly or daily intervals. Since the Free Tier of this API allows for only 1000 requests each day, an SQLite DB was used to store the already requested information.



## SQLite DB
Each time a query for weather information is run, the app first searches the aforementioned DB for the requested values. All values not found are then requested to the API. The choice of a SQLite DB was mainly for usage convenience, but in the future, with bigger data, a more powerful local DB should be used; better yet, a cloud DB could be set. Since the DB used locally for the challenge is extremely light weight, it was uploaded to the repo for convenience of the evaluators. 
The code used for the creation of the tables can be found in _src/sql_queries_/.

## Relevant Code
Each question was coded as an independent method inside the `PropertiesInfo` class, located in _src/properties_info/properties_info.py_. In particular, the following methods answer each question:
- Question 1: `get_total_visits`
- Question 2: `get_average_properties_by_user`
- Question 3: `get_average_temperature_in_visits_by_user`
- Question 4: `get_average_temperature_in_visits_by_weather_condition`
- Question 5: `get_average_temperature_in_visits_by_location`

## CONFIG FILE
A config file can be found at _src/config.yaml_. In it, you can specify the API KEY, the location of the csv data, the filenames of the data, among others. 

## Assumptions

To solve this challenge, the following assumptions were made regarding the nature of the questions and the data provided:
- Temperatures are provided in Celsious degrees.
- To get the average temperature of a particular visit to a property, the temperatures of each hour between `begin_date` and `end_date` were used, not including the `end_date` hour. For example, if the time in `begin_date` was 14:00 and the time in `end_date` was 17:00, we use the hourly temperatures at 14:00, 15:00, and 16:00 to compute the average temperature of the visit.
- When asked visits done in a particular weather condition such as Rain, we consider the global weather condition of the day given in the `begin_date` column. Moreover, we search for the weather condition as a keyword, so, Rain would also imply Heavy Rain, Light Rain, and so on. This could be improved adding a 'strict' parameter to force specific matches.
- All dates were transformed to UTC for convenience. I am aware that this implies that the `begin_date` used for daily weather conditions my differ from the original date using in the timezone, but since the data may contain many different countries, this was the simplest approach. A more sophisticated solution in the future is in order.




## Installation

Create a conda enviroment using python 3.10 and activate it. Move to root folder of the project and run:

```sh
pip install -r requirements
```

## Usage
Froom root folder:
```sh
python src/main.py
```
It will go through every question outputing the results. If conditions for each question want to be changes (such as `user_id`, `location`, `weather_condition`), they should be manually edited in main.py script.

## Testing

Unit tests for the most important functions were programmed. Tests in the _test/utils_ folder provide with mainly sanity checks for the query functions, and can definitely be improved.

To run the tests, go to the folder of the test script and run
```sh
pytest <test_script>
```
For example, in _test/utils/_ you can run:
```sh
pytest test_queries.py
```

