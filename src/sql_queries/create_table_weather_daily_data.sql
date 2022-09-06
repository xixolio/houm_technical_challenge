--- Table used to store data coming from the weather API in its daily format
CREATE TABLE weather_daily_data (
    date DATE NOT NULL, --- date of the weather data. E.g. 2022-01-01.
    latitude FLOAT NOT NULL, --- latitude of the weather data.
    longitude FLOAT NOT NULL, --- longitude of the weather data.
    conditions VARCHAR(255), --- weather conditions provided by the weather API: Rain, Snow, etc.
    temperature FLOAT NOT NULL, --- temperature in Celsius degrees provided by the weather API
    PRIMARY KEY (date, latitude, longitude)
    )