CREATE TABLE weather_daily_data (
    date DATE NOT NULL,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL,
    conditions VARCHAR(255),
    temperature FLOAT NOT NULL,
    PRIMARY KEY (date, latitude, longitude)
    )