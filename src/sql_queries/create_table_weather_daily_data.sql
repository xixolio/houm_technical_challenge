CREATE TABLE weather_daily_data (
    date DATE NOT NULL,
    location VARCHAR(255) NOT NULL,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL,
    conditions VARCHAR(255),
    temperature FLOAT NOT NULL,
    PRIMARY KEY (date, location, latitude, longitude)
    )