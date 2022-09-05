CREATE TABLE weather_hourly_data (
    date DATE NOT NULL,
    hour TIME NOT NULL,
    location VARCHAR(255) NOT NULL,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL,
    conditions VARCHAR(255),
    temperature FLOAT NOT NULL,
    PRIMARY KEY (date, location, latitude, longitude)
    )
