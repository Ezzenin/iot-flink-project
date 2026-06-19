INSERT INTO device_types (id, type_name) VALUES
    (1, 'temperature_sensor'),
    (2, 'humidity_sensor'),
    (3, 'thermostat'),
    (4, 'weather_station')
ON CONFLICT (id) DO UPDATE SET type_name = EXCLUDED.type_name;
