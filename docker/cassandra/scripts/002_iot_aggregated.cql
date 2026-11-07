CREATE KEYSPACE IF NOT EXISTS iot_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE iot_keyspace;

CREATE TABLE IF NOT EXISTS sensor_averages (
    sensor_id text,
    window_start timestamp,
    window_end timestamp,
    avg_temperature double,
    PRIMARY KEY (sensor_id, window_start) -- センサーごと・時間順に並ぶように複合主キーにします
);

EXIT;
