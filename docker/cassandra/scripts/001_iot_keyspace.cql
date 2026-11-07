-- テスト用キースペースの作成
CREATE KEYSPACE IF NOT EXISTS iot_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE iot_keyspace;

-- 保存先テーブルの作成
CREATE TABLE IF NOT EXISTS sensor_logs (
    sensor_id text PRIMARY KEY,
    timestamp timestamp,
    temperature double,
    status text
) WITH default_time_to_live = 3600; -- default: 1 hours

EXIT;
