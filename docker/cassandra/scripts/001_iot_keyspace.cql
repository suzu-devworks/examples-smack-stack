-- テスト用キースペースの作成
CREATE KEYSPACE IF NOT EXISTS iot_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE iot_keyspace;

-- 保存先テーブルの作成
CREATE TABLE IF NOT EXISTS sensor_logs (
    sensor_id text,
    timestamp timestamp,
    temperature double,
    status text,
    PRIMARY KEY (sensor_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
  AND default_time_to_live = 86400; -- 例: 24時間（86400秒）で自動削除する場合

EXIT;
