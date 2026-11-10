CREATE KEYSPACE IF NOT EXISTS iot_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

USE iot_keyspace;

-- 標準偏差やパーセンタイルを含む高度なメトリクス集計テーブル
CREATE TABLE IF NOT EXISTS sensor_metrics_summary (
    sensor_id text,
    period_type text,       -- 集計粒度 ('hourly' または 'daily')
    window_start timestamp, -- 集計の開始時刻 (timestamp型)
    metric_name text,       -- 指標名 ('avg_temperature', 'median_temperature', 'p95_temperature', 'stddev_temperature', 'total_count')
    metric_value double,    -- 集計された数値
    PRIMARY KEY ((sensor_id, period_type), window_start, metric_name)
) WITH CLUSTERING ORDER BY (window_start DESC, metric_name ASC);
