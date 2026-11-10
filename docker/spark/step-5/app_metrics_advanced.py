from pyspark.sql import SparkSession  # type: ignore

# SparkSession の初期化
spark = SparkSession.builder.appName("SparkSQLAdvancedMetricsBatch").getOrCreate()

print("=== 1. Reading sensor_logs from Cassandra ===")
df_logs = (
    spark.read.format("org.apache.spark.sql.cassandra").options(table="sensor_logs", keyspace="iot_keyspace").load()
)

# Spark SQLが使えるように一時ビューを作成
df_logs.createOrReplaceTempView("v_sensor_logs")

print("=== 2. Performing Advanced Aggregations & UnPivoting using Spark SQL ===")

# 時間別 (hourly) および日別 (daily) の集計を同時に計算し、STACKで縦持ちに展開
df_metrics = spark.sql("""
    WITH hourly_base AS (
        SELECT
            sensor_id,
            'hourly' AS period_type,
            date_trunc('hour', timestamp) AS window_start,
            AVG(temperature) AS avg_temperature,
            percentile_approx(temperature, 0.5) AS median_temperature,
            percentile_approx(temperature, 0.95) AS p95_temperature,
            STDDEV(temperature) AS stddev_temperature,
            CAST(COUNT(1) AS DOUBLE) AS total_count
        FROM v_sensor_logs
        GROUP BY sensor_id, date_trunc('hour', timestamp)
    ),
    daily_base AS (
        SELECT
            sensor_id,
            'daily' AS period_type,
            date_trunc('day', timestamp) AS window_start,
            AVG(temperature) AS avg_temperature,
            percentile_approx(temperature, 0.5) AS median_temperature,
            percentile_approx(temperature, 0.95) AS p95_temperature,
            STDDEV(temperature) AS stddev_temperature,
            CAST(COUNT(1) AS DOUBLE) AS total_count
        FROM v_sensor_logs
        GROUP BY sensor_id, date_trunc('day', timestamp)
    ),
    combined AS (
        SELECT * FROM hourly_base
        UNION ALL
        SELECT * FROM daily_base
    )
    SELECT
        sensor_id,
        period_type,
        window_start,
        metric_name,
        metric_value
    FROM combined
    LATERAL VIEW stack(5,
        'avg_temperature', avg_temperature,
        'median_temperature', median_temperature,
        'p95_temperature', p95_temperature,
        'stddev_temperature', stddev_temperature,
        'total_count', total_count
    ) AS metric_name, metric_value
""")

# 集計結果をコンソールに出力してプレビュー
df_metrics.show(20, truncate=False)

print("=== 3. Writing Advanced Metrics back to Cassandra ===")

# Cassandra の sensor_metrics_summary テーブルへ書き戻す
df_metrics.write.format("org.apache.spark.sql.cassandra").options(
    table="sensor_metrics_summary", keyspace="iot_keyspace"
).mode("append").save()

print("Advanced metrics batch aggregation and write-back completed successfully!")
