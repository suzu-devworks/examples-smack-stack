from pyspark.sql import (  # type: ignore
    DataFrame,
    SparkSession,
)
from pyspark.sql.functions import avg, col, split, to_timestamp, window  # type: ignore

# 1. Sparkセッションの作成
spark = SparkSession.builder.appName("DockerIoTPipelineWithWatermark").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Kafkaからデータ読み出し
kafka_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "iot-topic")
    .option("startingOffsets", "latest")
    .load()
)

# 3. 文字列を分解し、デバイス側の計測時刻を timestamp 型に変換
parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING) as raw_data").select(
    split(col("raw_data"), ",").getItem(0).alias("sensor_id"),
    split(col("raw_data"), ",").getItem(1).cast("double").alias("temperature"),
    to_timestamp(split(col("raw_data"), ",").getItem(2)).alias("event_time"),
)

# 🌟 4. ウォーターマークの設定と1分ごとのウィンドウ集計
# 「最大2分間のデータ遅延（遅刻）までは許容して再計算する」という設定
aggregated_stream = (
    parsed_stream.withWatermark("event_time", "2 minutes")
    .groupBy(col("sensor_id"), window(col("event_time"), "1 minute"))
    .agg(avg("temperature").alias("avg_temperature"))
    .select(
        col("sensor_id"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("avg_temperature"),
    )
)


# 5. Cassandraへ書き込み
def write_to_cassandra(batch_df: DataFrame, batch_id: int) -> None:
    (
        batch_df.write.format("org.apache.spark.sql.cassandra")
        .option("spark.cassandra.connection.host", "cassandra")
        .option("keyspace", "iot_keyspace")
        .option("table", "sensor_averages")
        .option("spark.cassandra.output.ttl", "604800")
        .mode("append")
        .save()
    )


query = (
    aggregated_stream.writeStream.foreachBatch(write_to_cassandra)
    .outputMode("update")
    .option("checkpointLocation", "./checkpoint_watermark")
    .start()
)

query.awaitTermination()
