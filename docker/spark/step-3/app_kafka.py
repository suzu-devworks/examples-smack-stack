from pyspark.sql import (  # type: ignore
    DataFrame,
    SparkSession,
)
from pyspark.sql.functions import avg, col, current_timestamp, split, window  # type: ignore

# 1. Sparkセッションの作成 (接続先はサービス名 'cassandra')
spark = SparkSession.builder.appName("DockerIoTPipeline").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Kafkaからデータ読み出し (接続先はサービス名 'kafka:9092')
kafka_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "iot-topic")
    .option("startingOffsets", "latest")
    .load()
)

# 3. バイナリを文字列に直し、カンマで分割
parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING) as raw_data").select(
    split(col("raw_data"), ",").getItem(0).alias("sensor_id"),
    split(col("raw_data"), ",").getItem(1).cast("double").alias("temperature"),
    current_timestamp().alias("timestamp"),
)

# 4. 1分ごとの平均気温を集計
aggregated_stream = (
    parsed_stream.groupBy(col("sensor_id"), window(col("timestamp"), "1 minute"))
    .agg(avg("temperature").alias("avg_temperature"))
    .select(
        col("sensor_id"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("avg_temperature"),
    )
)


# 5. Cassandraへ書き込み (TTLは1週間)
# マイクロバッチごとに通常のバッチ書き込み（append）でCassandraへUpsertする
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
    .option("checkpointLocation", "/workspaces/spark/step-3/checkpoint_kafka")
    .start()
)

query.awaitTermination()
