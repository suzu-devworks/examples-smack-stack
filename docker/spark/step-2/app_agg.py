from pyspark.sql import (  # type: ignore
    DataFrame,
    SparkSession,
)
from pyspark.sql.functions import avg, col, concat, lit, rand, window  # type: ignore

# 1. Sparkセッションの作成
spark = SparkSession.builder.appName("SparkStreamingAggregationToCassandra").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. 擬似IoTデータソースの作成 (毎秒1件のデータを自動生成)
rate_stream = spark.readStream.format("rate").option("rowsPerSecond", 1).load()

# 3. データの加工 (センサーIDの割り振りとランダムな気温)
processed_stream = rate_stream.select(
    concat(lit("sensor_"), (col("value") % 3)).alias("sensor_id"),
    col("timestamp"),
    (lit(20.0) + (rand() * 10.0)).alias("temperature"),
)

# 🌟 4. 1分ごとのウィンドウ集計 (1分間の平均気温を計算)
# window(基準列, "時間幅") で時間ごとのグループを作れます
aggregated_stream = (
    processed_stream.groupBy(col("sensor_id"), window(col("timestamp"), "1 minute"))
    .agg(avg("temperature").alias("avg_temperature"))
    .select(
        col("sensor_id"),
        col("window.start").alias("window_start"),  # ウィンドウ開始時刻
        col("window.end").alias("window_end"),  # ウィンドウ終了時刻
        col("avg_temperature"),
    )
)


# 5. Cassandraへのストリーミング書き込み（foreachBatchでUpsertする）
def write_to_cassandra(batch_df: DataFrame, batch_id: int) -> None:
    (
        batch_df.write.format("org.apache.spark.sql.cassandra")
        .option("spark.cassandra.connection.host", "cassandra")
        .option("keyspace", "iot_keyspace")
        .option("table", "sensor_averages")
        .option("spark.cassandra.output.ttl", "3600")
        .mode("append")  # バッチ書き込みのmodeは "append" にします（Cassandra側で自動Upsertされます）
        .save()
    )


query = (
    aggregated_stream.writeStream.foreachBatch(write_to_cassandra)
    .outputMode("update")  # ここは "update" のままでOK！
    .option("checkpointLocation", "./checkpoint_agg")
    .start()
)

query.awaitTermination()
