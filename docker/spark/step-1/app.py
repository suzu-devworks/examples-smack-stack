from pyspark.sql import SparkSession  # type: ignore
from pyspark.sql.functions import col, concat, lit, rand, when  # type: ignore

# 1. Sparkセッションの作成
# Cassandraの接続先（ホスト）は起動時の設定に従います
spark = (
    SparkSession.builder.appName("SparkStreamingToCassandra")
    # .config("spark.cassandra.connection.host", "cassandra")
    .getOrCreate()
)

# ログレベルを下げて見やすくする
spark.sparkContext.setLogLevel("WARN")

# 2. 擬似IoTデータソースの作成 (毎秒1件のデータを自動生成)
# 'rate' ソースを使うことで、timestampとvalue(連番)を持つデータストリームが作られます
rate_stream = spark.readStream.format("rate").option("rowsPerSecond", 1).load()

# 3. Cassandraのスキーマに合わせてデータを加工・生成
# センサーIDを適当に割り振り、ランダムな気温（20℃〜30℃）とステータスを計算します
processed_stream = rate_stream.select(
    # sensor_id: 例 "sensor_1", "sensor_2"... (valueを3で割った余りで3台分シミュレート)
    concat(lit("sensor_"), (col("value") % 3)).alias("sensor_id"),
    # timestamp: rateソースが持つ生成時刻をそのまま使用
    col("timestamp"),
    # temperature: 20 + (0〜1の乱数 * 10) = 20.0℃〜30.0℃
    (lit(20.0) + (rand() * 10.0)).alias("temperature"),
    # status: 気温が27度以上なら 'WARNING'、それ以外は 'OK'
    when((lit(20.0) + (rand() * 10.0)) >= 27.0, "WARNING").otherwise("OK").alias("status"),
)

# 4. Cassandraへのストリーミング書き込み
# 事前に作成したキースペース(iot_keyspace)とテーブル(sensor_logs)を指定します
query = (
    processed_stream.writeStream.format("org.apache.spark.sql.cassandra")
    .option("keyspace", "iot_keyspace")
    .option("table", "sensor_logs")
    .option("spark.cassandra.output.ttl", "1800")  # 👈 ここにTTLを「秒数」で追加（例: 30分 = 1800秒）
    .outputMode("append")
    .option("checkpointLocation", "./checkpoint")
    .start()
)

# ストリーミング処理の継続
query.awaitTermination()
