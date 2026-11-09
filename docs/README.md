# Let's give the Smack Stack a try

## Table of Contents <!-- omit in toc -->

- [Overview](#overview)
- [Environments](#environments)
- [SMACK Stack is](#smack-stack-is)
- [Roles of Products](#roles-of-products)
- [Apache Spark](#apache-spark)
  - [Components of Apache Spark](#components-of-apache-spark)
  - [From Spark to Cassandra （Spark Streaming）](#from-spark-to-cassandra-spark-streaming)
  - [From Cassandra to Spark （Spark SQL）](#from-cassandra-to-spark-spark-sql)
- [Apache Kafka](#apache-kafka)
- [Step 1: Real-time log accumulation system for pseudo IoT sensors](#step-1-real-time-log-accumulation-system-for-pseudo-iot-sensors)
  - [Start Containers for Step 1](#start-containers-for-step-1)
  - [Create Cassandra Table　(sensor\_logs)](#create-cassandra-tablesensor_logs)
  - [Verify the connection from Spark](#verify-the-connection-from-spark)
  - [Start Spark Script (simulation)](#start-spark-script-simulation)
  - [Verify Cassandra (sensor\_logs)](#verify-cassandra-sensor_logs)
- [Step 2: Perform more advanced aggregation](#step-2-perform-more-advanced-aggregation)
  - [Start Containers for Step 2](#start-containers-for-step-2)
  - [Create Cassandra Table　(sensor\_averages)](#create-cassandra-tablesensor_averages)
  - [Start Spark Script (aggregator)](#start-spark-script-aggregator)
  - [Verify Cassandra (sensor\_averages)](#verify-cassandra-sensor_averages)
- [Step 3　　Incorporate Kafka as a buffer for IoT data ingestion](#step-3incorporate-kafka-as-a-buffer-for-iot-data-ingestion)
  - [Start Containers for Step 3](#start-containers-for-step-3)
  - [Create Cassandra Table　(sensor\_averages)](#create-cassandra-tablesensor_averages-1)
  - [Start Repeater](#start-repeater)
  - [Start Spark Script (buffered by Kafka)](#start-spark-script-buffered-by-kafka)
  - [(optional) Kafka Console Consumer](#optional-kafka-console-consumer)
  - [Launch a simulated IoT device](#launch-a-simulated-iot-device)
  - [Verify Cassandra (sensor\_averages)](#verify-cassandra-sensor_averages-1)
- [Step 4　Processing of delayed data](#step-4processing-of-delayed-data)
  - [Start Cassandra and Spark Containers for Step 4](#start-cassandra-and-spark-containers-for-step-4)
  - [Create Cassandra Table　(sensor\_averages)](#create-cassandra-tablesensor_averages-2)
  - [Start Repeater](#start-repeater-1)
  - [Start Spark Script (watermark)](#start-spark-script-watermark)
  - [Run Watermark Test Script](#run-watermark-test-script)
  - [Verify Cassandra (sensor\_averages for Watermark)](#verify-cassandra-sensor_averages-for-watermark)

## Overview

Cassandra と Apache Spark を組み合わせて、Spark Streaming(Structured Streaming) を手軽に試す Tips をまとめるつもりが、
結局は本番さながらの「データ損失を防ぐための実践的な基盤」を構築するための手順やポイントをまとめることになりました。

これ以上は膨らませない様にしたいです。

## Environments

- 2026-11-07: on Docker outside of docker
  - Cassandra: 5.0.9 (cassandra:latest)
  - Apache Spark: 3.1.9, Scala 2.12, Python 3.10.12 (spark:3.5.9-python3) ※
  - Apache Kafka: 4.3.1 (apache/kafka:latest)
  - Python 3.14+ from uv

> [!NOTE]
> Apache Spark の最新バージョンは 4.2.1 でしたが、対応する Cassandra コネクタが存在しないため、3.x 系の Spark を使用しています。

## SMACK Stack is

 README にまとめたので省略します。

## Roles of Products

- Cassandraの役割：大量のデータを高速に「書き込む」「保存する」のが得意（リアルタイムのデータ蓄積）。しかし、複雑な集計や分析（例：全データの平均値を出す、複数のテーブルを結合する）は苦手。
- Sparkの役割：Cassandraに貯まった大量のデータを、メモリ上に一瞬で引っ張ってきて「超高速で集計・分析・加工」するのが得意。
- Kafkaの役割：リアルタイムに流れ続けるデータを高速に受け取り、必要に応じてSpark Streamingなどの処理系に渡すのが得意。データの一時的なバッファとしても機能します。

## Apache Spark

### Components of Apache Spark

> Apache Spark の主要コンポーネント

- Spark SQL: 構造化データを扱うための機能（DataFrame / Dataset API）やSQLクエリを提供します。
- Spark Streaming: リアルタイムに流れ続けるデータをミニバッチ方式で処理します。
- MLlib: 分類、回帰、クラスタリングなど標準的な機械学習アルゴリズムのライブラリです。
- GraphX: グラフ構造（人間関係やネットワーク等）を解析するライブラリです。

このうち、Cassandra と組み合わせてよく使われるのは次のパターンです。

- Spark → Cassandra = Spark Streaming でリアルタイムにデータを処理し、Cassandra に書き込むパターンです。
- Cassandra → Spark = Spark SQL で Cassandra に貯まった大量のデータを Spark で読み込み、分析・加工するパターンです。Cassandra は DataFrame として直接扱うことができます。

### From Spark to Cassandra （Spark Streaming）

Cassandraは「大量のデータを高速に書き込む・蓄積する」のが非常に得意なため、Sparkでリアルタイム処理（Spark Streaming）した結果の吐き出し先としてよく使われます。

### From Cassandra to Spark （Spark SQL）

Cassandra単体では、複雑な条件での集計や結合（JOIN）、全件を舐めるような分析が苦手です。そこにSparkを組み合わせることで、「Cassandraに貯めて、Sparkで分析する」という強力な分業体制が作れます。

## Apache Kafka

「デバイスとサーバー間（入口）」はUDPなどで通信しますが、「サーバーからSpark Streaming間」は直接UDPで受け取らず、メッセージブローカー（Kafkaなど）を中間に挟むのが一般的なアーキテクチャです。

とは言え、IoT デバイスなどからのデータは UDP / CoAP などの軽量プロトコルで送信されることが多く、消費電力を最小限に抑えるための最適解として採用されることが多いです。

ではなぜ直接SparkでUDPを受けないのか？
ここが、Kafka などのメッセージブローカーを中間に挟む理由です。

- データの取りこぼしを防ぐ： Spark Streamingの処理（マイクロバッチなど）が一瞬詰まった際、UDPはデータを投げっぱなしにするため、データが消滅してしまいます。
- バッファの役割： 受信専用の軽量なサーバー（Node.jsやGo、Embulkなど）で一度UDPを受け、すぐに Apache Kafka や AWS Kinesis などのメッセージキュー（ブローカー）に安全に放り込みます。Sparkはそのキューからデータを確実に吸い上げます。

## Step 1: Real-time log accumulation system for pseudo IoT sensors

> 擬似的な IoT センサーからのログをリアルタイムに蓄積するシステムを構築するケースを考えます。

```mermaid
architecture-beta
    group backend(cloud)[Data Infrastructure]
        service device(server)[rate(Spark)] in backend
        service spark(server)[Spark] in backend
        service cassandra(database)[Cassandra] in backend
    
    device:R --> L:spark
    spark:B --> T:cassandra
```

### Start Containers for Step 1

- See [`compose.yaml`](../compose.yaml)

```bash
docker compose up -d
```

> devcontainer (dev) 内からホストの Docker を操作できる設定になっています。

### Create Cassandra Table　(sensor_logs)

Cassandra に接続して、キースペースとテーブルを作成します。

> コンテナ起動時に自動的に作成する設定はありませんでした。

- See [`cassandra/scripts/001_iot_keyspace.cql`](../docker/cassandra/scripts/001_iot_keyspace.cql)

```bash
docker exec -it cassandra cqlsh -f /workspaces/cassandra/scripts/001_iot_keyspace.cql
```

### Verify the connection from Spark

```bash
docker exec -it -u root spark bash
```

> `root` でログインしている理由は後述。

公式イメージの場合、実行ファイルは `/opt/spark/bin/` に配置されています。PATHは通っていません。

まずはコンテナの Scala バージョンを確認してみます。

```bash
/opt/spark/bin/spark-shell --version
```

```console
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.5.9
      /_/
                        
Using Scala version 2.12.18, OpenJDK 64-Bit Server VM, 11.0.32.1
Branch HEAD
Compiled by user holden on 2026-07-12T18:40:50Z
Revision 7c14a3c28b141cc97a330c4d0f5d2a6da7267f85
Url https://github.com/apache/spark
Type --help for more information.
```

Scala と Spark のバージョンを確認できたので対応する Cassandra コネクタを指定して接続します。

- [Cassandra Spark Connector - Maven Repository](https://mvnrepository.com/artifact/com.datastax.spark/spark-cassandra-connector)

コネクタを読み込ませるには以下のように実行します：

```bash
/opt/spark/bin/pyspark \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 \
  --conf spark.cassandra.connection.host=cassandra
```

このコマンドで次のエラーが出るため、コンテナに入る時に `-u root` を指定して実行する必要がありました。

```console
Exception in thread "main" java.io.FileNotFoundException: /nonexistent/.ivy2/cache/resolved-org.apache.spark-spark-submit-parent-b4100866-9367-4358-9a84-c57452cacb0f-1.0.xml (No such file or directory)
```

しばらく待つと. Python の プロンプトが表示されます。

```console
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.5.9
      /_/

Using Python version 3.10.12 (main, Aug 31 2026 10:18:17)
Spark context Web UI available at http://4d3241710721:4040
Spark context available as 'sc' (master = local[*], app id = local-1794039275062).
SparkSession available as 'spark'.
>>> 
```

次のスクリプトを手打ち（ペースト）します。

```python
df = spark.read.format("org.apache.spark.sql.cassandra").options(table="sensor_logs", keyspace="iot_keyspace").load()
df.show()
```

> `pyspark`　で入るので `spark`　がすでに有効です。

こんなのが出力されれば接続成功です。

```console
+---------+------+-----------+---------+
|sensor_id|status|temperature|timestamp|
+---------+------+-----------+---------+
+---------+------+-----------+---------+

>>> 
```

### Start Spark Script (simulation)

- See [`spark/step-1/app.py`](../docker/spark/step-1/app.py)

```bash
docker exec -it -u root spark /opt/spark/bin/spark-submit \
  --master "local[1]" \
  --driver-memory 512m \
  --conf spark.sql.shuffle.partitions=2 \
  --conf spark.cassandra.connection.host=cassandra \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 \
  /workspaces/spark/step-1/app.py
```

- `--master "local[1]"`: CPUを1コアだけ使うように制限し、スレッドの乱立を防ぎます（デフォルトの local[*] は全コアを使い切ろうとします）。
- `--driver-memory 512m`: Sparkのメモリ上限を512MBに抑えます。
- `--conf spark.sql.shuffle.partitions=2`: 集計時のデータ分割数（デフォルト200）を「2」に減らします。データ量が少ないテスト環境では、これを減らすだけで劇的に軽くなります。

### Verify Cassandra (sensor_logs)

データが cassandra に流れ込んでくれば成功です。

```bash
docker exec -it cassandra cqlsh -k iot_keyspace
```

```sql
cqlsh:iot_keyspace> SELECT * FROM sensor_logs;

 sensor_id | status  | temperature | timestamp
-----------+---------+-------------+---------------------------------
  sensor_2 | WARNING |    21.58791 | 2026-11-07 09:18:07.895000+0000
  sensor_1 |      OK |    21.71743 | 2026-11-07 09:18:09.895000+0000
  sensor_0 |      OK |    27.36492 | 2026-11-07 09:18:08.895000+0000
```

## Step 2: Perform more advanced aggregation

> window 関数を使い、「1分間の枠（固定ウィンドウ）」でグループ化して平均値を計算します。

### Start Containers for Step 2

- See [`compose.yaml`](../compose.yaml)

```bash
docker compose up -d
```

### Create Cassandra Table　(sensor_averages)

Cassandra に接続して、キースペースとテーブルを作成します。

- See [`cassandra/scripts/002_iot_aggregated.cql`](../docker/cassandra/scripts/002_iot_aggregated.cql)

```bash
docker exec -it cassandra cqlsh -f /workspaces/cassandra/scripts/002_iot_aggregated.cql
```

### Start Spark Script (aggregator)

- See [`spark/step-2/app.py`](../docker/spark/step-2/app.py)

```bash
docker exec -it -u root spark /opt/spark/bin/spark-submit \
  --master "local[1]" \
  --driver-memory 512m \
  --conf spark.sql.shuffle.partitions=2 \
  --conf spark.cassandra.connection.host=cassandra \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 \
  /workspaces/spark/step-2/app_agg.py
```

### Verify Cassandra (sensor_averages)

データが cassandra に流れ込んでくれば成功です。

```bash
docker exec -it cassandra cqlsh -k iot_keyspace
```

```sql
cqlsh:iot_keyspace> SELECT * FROM sensor_averages LIMIT 10;

 sensor_id | window_start                    | avg_temperature | window_end
-----------+---------------------------------+-----------------+---------------------------------
  sensor_2 | 2026-11-09 05:25:00.000000+0000 |        25.84015 | 2026-11-09 05:26:00.000000+0000
  sensor_2 | 2026-11-09 05:26:00.000000+0000 |        24.81404 | 2026-11-09 05:27:00.000000+0000
  sensor_1 | 2026-11-09 05:25:00.000000+0000 |        25.03175 | 2026-11-09 05:26:00.000000+0000
  sensor_1 | 2026-11-09 05:26:00.000000+0000 |        24.33107 | 2026-11-09 05:27:00.000000+0000
  sensor_0 | 2026-11-09 05:25:00.000000+0000 |        26.02362 | 2026-11-09 05:26:00.000000+0000
  sensor_0 | 2026-11-09 05:26:00.000000+0000 |        25.70184 | 2026-11-09 05:27:00.000000+0000
```

## Step 3　　Incorporate Kafka as a buffer for IoT data ingestion

> IoTデータ取り込みのバッファとしてKafkaを組み込む

```mermaid
architecture-beta
    group iot(cloud)[IoT]
        service device(server)[Device] in iot

    group gw(server)[DMZ/Gateway]
        service repeater(server)[Repeater] in gw

    group backend(cloud)[Data Infrastructure]
        service kafka(server)[Kafka] in backend
        service spark(server)[Spark] in backend
        service cassandra(database)[Cassandra] in backend
    
    device:R --> L:repeater
    repeater:R --> L:kafka
    kafka:R --> L:spark
    spark:B --> T:cassandra
```
  
一連のパイプラインをローカル環境で動かすための設定手順とPythonコードをまとめました。

### Start Containers for Step 3

- See [`compose.yaml`](../compose.yaml)

```bash
docker compose up -d
```

### Create Cassandra Table　(sensor_averages)

Step 2と同じテーブルなので作成済みであればスキップして構いません。

- See [`cassandra/scripts/002_iot_aggregated.cql`](../docker/cassandra/scripts/002_iot_aggregated.cql)

```bash
docker exec -it cassandra cqlsh -f /workspaces/cassandra/scripts/002_iot_aggregated.cql
```

### Start Repeater

リピーターは、IoTデバイスからのデータを受信し、Kafka に送信する役割を果たします。
新たなコンテナを作らず　devcontainer でリピーターの代役をやります。

- See [`repeater/bridge.py`](../docker/repeater/bridge.py)

repeater専用のコンテナを用意しているので次を実行します。

```bash
docker exec -it repeater uv run /workspaces/repeater/bridge.py --host 0.0.0.0
```

```console
UDP Bridge started. Listening on port 5005...
```

> [!NOTE]
> Kafkaとの接続用に `confluent-kafka` を使用していますが、インストールの際にC言語のヘッダファイルが必要になることがあります。
> 環境によって用意できない場合には `kafka-python-ng` でも接続できます。その際には `bridge-slim.py` を参照してください。

### Start Spark Script (buffered by Kafka)

- See [`spark/step-3/app_kafka.py`](../docker/spark/step-3/app_kafka.py)

```bash
docker exec -it -u root spark /opt/spark/bin/spark-submit \
  --master "local[1]" \
  --driver-memory 512m \
  --conf spark.sql.shuffle.partitions=2 \
  --conf spark.cassandra.connection.host=cassandra \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 \
  /workspaces/spark/step-3/app_kafka.py
```

### (optional) Kafka Console Consumer

Kafka に送信されたデータを確認するために、Kafka コンテナでコンソールコンシューマを起動できます。

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 \
  --topic iot-topic \
  --from-beginning
```

### Launch a simulated IoT device

repeaterコンテナは、ホストの5005(udp)へポートフォワードしているので `host.docker.internal` 宛に通知します。

```bash
 uv run ./docker/iot/device_sim.py --host host.docker.internal
```

### Verify Cassandra (sensor_averages)

データが cassandra に流れ込んでくれば成功です。

```bash
docker exec -it cassandra cqlsh -k iot_keyspace
```

```sql
cqlsh:iot_keyspace> SELECT * FROM sensor_averages LIMIT 10;

 sensor_id | window_start                    | avg_temperature | window_end
-----------+---------------------------------+-----------------+---------------------------------
  sensor_2 | 2026-11-09 07:35:00.000000+0000 |         25.1381 | 2026-11-09 07:36:00.000000+0000
  sensor_2 | 2026-11-09 07:36:00.000000+0000 |           23.74 | 2026-11-09 07:37:00.000000+0000
  sensor_1 | 2026-11-09 07:35:00.000000+0000 |        25.39706 | 2026-11-09 07:36:00.000000+0000
  sensor_1 | 2026-11-09 07:36:00.000000+0000 |           28.87 | 2026-11-09 07:37:00.000000+0000
  sensor_0 | 2026-11-09 07:35:00.000000+0000 |        25.65905 | 2026-11-09 07:36:00.000000+0000
  sensor_0 | 2026-11-09 07:36:00.000000+0000 |           20.74 | 2026-11-09 07:37:00.000000+0000
```

## Step 4　Processing of delayed data

ウォーターマーク（Watermark）を導入することで、ネットワーク遅延などで遅れて届いたIoTデータを正しく集計しつつ、メモリ不足（OOM）を防ぐことができます。

### Start Cassandra and Spark Containers for Step 4

- See [`compose.yaml`](../compose.yaml)

```bash
docker compose up -d
```

### Create Cassandra Table　(sensor_averages)

Step 2と同じテーブルなので作成済みであればスキップして構いません。

- See [`cassandra/scripts/002_iot_aggregated.cql`](../docker/cassandra/scripts/002_iot_aggregated.cql)

```bash
docker exec -it cassandra cqlsh -f /workspaces/cassandra/scripts/002_iot_aggregated.cql
```

### Start Repeater

Step 3 と同じ中継機です。

- See [`repeater/bridge.py`](../docker/repeater/bridge.py)

```bash
docker exec -it repeater uv run /workspaces/repeater/bridge.py --host 0.0.0.0
```

### Start Spark Script (watermark)

- See [`spark/step-4/app_watermark.py`](../docker/spark/step-4/app_watermark.py)

```bash
docker exec -it -u root spark /opt/spark/bin/spark-submit \
  --master "local[1]" \
  --driver-memory 512m \
  --conf spark.sql.shuffle.partitions=2 \
  --conf spark.cassandra.connection.host=cassandra \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 \
  /workspaces/spark/step-4/app_watermark.py
```

### Run Watermark Test Script

```bash
uv run ./docker/iot/test_watermark.py --host host.docker.internal
```

### Verify Cassandra (sensor_averages for Watermark)

```bash
docker exec -it cassandra cqlsh -k iot_keyspace
```

```sql
cqlsh:iot_keyspace> SELECT sensor_id, window_start, window_end, avg_temperature
  FROM iot_keyspace.sensor_averages
  WHERE sensor_id = 'sensor_test';
```

次のような動きになります。

1. 基準時刻の正常データを送信 (気温 20.0℃)

    ```text
     sensor_id   | window_start                    | window_end                      | avg_temperature
    -------------+---------------------------------+---------------------------------+-----------------
     sensor_test | 2026-11-09 09:21:00.000000+0000 | 2026-11-09 09:22:00.000000+0000 |              20
    ```

2. 時間を進めてウォーターマークを少し押し上げる

    ```text
     sensor_id   | window_start                    | window_end                      | avg_temperature
    -------------+---------------------------------+---------------------------------+-----------------
     sensor_test | 2026-11-09 09:21:00.000000+0000 | 2026-11-09 09:22:00.000000+0000 |              20
     sensor_test | 2026-11-09 09:22:00.000000+0000 | 2026-11-09 09:23:00.000000+0000 |              25
    ```

3. 1分遅れのデータを送信 (許容範囲内: 2分以内)

    気温 100.0℃ を送るので、元の 20.0℃ と平均されて「60.0℃」に更新されるはず！

    ```text
     sensor_id   | window_start                    | window_end                      | avg_temperature
    -------------+---------------------------------+---------------------------------+-----------------
     sensor_test | 2026-11-09 09:21:00.000000+0000 | 2026-11-09 09:22:00.000000+0000 |              60
     sensor_test | 2026-11-09 09:22:00.000000+0000 | 2026-11-09 09:23:00.000000+0000 |              25
    ```

4. さらに時間を3分進める (ウォーターマークが基準ウィンドウを完全に通り過ぎる)

    ```text
     sensor_id   | window_start                    | window_end                      | avg_temperature
    -------------+---------------------------------+---------------------------------+-----------------
     sensor_test | 2026-11-09 09:21:00.000000+0000 | 2026-11-09 09:22:00.000000+0000 |              60
     sensor_test | 2026-11-09 09:22:00.000000+0000 | 2026-11-09 09:23:00.000000+0000 |              25
     sensor_test | 2026-11-09 09:25:00.000000+0000 | 2026-11-09 09:26:00.000000+0000 |              30
    ```

5. 期限切れの遅刻データを送信 (許容範囲外: 2分超過)

    気温 999.0℃ を送るが、破棄されるため平均は「60.0℃」のまま変わらないはず！

    ```text
     sensor_id   | window_start                    | window_end                      | avg_temperature
    -------------+---------------------------------+---------------------------------+-----------------
     sensor_test | 2026-11-09 09:21:00.000000+0000 | 2026-11-09 09:22:00.000000+0000 |              60
     sensor_test | 2026-11-09 09:22:00.000000+0000 | 2026-11-09 09:23:00.000000+0000 |              25
     sensor_test | 2026-11-09 09:25:00.000000+0000 | 2026-11-09 09:26:00.000000+0000 |              30
    ```

やり直す場合にはデータを削除して再度送信してください。

```sql
TRUNCATE TABLE iot_keyspace.sensor_averages;
```
