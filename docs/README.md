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
  - [Start Cassandra and Spark Containers for Step 1](#start-cassandra-and-spark-containers-for-step-1)
  - [Copy resources to Containers](#copy-resources-to-containers)
  - [Create Cassandra Keyspaces and Table](#create-cassandra-keyspaces-and-table)
  - [Verify the connection from Spark](#verify-the-connection-from-spark)
  - [Start Spark Script (simulation)](#start-spark-script-simulation)
  - [Verify Cassandra](#verify-cassandra)
- [Step 2: Perform more advanced aggregation](#step-2-perform-more-advanced-aggregation)
  - [Start Cassandra and Spark Containers for Step 2](#start-cassandra-and-spark-containers-for-step-2)
  - [Copy resources to Containers](#copy-resources-to-containers-1)
  - [Create Aggregated Table](#create-aggregated-table)
  - [Start Spark Script (aggregator)](#start-spark-script-aggregator)
  - [Verify Cassandra](#verify-cassandra-1)
- [Step 3　　Incorporate Kafka as a buffer for IoT data ingestion](#step-3incorporate-kafka-as-a-buffer-for-iot-data-ingestion)
  - [Start Cassandra and Spark Containers for Step 3](#start-cassandra-and-spark-containers-for-step-3)
  - [Copy resources to Containers](#copy-resources-to-containers-2)
  - [Create Table](#create-table)
  - [Connect the devcontainer to the interconnect network](#connect-the-devcontainer-to-the-interconnect-network)
  - [Start Repeater](#start-repeater)
  - [Start Spark Script (production)](#start-spark-script-production)
  - [(optional) Kafka Console Consumer](#optional-kafka-console-consumer)
  - [Launch a simulated IoT device](#launch-a-simulated-iot-device)
  - [Verify Cassandra](#verify-cassandra-2)

## Overview

Cassandra と Apache Spark を組み合わせて、Spark Streaming(Structured Streaming) を手軽に試す Tips をまとめるつもりが、
結局は本番さながらの「データ損失を防ぐための実践的な基盤」を構築するための手順やポイントをまとめることになりました。

これ以上は膨らませない様にしたいです。

## Environments

- 2026-11-07: on Docker
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

//TODO あとで図を入れる

### Start Cassandra and Spark Containers for Step 1

- [See `compose.yaml`](../compose.yaml)

```bash
docker compose up -d
```

> devcontainer (dev) 内からホストの Docker を操作できる設定になっています。

### Copy resources to Containers

本当は 各コンテナの作業ディレクトリに直接マウントして使いたいのですが、devcontainer からはパスが違うのでエラーになります。

```console
The path /workspaces/examples-smack-stack/docker/spark is not shared from the host and is not known to Docker.
```

これ解決するのは難しそうなので、コンテナ内にコピーして使うことにします。

```bash
# copy to cassandra container
docker exec -u root cassandra mkdir -p /workspaces/cassandra/
docker cp ./docker/cassandra/. cassandra:/workspaces/cassandra/

# copy to spark container
docker exec -u root spark mkdir -p /workspaces/spark/
docker cp ./docker/spark/. spark:/workspaces/spark/
```

### Create Cassandra Keyspaces and Table

Cassandra に接続して、キースペースとテーブルを作成します。

> コンテナ起動時に自動的に作成する設定はありませんでした。

- [See `001_iot_keyspace.cql`](../docker/cassandra/scripts/001_iot_keyspace.cql)

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

- [Cassandra Spark Connector](https://mvnrepository.com/artifact/com.datastax.spark/spark-cassandra-connector)

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

- [`step-1/app.py`](../docker/spark/step-1/app.py)

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

### Verify Cassandra

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

### Start Cassandra and Spark Containers for Step 2

- [See `compose.yaml`](../compose.yaml)

```bash
docker compose up -d
```

### Copy resources to Containers

```bash
# copy to cassandra container
docker exec -u root cassandra mkdir -p /workspaces/cassandra/
docker cp ./docker/cassandra/. cassandra:/workspaces/cassandra/

# copy to spark container
docker exec -u root spark mkdir -p /workspaces/spark/
docker cp ./docker/spark/. spark:/workspaces/spark/
```

### Create Aggregated Table

Cassandra に接続して、キースペースとテーブルを作成します。

- [See `002_iot_aggregated.cql`](../docker/cassandra/scripts/002_iot_aggregated.cql)

```bash
docker exec -it cassandra cqlsh -f /workspaces/cassandra/scripts/002_iot_aggregated.cql
```

### Start Spark Script (aggregator)

- [`step-2/app.py`](../docker/spark/step-2/app.py)

```bash
docker exec -it -u root spark /opt/spark/bin/spark-submit \
  --master "local[1]" \
  --driver-memory 512m \
  --conf spark.sql.shuffle.partitions=2 \
  --conf spark.cassandra.connection.host=cassandra \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 \
  /workspaces/spark/step-2/app_agg.py
```

### Verify Cassandra

```bash
docker exec -it cassandra cqlsh -k iot_keyspace
```

```sql
cqlsh:iot_keyspace> SELECT * FROM sensor_averages WHERE sensor_id = 'sensor_1' ORDER BY window_start DESC LIMIT 5;

 sensor_id | window_start                    | avg_temperature | window_end
-----------+---------------------------------+-----------------+---------------------------------
  sensor_1 | 2026-11-07 13:27:00.000000+0000 |        26.06393 | 2026-11-07 13:28:00.000000+0000
  sensor_1 | 2026-11-07 13:26:00.000000+0000 |        24.74708 | 2026-11-07 13:27:00.000000+0000
  sensor_1 | 2026-11-07 13:25:00.000000+0000 |        25.38283 | 2026-11-07 13:26:00.000000+0000
  sensor_1 | 2026-11-07 13:24:00.000000+0000 |        24.88053 | 2026-11-07 13:25:00.000000+0000
  sensor_1 | 2026-11-07 13:23:00.000000+0000 |        25.29752 | 2026-11-07 13:24:00.000000+0000
```

## Step 3　　Incorporate Kafka as a buffer for IoT data ingestion

> IoTデータ取り込みのバッファとしてKafkaを組み込む

//TODO あとで絵を描く

[IoTデバイス] --(UDP/CoAP)--> [ゲートウェイ / 受信サーバー] --(書き込み)--> [Apache Kafka] --(ストリーム)--> [Spark Streaming] --> [Cassandra]

一連のパイプラインをローカル環境で動かすための設定手順とPythonコードをまとめました。

### Start Cassandra and Spark Containers for Step 3

[現場のIoTデバイス]
       │ (インターネット / VPN / 閉域網)
       ▼
[DMZ / ゲートウェイ層]  ◀── ★ 中継機（Bridge）はここ！（Kafkaの外側）
       │ (外部公開用リスナー: EXTERNAL)
       ▼
[データ基盤・内部ネットワーク]
  ├── [Kafka クラスター] ◀── 内部通信は INTERNAL（ブローカー同士・Spark用）
  ├── [Spark クラスター]
  └── [Cassandra クラスター]
  
- [See `compose.yaml`](../compose.yaml)

```bash
docker compose up -d
```

### Copy resources to Containers

```bash
# copy to cassandra container
docker exec -u root cassandra mkdir -p /workspaces/cassandra/
docker cp ./docker/cassandra/. cassandra:/workspaces/cassandra/

# copy to spark container
docker exec -u root spark mkdir -p /workspaces/spark/
docker cp ./docker/spark/. spark:/workspaces/spark/
```

### Create Table

- [See `002_iot_aggregated.cql`](../docker/cassandra/scripts/002_iot_aggregated.cql)

```bash
docker exec -it cassandra cqlsh -f /workspaces/cassandra/scripts/002_iot_aggregated.cql
```

### Connect the devcontainer to the interconnect network

devcontainer を compose.yaml で定義された interconnect ネットワークに接続します。

```bash
docker network connect examples-smack-bridge examples-smack-stack_devcontainer-dev-1
```

> ネットワーク名とコンテナ名は適時変更確認して接続してください。

### Start Repeater

リピーターは、IoTデバイスからのデータを受信し、Kafka に送信する役割を果たします。
新たなコンテナを作らず　devcontainer でリピーターの代役をやります。

- [See `repeater/bridge.py`](../docker/repeater/bridge.py)

まずは devcontainer に依存関係をインストールします。
本来は `confluent-kafka` を使うところですがC言語のヘッダファイルが必要になるため、今回は `kafka-python-ng` を使います。

```bash
uv add kafka-python-ng
```

> `pyproject.toml` に依存関係を追加ずみなので、`uv sync` を実行して依存関係を同期してください。

中継機実行します。

```bash
uv run ./docker/repeater/bridge.py 
```

```console
UDP Bridge started. Listening on port 5005...
```

### Start Spark Script (production)

- [`step-2/app.py`](../docker/spark/step-2/app.py)

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

```bash
 uv run ./docker/iot/device_sim.py 
```

### Verify Cassandra

データが cassandra に流れ込んでくれば成功です。

```bash
docker exec -it cassandra cqlsh -k iot_keyspace
```

```sql
cqlsh:iot_keyspace> SELECT * FROM sensor_averages LIMIT 10;

 sensor_id | window_start                    | avg_temperature | window_end
-----------+---------------------------------+-----------------+---------------------------------
  sensor_2 | 2026-11-08 09:39:00.000000+0000 |          28.995 | 2026-11-08 09:40:00.000000+0000
  sensor_2 | 2026-11-08 09:40:00.000000+0000 |        25.78842 | 2026-11-08 09:41:00.000000+0000
  sensor_1 | 2026-11-08 09:39:00.000000+0000 |           21.83 | 2026-11-08 09:40:00.000000+0000
  sensor_1 | 2026-11-08 09:40:00.000000+0000 |        23.93562 | 2026-11-08 09:41:00.000000+0000
  sensor_0 | 2026-11-08 09:39:00.000000+0000 |          20.465 | 2026-11-08 09:40:00.000000+0000
  sensor_0 | 2026-11-08 09:40:00.000000+0000 |        23.81455 | 2026-11-08 09:41:00.000000+0000
```
