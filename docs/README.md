# Let's give the Smack Stack a try

## Table of Contents <!-- omit in toc -->

- [Overview](#overview)
- [Environments](#environments)
- [SMACK Stack is](#smack-stack-is)
- [Roles of Products](#roles-of-products)
- [Components of Apache Spark](#components-of-apache-spark)
- [Step 1: Real-time log accumulation system for pseudo IoT sensors](#step-1-real-time-log-accumulation-system-for-pseudo-iot-sensors)
  - [Start Cassandra and Spark Containers](#start-cassandra-and-spark-containers)
  - [Copy resources to Containers](#copy-resources-to-containers)
  - [Create Cassandra Keyspaces and Table](#create-cassandra-keyspaces-and-table)
  - [Verify the connection from Spark](#verify-the-connection-from-spark)
  - [Start the simulation](#start-the-simulation)
  - [Verify Cassandra](#verify-cassandra)

## Overview

Cassandra と Apache Spark を組み合わせて、Spark Streaming(Structured Streaming) を手軽に試す Tips をまとめるつもりが、
結局は本番さながらの「データ損失を防ぐための実践的な基盤」を構築するための手順やポイントをまとめることになりました。

これ以上は膨らませない様にしたいです。

## Environments

- 2026-11-07: on Docker
  - Cassandra: 5.0.9 (cassandra:latest)
  - Apache Spark: 3.1.9, Scala 2.12, Python 3.10.12 (spark:3.5.9-python3) ※
  - Python 3.14+ from uv

> [!NOTE]
> Apache Spark の最新バージョンは 4.2.1 でしたが、対応する Cassandra コネクタが存在しないため、3.x 系の Spark を使用しています。

## SMACK Stack is

 README にまとめたので省略します。

## Roles of Products

- Cassandraの役割：大量のデータを高速に「書き込む」「保存する」のが得意（リアルタイムのデータ蓄積）。しかし、複雑な集計や分析（例：全データの平均値を出す、複数のテーブルを結合する）は苦手。
- Sparkの役割：Cassandraに貯まった大量のデータを、メモリ上に一瞬で引っ張ってきて「超高速で集計・分析・加工」するのが得意。
- Kafkaの役割：リアルタイムに流れ続けるデータを高速に受け取り、必要に応じてSpark Streamingなどの処理系に渡すのが得意。データの一時的なバッファとしても機能します。

## Components of Apache Spark

> Apache Spark の主要コンポーネント

- Spark SQL: 構造化データを扱うための機能（DataFrame / Dataset API）やSQLクエリを提供します。
- Spark Streaming: リアルタイムに流れ続けるデータをミニバッチ方式で処理します。
- MLlib: 分類、回帰、クラスタリングなど標準的な機械学習アルゴリズムのライブラリです。
- GraphX: グラフ構造（人間関係やネットワーク等）を解析するライブラリです。

このうち、Cassandra と組み合わせてよく使われるのは次のパターンです。

- Spark → Cassandra = Spark Streaming でリアルタイムにデータを処理し、Cassandra に書き込むパターンです。
- Cassandra → Spark = Spark SQL で Cassandra に貯まった大量のデータを Spark で読み込み、分析・加工するパターンです。Cassandra は DataFrame として直接扱うことができます。

## Step 1: Real-time log accumulation system for pseudo IoT sensors

> 擬似的な IoT センサーからのログをリアルタイムに蓄積するシステムを構築するケースを考えます。

//TODO あとで図を入れる

### Start Cassandra and Spark Containers

devcontainer (dev) 内からホストの Docker を操作できる設定になっています。

- [See `compose.yaml`](../compose.yaml)

```bash
docker compose up -d
```

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

### Start the simulation

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
cqlsh:iot_keyspace> select * from sensor_logs;

 sensor_id | status  | temperature | timestamp
-----------+---------+-------------+---------------------------------
  sensor_2 | WARNING |    21.58791 | 2026-11-07 09:18:07.895000+0000
  sensor_1 |      OK |    21.71743 | 2026-11-07 09:18:09.895000+0000
  sensor_0 |      OK |    27.36492 | 2026-11-07 09:18:08.895000+0000
```
