import socket

from confluent_kafka import KafkaError, Message, Producer  # type: ignore


def delivery_report(err: KafkaError, msg: Message) -> None:
    if err is not None:
        print(f"❌ Send Failed: {err}")
    else:
        # 📌 ここで完了メッセージを出します
        print(f"✅ Sent to Kafka -> Topic: {msg.topic()}, Partition: {msg.partition()}, Offset: {msg.offset()}")


# Kafkaの設定
producer_config = {
    "bootstrap.servers": "kafka:9092",
    # 1. ブローカーからの応答待ちタイムアウト (例: 5秒)
    "request.timeout.ms": 5000,
    # 2. メッセージ送信全体のタイムアウト。リトライも含めた上限 (例: 10秒)
    "message.timeout.ms": 10000,
    # 3. ネットワーク接続自体のタイムアウト (例: 10秒)
    "socket.timeout.ms": 10000,
    # 必要に応じてリトライ回数なども設定可能
    "retries": 3,
}
producer = Producer(producer_config)

topic_name = "iot-topic"

# UDPソケットの設定 (ポート 5005番で待ち受け)
UDP_IP = "0.0.0.0"
UDP_PORT = 5005
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((UDP_IP, UDP_PORT))

print(f"UDP Bridge started. Listening on port {UDP_PORT}...")

while True:
    # UDPデータの受信 (例: "sensor_1,25.4" のようなCSV形式のテキスト)
    data, addr = sock.recvfrom(1024)
    message = data.decode("utf-8").strip()
    print(f"📥 Received UDP from {addr}: {message}")

    # 受信したデータをそのままKafkaのトピックに送信
    producer.produce(topic_name, value=message, on_delivery=delivery_report)

    # 毎回 poll(0) を呼ぶことで、裏で完了したメッセージの delivery_report を実行させる
    producer.poll(0)
