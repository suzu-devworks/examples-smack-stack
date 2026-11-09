import socket

from kafka import KafkaProducer
from kafka.producer.future import RecordMetadata


def on_send_success(record_metadata: RecordMetadata) -> None:
    print(
        f"✅ Sent to Kafka -> "
        f"Topic: {record_metadata.topic}, "
        f"Partition: {record_metadata.partition}, "
        f"Offset: {record_metadata.offset}"
    )


def on_send_error(err: Exception) -> None:
    print(f"❌ Send Failed: {err}")


# Kafkaの設定
producer = KafkaProducer(bootstrap_servers=["kafka:9092"], request_timeout_ms=5000, max_block_ms=5000)

topic_name = "iot-topic"

# UDPソケットの設定
UDP_IP = "0.0.0.0"
UDP_PORT = 5005
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((UDP_IP, UDP_PORT))

print(f"UDP Bridge started. Listening on port {UDP_PORT}...")

while True:
    data, addr = sock.recvfrom(1024)
    message = data.decode("utf-8").strip()
    print(f"📥 Received UDP: {message}")

    # Kafkaへ送信（バイト列で渡す）
    producer.send(topic_name, value=message.encode("utf-8")).add_callback(on_send_success).add_errback(on_send_error)
