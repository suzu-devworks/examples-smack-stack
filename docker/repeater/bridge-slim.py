import argparse
import socket

from kafka import KafkaProducer  # type: ignore
from kafka.producer.future import RecordMetadata  # type: ignore

parser = argparse.ArgumentParser(description="bridge for UDP to Kafka")
parser.add_argument("--host", default="127.0.0.1", help="Listening host for UDP traffic (default: 127.0.0.1)")
parser.add_argument("--port", type=int, default=5005, help="Listening port for UDP traffic (default: 5005)")
args = parser.parse_args()


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
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((args.host, args.port))
print(f"UDP Bridge started. Listening on {args.host}:{args.port} ...")

while True:
    data, addr = sock.recvfrom(1024)
    message = data.decode("utf-8").strip()
    print(f"📥 Received UDP: {message}")

    # Kafkaへ送信（バイト列で渡す）
    producer.send(topic_name, value=message.encode("utf-8")).add_callback(on_send_success).add_errback(on_send_error)
