import socket

from kafka import KafkaProducer

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
    print(f"Received UDP: {message}")

    # Kafkaへ送信（バイト列で渡す）
    # producer.send(topic_name, value=message.encode("utf-8"))
    # bridge.py の送信部分をこれに差し替え
    future = producer.send(topic_name, value=message.encode("utf-8"))
    record_metadata = future.get(timeout=5)  # 送信完了を待ち、失敗したら例外を出す
    print(f"Sent to Kafka -> topic:{record_metadata.topic}, offset:{record_metadata.offset}")
    # producer.flush()
