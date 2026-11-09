import argparse
import random
import socket
import time
from datetime import UTC, datetime

parser = argparse.ArgumentParser(description="IoT Device Simulator")
parser.add_argument("--host", default="127.0.0.1", help="Target UDP host (default: 127.0.0.1)")
parser.add_argument("--port", type=int, default=5005, help="Target UDP port (default: 5005)")
args = parser.parse_args()

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
print(f"Device simulator sending UDP to {args.host}:{args.port}...")

while True:
    sensor_id = f"sensor_{random.randint(0, 2)}"
    temp = round(random.uniform(20.0, 30.0), 2)
    event_time = datetime.now(UTC).isoformat()
    message = f"{sensor_id},{temp},{event_time}"

    sock.sendto(message.encode("utf-8"), (args.host, args.port))
    print(f"📤 Sent UDP: {message}")
    time.sleep(1)
