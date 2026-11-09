import argparse
import socket
import time
from datetime import UTC, datetime, timedelta

parser = argparse.ArgumentParser(description="Watermark tester")
parser.add_argument("--host", default="127.0.0.1", help="Target UDP host (default: 127.0.0.1)")
parser.add_argument("--port", type=int, default=5005, help="Target UDP port (default: 5005)")
args = parser.parse_args()

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
print(f"Device simulator sending UDP to {args.host}:{args.port}...")


def send_data(sensor_id: str, temp: float, event_time: datetime, label: str) -> None:
    message = f"{sensor_id},{temp},{event_time.isoformat()}"
    sock.sendto(message.encode("utf-8"), (args.host, args.port))
    print(f"[{label}] Sent -> {message}")


print("=== Watermark operation confirmation test started ===")

# Record the reference time (e.g. 12:00:00)
base_time = datetime.now(UTC)
target_window_str = base_time.strftime("%H:%M")

# ------------------------------------------------------
# Step 1: Send normal data at reference time (temperature 20.0℃)
# ------------------------------------------------------
print("")
print(f"1. Send 20.0℃ to the reference window ({target_window_str} units)...")
send_data("sensor_test", 20.0, base_time, "normal data")
time.sleep(5)

# ------------------------------------------------------
# Step 2: Advance the time and push the watermark up a little
# (*Watermark is calculated based on "latest time received so far - 2 minutes")
# ------------------------------------------------------
print("")
print("2. Send data after 1 minute and 30 seconds and advance the clock in the system...")
future_time_1 = base_time + timedelta(minutes=1, seconds=30)
send_data("sensor_test", 25.0, future_time_1, "time progression 1")
time.sleep(5)

# ------------------------------------------------------
# Step 3: Send data with a delay of 1 minute (within tolerance: within 2 minutes)
# Since the temperature is 100.0℃, it should be averaged with the original 20.0℃ and updated to "60.0℃"!
# ------------------------------------------------------
print("")
print(f"3. Send lateness data (100.0℃) within the allowable range to {target_window_str} units...")
late_valid_time = base_time + timedelta(seconds=10)  # Time slightly later in the same 1 minute window
send_data("sensor_test", 100.0, late_valid_time, "valid late")
time.sleep(10)  # Wait for Spark micro batch processing

# ------------------------------------------------------
# Step 4: Advance the time by another 3 minutes (watermark completely passes reference window)
# ------------------------------------------------------
print("")
print("4. Advance time further and close old windows...")
future_time_2 = base_time + timedelta(minutes=4)
send_data("sensor_test", 30.0, future_time_2, "time progression 2")
time.sleep(10)

# ------------------------------------------------------
# Step 5: Send expired tardiness data (outside tolerance: more than 2 minutes)
# Temperature 999.0℃ will be sent, but since it will be discarded, the average should remain ``60.0℃''!
# ------------------------------------------------------
print("")
print(f"5. Send expired tardiness data (999.0℃) to {target_window_str} units...")
late_expired_time = base_time + timedelta(seconds=20)
send_data("sensor_test", 999.0, late_expired_time, "late expired")
time.sleep(10)

print("")
print("=== Test submission completed! Check the Cassandra results ===")
