import os
import json
import time
from datetime import datetime
from faker import Faker
import argparse

fake = Faker()

# Function to generate random vehicle events
def generate_vehicle_events():
    events = []
    for _ in range(5):  # Generate 5 random events
        event = {
            "vehicle_id": fake.uuid4(),
            "event_time": datetime.now().isoformat(),
            "event_source": fake.word(),
            "event_type": fake.word(),
            "event_value": str(fake.coordinate()),
            "event_extra_data": {
                "note": fake.sentence(),
                "boot_time": fake.random_number(digits=2),
                "emergency_call": fake.boolean()
            }
        }
        events.append(event)
    return events

# Function to generate random vehicle statuses
def generate_vehicle_status():
    statuses = []
    for _ in range(5):  # Generate 5 random statuses
        status = {
            "vehicle_id": fake.uuid4(),
            "report_time": fake.iso8601(),
            "status_source": fake.word(),
            "status": fake.word()
        }
        statuses.append(status)
    return statuses

# Function to save events or statuses to JSON files
def save_to_json(data, file_path):
    with open(file_path, 'w') as file:
        json.dump(data, file, indent=4)
        print(f"wrote {len(data)} records to {file_path}")

# Argument parser for specifying the data path
parser = argparse.ArgumentParser(description='Generate vehicle events and statuses JSON files.')
parser.add_argument('--data-path', type=str, help='Path to save vehicle events and statuses JSON files')
args = parser.parse_args()

if not args.data_path:
    raise ValueError("data_path must be provided.")

# makedir if not exists
if not os.path.exists(args.data_path):
    os.makedirs(args.data_path)

# Continuously generate and save events and statuses with a delay
while True:
    events_data = generate_vehicle_events()
    statuses_data = generate_vehicle_status()

    # Save events and statuses to JSON files
    save_to_json(events_data, os.path.join(args.data_path, f"vehicles_events_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"))
    save_to_json(statuses_data, os.path.join(args.data_path, f"vehicles_status_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"))

    # Sleep for 5 seconds (adjust as needed)
    time.sleep(5)
