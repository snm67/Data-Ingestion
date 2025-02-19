import time
import random
import json
import pika
from datetime import datetime
import uuid
from faker import Faker

# Initialize Faker
fake = Faker()

RABBITMQ_HOST = "localhost"
QUEUE_NAME = "event_queue"

# Get a RabbitMQ connection
def get_rabbitmq_connection():
    try:
        return pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    except Exception as e:
        print(f"Error connecting to RabbitMQ: {e}")
        raise

# Generate a single user record
def generate_user_record():
    return {
        "id": str(uuid.uuid4()),
        "name": fake.name(),
        "age": fake.random_int(min=18, max=80),
        "city": fake.city(),
        "created_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

# Produce and publish events in real-time
def produce_and_publish_events(channel, interval=1):
    try:
        while True:
            record = generate_user_record()
            channel.basic_publish(exchange="", routing_key=QUEUE_NAME, body=json.dumps(record))
            print(f"Produced event: {record}")
            time.sleep(random.uniform(0, 0.5))  # Simulation of stochastic events.
    except KeyboardInterrupt:
        print("\nEvent Producer stopped.")
    except Exception as e:
        print(f"Error during event production: {e}")

# Main logic
def main():
    try:
        with get_rabbitmq_connection() as connection:
            channel = connection.channel()
            produce_and_publish_events(channel)
    except Exception as e:
        print(f"Error in producer: {e}")

if __name__ == "__main__":
    main()
