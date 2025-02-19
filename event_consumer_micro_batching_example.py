import json
import pika
import pandas as pd
import sqlite3
from contextlib import closing
from sqlite3 import Connection

# Global variables
RABBITMQ_HOST = "localhost"
QUEUE_NAME = "event_queue"
SQLITE_DB = "users.db"  # Global variable for the SQLite database file name
BATCH_SIZE = 5 # Adjustable micro-batch size.

# Get a database connection
def get_db_connection() -> Connection:
    try:
        return sqlite3.connect(SQLITE_DB)
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
        raise

# Ensure the users table exists
def ensure_table_exists():
    create_table_query = """
    CREATE TABLE IF NOT EXISTS users (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        age INTEGER NOT NULL,
        city TEXT NOT NULL,
        created_date TIMESTAMP NOT NULL
    );
    """
    try:
        with get_db_connection() as connection:
            with closing(connection.cursor()) as cursor:
                cursor.execute(create_table_query)
    except sqlite3.Error as e:
        print(f"Error ensuring table exists: {e}")
        raise

# Batch insert records into SQLite
def insert_batch_records(batch_records):
    try:
        with get_db_connection() as connection:
            df = pd.DataFrame(batch_records, columns=["id", "name", "age", "city", "created_date"])
            df.to_sql("users", connection, if_exists="append", index=False, dtype={"created_date": "TIMESTAMP"})
            for record in batch_records:
                print(f"Consumed event: {record}")
    except Exception as e:
        print(f"Error inserting batch records: {e}")

# Display all records in the database
def query_database():
    try:
        with get_db_connection() as connection:
            with closing(connection.cursor()) as cursor:
                print("\nFinal data in the users table (SQLite):")
                rows = cursor.execute("SELECT * FROM users").fetchall()
                for row in rows:
                    print(row)
    except Exception as e:
        print(f"Error querying database: {e}")

# Callback function to process messages in batches
def callback_batch_consumer(ch, method_buffer, body_buffer):
    try:
        # Deserialize and prepare records for batch insertion
        records = [json.loads(body) for body in body_buffer]
        print(f"\nConsumed batch of {len(records)} events.")

        # Insert batch records into SQLite
        insert_batch_records(records)

        # Acknowledge all messages in the batch
        for method_item in method_buffer:
            ch.basic_ack(delivery_tag=method_item.delivery_tag)
    except Exception as e:
        print(f"Error processing batch: {e}")

# Main consumer logic
def consume_events():
    ensure_table_exists()

    connection_params = pika.ConnectionParameters(host=RABBITMQ_HOST)
    rabbit_connection = pika.BlockingConnection(connection_params)

    try:
        channel = rabbit_connection.channel()

        # Configure basic QoS for batch processing
        channel.basic_qos(prefetch_count=BATCH_SIZE)

        # Prepare batch buffers
        body_buffer = []
        method_buffer = []

        def on_message(ch, method, properties, body):
            nonlocal body_buffer, method_buffer

            body_buffer.append(body)
            method_buffer.append(method)

            # Process batch when buffer size reaches threshold
            if len(body_buffer) >= BATCH_SIZE:
                callback_batch_consumer(ch, method_buffer, body_buffer)
                body_buffer.clear()
                method_buffer.clear()

        channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message, auto_ack=False)
        print("Micro-batch Event Consumer started. Press Ctrl+C to stop.")
        channel.start_consuming()

    except KeyboardInterrupt:
        query_database()
        print("\nMicro-batch Event Consumer stopped.")
    except Exception as e:
        print(f"Error in RabbitMQ connection: {e}")
    finally:
        rabbit_connection.close()

if __name__ == "__main__":
    consume_events()
