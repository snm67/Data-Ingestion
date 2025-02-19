import json
import pika
import pandas as pd
import sqlite3
from contextlib import closing
from sqlite3 import Connection, Cursor

RABBITMQ_HOST = "localhost"
QUEUE_NAME = "event_queue"
DATABASE_NAME = "users.db" 

# Get a database connection
def get_db_connection() -> Connection:
    try:
        return sqlite3.connect(DATABASE_NAME)
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

# Insert a single record into the SQLite database
def insert_record(record):
    try:
        with get_db_connection() as connection:
            new_row = pd.DataFrame([record], columns=["id", "name", "age", "city", "created_date"])
            new_row.to_sql("users", connection, if_exists="append", index=False)
            print(f"Consumed event: {record}")
    except Exception as e:
        print(f"Error inserting record: {e}")

# Display all records in the SQLite database
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

# Callback function for processing messages from the queue
def callback(ch, method, properties, body):
    try:
        record = json.loads(body)  # Deserialize the JSON string
        insert_record(record)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"Error processing message: {e}")

# Main consumer logic
def consume_events():
    ensure_table_exists()

    connection_params = pika.ConnectionParameters(host=RABBITMQ_HOST)
    rabbit_connection = pika.BlockingConnection(connection_params)

    try:
        channel = rabbit_connection.channel()
        channel.queue_declare(queue=QUEUE_NAME)

        channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=False)
        print("Event Consumer started. Press Ctrl+C to stop.")
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\nEvent Consumer stopped.")
    except Exception as e:
        print(f"Error in RabbitMQ connection: {e}")
    finally:
        rabbit_connection.close()
        query_database()  # Call the new function to query the database

if __name__ == "__main__":
    consume_events()
