import pika

RABBITMQ_HOST = "localhost"
QUEUE_NAME = "event_queue"

# Get a RabbitMQ connection
def get_rabbitmq_connection():
    try:
        return pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    except Exception as e:
        print(f"Error connecting to RabbitMQ: {e}")
        raise

# Initialize the queue
def initialize_queue():
    try:
        with get_rabbitmq_connection() as connection:
            channel = connection.channel()

            # Declare the queue
            channel.queue_declare(queue=QUEUE_NAME)
            print(f"Queue '{QUEUE_NAME}' initialized successfully.")

    except Exception as e:
        print(f"Error initializing queue: {e}")

if __name__ == "__main__":
    initialize_queue()
