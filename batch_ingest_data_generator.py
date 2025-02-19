import pandas as pd
from datetime import datetime
from faker import Faker
import uuid

OUTPUT_CSV_FILE = "user_data.csv"  # Output file for generated data
RECORDS_TO_GENERATE = 100         # Number of records to generate

# Initialize Faker
fake = Faker()

# Generate batch data and save it to a CSV file
def generate_data_to_csv():
    try:
        # Generate data
        data = [
            {
                "id": str(uuid.uuid4()),
                "name": fake.name(),
                "age": fake.random_int(min=18, max=80),
                "city": fake.city(),
                "created_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            for _ in range(RECORDS_TO_GENERATE)
        ]

        # Create a DataFrame and write to CSV
        df = pd.DataFrame(data)
        df.to_csv(OUTPUT_CSV_FILE, index=False)
        print(f"Generated {RECORDS_TO_GENERATE} records and saved to {OUTPUT_CSV_FILE}")

    except Exception as e:
        print(f"Error generating data: {e}")

if __name__ == "__main__":
    generate_data_to_csv()
