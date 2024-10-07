
import json
import random
from datetime import datetime
from azure.storage.queue import QueueClient
from dotenv import load_dotenv
import os

# Function to generate random sample data
def generate_data():
    # Sample data fields
    data = {
        "id": random.randint(1000, 9999),  # Random ID between 1000 and 9999
        "temperature": round(random.uniform(10.0, 35.0), 2),  # Random temperature between 10 and 35
        "humidity": round(random.uniform(20.0, 80.0), 2),     # Random humidity between 20% and 80%
        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # Current date and time
    }
    return data

# Function to send data to Azure Queue
def send_data_to_queue(data):
    # Azure Storage Queue connection string and queue name
    connection_string = os.getenv("AZURE_QUEUE_CONNECTION_STRING") 
    queue_name = "data-queue"  # Your queue name here

    # Create a QueueClient
    queue_client = QueueClient.from_connection_string(conn_str=connection_string, queue_name=queue_name)

    # Send the message to the queue
    message = json.dumps(data)
    queue_client.send_message(message)
    print(f"Sent message to queue: {message}")

# Generate data and send to the queue
def produce_data():
    for _ in range(10):  # Produces 10 data entries
        data = generate_data()
        send_data_to_queue(data)  # Send data to the Azure queue

# Call the data producer function
if __name__ == "__main__":
    produce_data()
