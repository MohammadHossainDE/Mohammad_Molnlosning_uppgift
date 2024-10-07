
import json
import pyodbc
from azure.storage.queue import QueueClient
from dotenv import load_dotenv
import os

def get_data_from_queue():
    queue_name = "data-queue"
    connection_string = os.getenv("AZURE_QUEUE_CONNECTION_STRING")  # Replace with your connection string

    queue_client = QueueClient.from_connection_string(conn_str=connection_string, queue_name=queue_name)
    messages = queue_client.receive_messages()

    for message in messages:
        print(f"Raw message content: {message.content}")  # Log raw message for debugging
        process_message(message)
        queue_client.delete_message(message)  # Remove message from queue after processing

def process_message(message):
    data = json.loads(message.content)
    print(f"Parsed data: {data}")  # Log parsed data for debugging

    # Check if required fields are present
    if 'id' in data and 'temperature' in data and 'humidity' in data and 'timestamp' in data:
        insert_data_to_sql(data)
    else:
        print(f"Skipped inserting due to missing critical data: {data}")

def insert_data_to_sql(data):
    server = os.getenv("SQL_SERVER")  # Use environment variable
    database = os.getenv("SQL_DATABASE")  # Use environment variable
    username = os.getenv("SQL_USERNAME")  # Use environment variable
    password = os.getenv("SQL_PASSWORD")  # Use environment variable
    driver = '{ODBC Driver 17 for SQL Server}'

    conn_str = (
        'DRIVER=' + driver + ';'
        'SERVER=' + server + ';'
        'PORT=1433;'
        'DATABASE=' + database + ';'
        'UID=' + username + ';'
        'PWD=' + password + ';'
        'Encrypt=yes;'
        'TrustServerCertificate=no;'
        'Connection Timeout=30;'
    )

    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Adjust the SQL insert statement to match the data structure
    insert_query = """
    INSERT INTO SensorData (id, temperature, humidity, timestamp)
    VALUES (?, ?, ?, ?)
    """
    cursor.execute(insert_query, (data['id'], data['temperature'], data['humidity'], data['timestamp']))
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Inserted data: {data}")

if __name__ == "__main__":
    get_data_from_queue()


