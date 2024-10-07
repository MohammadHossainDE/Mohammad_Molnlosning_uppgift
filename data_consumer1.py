import pyodbc
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter  # Import DateFormatter
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

# Database connection function
def connect_to_database():
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
        'Connection Timeout=60;'  # Increased timeout
    )
    try:
        print("Attempting to connect to the database...")
        conn = pyodbc.connect(conn_str)
        print("Successfully connected to the database.")
        return conn
    except pyodbc.Error as e:
        print(f"Error connecting to database: {e}")
        print(f"Connection string used: {conn_str}")
        return None

# Function to fetch data from the database
def fetch_data(conn, hours=72):
    cursor = conn.cursor()
    try:
        time_threshold = datetime.now() - timedelta(hours=hours)
        query = """
        SELECT id, temperature, humidity, timestamp
        FROM SensorData
        WHERE timestamp > ?
        ORDER BY timestamp
        """
        print(f"Executing query: {query}")
        print(f"With time threshold: {time_threshold}")
        cursor.execute(query, (time_threshold,))
        data = cursor.fetchall()
        
        if not data:
            print(f"No data found in the last {hours} hours. Fetching all available data.")
            query = """
            SELECT TOP 1000 id, temperature, humidity, timestamp
            FROM SensorData
            ORDER BY timestamp DESC
            """
            print(f"Executing query: {query}")
            cursor.execute(query)
            data = cursor.fetchall()
        
        if data:
            print("First row of data:")
            print(data[0])
            print("Number of columns:", len(data[0]))
            print("Column types:", [type(item) for item in data[0]])
            print(f"Total rows fetched: {len(data)}")
        else:
            print("No data retrieved from the database.")
        
        return data
    except pyodbc.Error as e:
        print(f"Error fetching data: {e}")
        return None
    finally:
        cursor.close()

# Function to print data summary
def print_data_summary(data):
    if not data:
        print("No data available.")
        return

    print("\nRaw data structure:")
    unpacked_data = [list(row) for row in data]  # Convert to list of lists
    for i, row in enumerate(unpacked_data[:5]):  # Print first 5 rows
        print(f"Row {i}: {row}")
        print(f"Row {i} type: {type(row)}")
        print(f"Row {i} length: {len(row)}")
        print()

    try:
        df = pd.DataFrame(unpacked_data)  # Create DataFrame from unpacked data
        print("\nDataFrame created successfully.")
        print("\nDataFrame shape:", df.shape)
        print("\nColumn names:")
        print(df.columns)
        print("\nFirst few records:")
        print(df.head())
        print("\nData types:")
        print(df.dtypes)
        
        # Rename columns and convert data types
        if df.shape[1] == 4:
            df.columns = ['id', 'temperature', 'humidity', 'timestamp']
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
            df['temperature'] = pd.to_numeric(df['temperature'], errors='coerce')
            df['humidity'] = pd.to_numeric(df['humidity'], errors='coerce')
            df['id'] = pd.to_numeric(df['id'], errors='coerce')
            
            print("\nAfter data type conversion:")
            print(df.dtypes)
            print("\nDescriptive statistics:")
            print(df.describe())
            print("\nDate range:")
            print(f"From: {df['timestamp'].min()}")
            print(f"To: {df['timestamp'].max()}")
        else:
            print(f"Unexpected number of columns: {df.shape[1]}")
    except Exception as e:
        print(f"Error in print_data_summary: {e}")

# Function to plot the data
# Function to plot the data
def plot_data(data):
    if not data:
        print("No data available to plot.")
        return

    try:
        # Create DataFrame
        unpacked_data = [list(row) for row in data]  # Convert to list of lists
        df = pd.DataFrame(unpacked_data)
        if df.shape[1] != 4:
            print(f"Unexpected number of columns: {df.shape[1]}. Cannot create plot.")
            return

        df.columns = ['id', 'temperature', 'humidity', 'timestamp']
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['temperature'] = pd.to_numeric(df['temperature'], errors='coerce')
        df['humidity'] = pd.to_numeric(df['humidity'], errors='coerce')
        df['id'] = pd.to_numeric(df['id'], errors='coerce')

        # Create a figure with three subplots
        fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 15), sharex=True)

        # 1. Line chart for temperature and humidity by ID
        ax1.plot(df['id'], df['temperature'], label='Temperature', marker='o')
        ax1.plot(df['id'], df['humidity'], label='Humidity', marker='o')
        ax1.set_ylabel('Value')
        ax1.set_title('Temperature and Humidity by ID')
        ax1.legend()

        # 2. Bar chart for temperature and humidity
        bar_width = 0.35
        index = range(len(df))
        ax2.bar(index, df['temperature'], width=bar_width, label='Temperature', color='b', align='center')
        ax2.bar([i + bar_width for i in index], df['humidity'], width=bar_width, label='Humidity', color='g', align='center')
        ax2.set_ylabel('Value')
        ax2.set_title('Temperature and Humidity Bar Chart')
        ax2.set_xticks([i + bar_width / 2 for i in index])
        ax2.set_xticklabels(df['id'])
        ax2.legend()

        # 3. Scatter plot for Temperature vs Humidity
        scatter = ax3.scatter(df['temperature'], df['humidity'], c=df['timestamp'], cmap='viridis')
        ax3.set_xlabel('Temperature')
        ax3.set_ylabel('Humidity')
        ax3.set_title('Temperature vs Humidity')
        plt.colorbar(scatter, ax=ax3, label='Time')

        # Format x-axis for the first two subplots
        ax1.set_xticks(df['id'])  # Set x-ticks to ID numbers
        ax1.set_xticklabels(df['id'], rotation=45)  # Rotate x-tick labels for better visibility

        plt.tight_layout()
        plt.savefig('sensor_data_visualization.png')
        print("Plot saved as sensor_data_visualization.png")
    except Exception as e:
        print(f"Error in plot_data: {e}")

# Main function
def main():
    conn = connect_to_database()
    if not conn:
        print("Failed to connect to the database. Exiting.")
        return

    try:
        print("Fetching data from the database...")
        data = fetch_data(conn, hours=72)  # Fetch last 72 hours of data
        if data:
            print("Data fetched successfully. Printing summary...")
            print_data_summary(data)
            print("Generating plot...")
            plot_data(data)
            print("Script execution completed successfully.")
        else:
            print("No data available. Script execution completed.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        print("Closing database connection...")
        conn.close()
        print("Database connection closed.")

if __name__ == "__main__":
    main()