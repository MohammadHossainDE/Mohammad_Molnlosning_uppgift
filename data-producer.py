

import json
import random
from datetime import datetime

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

# Generate data continuously (if needed)
def produce_data():
  with open('data_output.json', 'a') as f:
    for _ in range(10):  # Produces 10 data entries
        data = generate_data()
        json_data = json.dumps(data)  # Convert data to JSON format
        f.write(json_data + '\n') 
        print(json_data)  # Print the JSON data (or you could save it to a file)

# Call the data producer function
if __name__ == "__main__":
    produce_data() 




