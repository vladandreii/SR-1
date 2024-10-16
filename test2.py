from recombee_api_client.api_client import RecombeeClient, Region
from recombee_api_client.api_requests import *
import json
import time
from datetime import datetime

# Initialize the Recombee client
client = RecombeeClient('upb1-dev', 'y4Q07Edn6S1GS1DxCPweJBqj00Gw73sSfTcsoFuQLnrA5FZgcsWaZEIXbbCIz4i0', region=Region.EU_WEST)

# Reset the database
client.send(ResetDatabase())

# Wait for the reset to complete
time.sleep(5)

# Define item properties
client.send(AddItemProperty('name', 'string'))
client.send(AddItemProperty('model', 'string'))  # Added model property
client.send(AddItemProperty('year', 'int'))      # Changed year to int
client.send(AddItemProperty('displacement', 'double'))
client.send(AddItemProperty('horsepower', 'int'))

# Create requests for all items
requests = []
with open('cars.json') as f:
    data = json.loads(f.read())
    for index, item in enumerate(data):
        item_id = f"item_{index}"  # Generate a unique ID based on the index
        
        # Create a new dictionary for the item
        item_values = {
            "name": item.get("Name"),
            "model": item.get("Model"),  # Include model from the dataset
            "year": item.get("Year"),     # Use year as integer
            "displacement": item.get("Displacement"),
            "horsepower": item.get("Horsepower")
        }
        
        # Set item values using the generated ID and item data
        r = SetItemValues(item_id, item_values, cascade_create=True)
        requests.append(r)

# Define max number of retries and attempt counter
max_retries = 5
attempt = 0
success = False

# While loop to retry batch requests in case of failure
while attempt < max_retries and not success:
    try:
        res = client.send(Batch(requests))
        print(res)
        success = True  # If no exception, set success to True to exit the loop
    except Exception as e:
        print(f"Attempt {attempt + 1} failed with error: {e}")
        attempt += 1
        time.sleep(2)  # Wait for 2 seconds before retrying

if not success:
    print(f"Failed to send requests after {max_retries} attempts")
