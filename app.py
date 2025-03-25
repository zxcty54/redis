import os
import time
import redis
import requests
from supabase import create_client, Client
from flask import Flask, jsonify

# Set up Supabase URL and Key from environment variables
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_API_KEY = os.getenv("SUPABASE_API_KEY")

# Set up Redis connection
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = os.getenv("REDIS_PORT")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

# Create Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_API_KEY)

# Set up Redis client
redis_client = redis.StrictRedis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASSWORD,
    db=0,
    decode_responses=True
)

# Initialize Flask app
app = Flask(__name__)

# Function to fetch data from Supabase and store it in Redis
def fetch_and_store_data():
    try:
        # Fetch data from Supabase (adjust query based on your requirements)
        response = supabase.from_("live_prices").select("stock, change").execute()
        
        # Check for errors in the response
        if response.error:
            print(f"Error fetching data from Supabase: {response.error}")
            return
        
        # Process and store in Redis
        data = response.data
        for stock in data:
            stock_name = stock["stock"]
            stock_change = stock["change"]
            
            # Store stock change in Redis (use stock name as the key)
            redis_client.set(stock_name, stock_change)
        
        print("Data successfully fetched and stored in Redis.")
    
    except Exception as e:
        print(f"Error: {e}")

# Schedule the data fetching process every 30 minutes
def scheduled_fetch():
    while True:
        fetch_and_store_data()
        time.sleep(1800)  # Sleep for 30 minutes (1800 seconds)

# Route to fetch data from Redis (frontend will call this endpoint)
@app.route("/stocks", methods=["GET"])
def get_stocks():
    try:
        # Fetch all stock data from Redis
        all_stocks = redis_client.keys('*')  # Get all keys (stock names)
        
        stocks = []
        for stock in all_stocks:
            stock_name = stock
            stock_change = redis_client.get(stock_name)
            stocks.append({"stock": stock_name, "change": stock_change})
        
        return jsonify(stocks), 200
    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"error": "Failed to fetch data"}), 500

if __name__ == "__main__":
    # Run the scheduled fetch in a separate thread or process
    from threading import Thread
    fetch_thread = Thread(target=scheduled_fetch, daemon=True)
    fetch_thread.start()

    # Run the Flask app
    app.run(host='0.0.0.0', port=5000)
