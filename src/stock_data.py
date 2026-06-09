import requests 
import os
import json
from dotenv import load_dotenv

load_dotenv("D:/projects/exchange_rate_analytics/.env")

def get_stock_data():

    api_key = os.getenv("API_KEY")
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_MONTHLY&symbol=IBM&apikey={api_key}"

    try:
        response = requests.get(url=url)

        data = response.json()
        json_data = json.dumps(data, indent=4)
        print(json_data)
    except Exception as e:
        print(f"An error occurred: {e}")

get_stock_data()