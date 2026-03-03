import os
import requests
import psycopg
from dotenv import load_dotenv

load_dotenv()

def request_function():
    url = "https://open.er-api.com/v6/latest/USD"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    print(response.json())
    return response.json()

if __name__ == "__main__":
    try:
        raw_data = request_function()
    except Exception as e:
        print(f"ERROR: {e}")
        exit(1)