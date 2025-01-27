import requests
import os
import json
# from config import api_configs

API_BASE = "https://newsapi.org/v2/top-headlines?country=us"
api_key = os.environ.get("NEWS_API_KEY")

def get_raw_data():
    path = 'landing_zone/raw_data.json'
    response = requests.get(API_BASE, headers={
        "X-Api-Key": api_key
    })
    data = response.json()
    with open(path, 'w') as f:
        json.dump(data, f)