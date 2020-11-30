from datetime import datetime
import requests
import json


def retrieve_data(url):
    res = requests.get(url)
    data = json.loads(res.text)
    return data


def retrieve_date():
    date = datetime.today().strftime('%Y-%m-%d')
    return date
