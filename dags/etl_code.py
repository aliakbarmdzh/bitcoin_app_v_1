from json import load, dump
from typing import Any, Optional
import requests
import sys
import logging
import connections
import datetime


def get_time(unix_time: Optional[Any], second: int = 1000):
    return (
        datetime.datetime.utcfromtimestamp(int(unix_time/second)).isoformat()
        if unix_time
        else None
    )


def extract_exchange_data():
    url = 'https://api.coincap.io/v2/exchanges'
    response = requests.get(url)
    response.raise_for_status()
    if response.status_code != 204:
        try:
            resp = response.json().get('data', [])
            outfile = open("data.json", "w")
            dump(resp, outfile)
            outfile.close()
        except ValueError as e:
            return print('There is an error: {e}')


def transform_exchange_data():
    file = open('data.json',)
    data = load(file)
    for d in data:
        d['update_time'] = get_time(d.get('updated'))
        d['percentTotalVolume'] = float(d.get('percentTotalVolume'))
        d['rank'] = int(d.get('rank'))
        d['volumeUsd'] = float(d.get('volumeUsd'))
        d['tradingPairs'] = int(d.get('tradingPairs'))
        d.pop('updated')
    outfile = open("data_transformed.json", "w")
    dump(data, outfile)
    outfile.close()
    return


def load_exchange_data():
    file = open('data_transformed.json',)
    data = load(file)
    connections.insert_to_mongo(data)
    return
