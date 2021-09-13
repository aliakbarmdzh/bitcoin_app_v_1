import os
from pymongo import MongoClient, database
from decouple import config
from airflow.models import Variable


def insert_to_mongo(data):
    MONGO_USER = Variable.get('MONGO_USER')
    MONGO_PASSWORD = Variable.get('MONGO_PASSWORD')
    uri_mongo = 'mongodb+srv://{0}:{1}@cluster0.kyioa.mongodb.net/my_bitcoin_app?retryWrites=true&w=majority'.format(
        MONGO_USER, MONGO_PASSWORD)
    client = MongoClient(uri_mongo)
    db = client.my_bitcoin_app
    db.bitcoin_collection.insert_many(data)
    return
