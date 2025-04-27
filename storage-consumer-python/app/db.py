# # app/db.py

from pymongo import MongoClient
from app.config import MONGO_URI, MONGO_DB, MONGO_COLLECTION

class MongoDB:
    def __init__(self):
        self.client = MongoClient(MONGO_URI)
        self.db = self.client[MONGO_DB]
        self.collection = self.db[MONGO_COLLECTION]

    def save_metric(self, metric):
        """Save a storage metric document to MongoDB."""
        result = self.collection.insert_one(metric)
        print(f"Inserted document with _id: {result.inserted_id}")
