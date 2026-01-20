"""
MongoDB database connection module.
"""

import os
from pymongo import MongoClient
from functools import lru_cache

MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:admin123@localhost:27017/")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "ecommerce")


@lru_cache()
def get_mongo_client() -> MongoClient:
    """Get cached MongoDB client."""
    return MongoClient(MONGO_URI)


def get_database():
    """Get the MongoDB database instance."""
    client = get_mongo_client()
    return client[MONGO_DATABASE]


def get_collection(collection_name: str):
    """Get a specific collection."""
    db = get_database()
    return db[collection_name]
