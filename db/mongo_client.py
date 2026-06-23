from pymongo import MongoClient
import os
from typing import Optional

_client: Optional[MongoClient] = None

def get_project_db():
    global _client
    if _client is None:
        mongo_uri = os.getenv("MONGO_URI")
        if not mongo_uri or "<user>" in mongo_uri:
            # Fallback to a local mongodb instance for testing if no atlas string is provided
            mongo_uri = "mongodb://localhost:27017/"
        
        _client = MongoClient(mongo_uri)
    
    db_name = os.getenv("MONGO_DB_NAME", "dbgpt")
    return _client[db_name]

def init_db_indexes():
    """Create necessary collections and indexes if they don't exist."""
    try:
        db = get_project_db()
        # users collection
        db.users.create_index("email", unique=True)
        
        # conversations collection
        db.conversations.create_index([("user_id", 1), ("updated_at", -1)])
        db.conversations.create_index([("user_id", 1), ("db_id", 1), ("target", 1)])
        db.conversations.create_index("updated_at", expireAfterSeconds=2592000) # 30 days
        
        # schema_cache collection
        db.schema_cache.create_index([("user_id", 1), ("db_id", 1)], unique=True)
        db.schema_cache.create_index("expires_at", expireAfterSeconds=0)
        print("MongoDB indexes initialized successfully.")
    except Exception as e:
        print(f"Warning: Could not initialize MongoDB indexes: {e}")

def close_mongo_client():
    global _client
    if _client:
        _client.close()
        _client = None
