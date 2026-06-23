from datetime import datetime, timedelta
from typing import Optional, Dict
from db.mongo_client import get_project_db

class SchemaCacheService:
    TTL_MINUTES = 60

    def get(self, user_id: str, db_id: str) -> Optional[Dict]:
        db = get_project_db()
        doc = db.schema_cache.find_one({
            "user_id": user_id,
            "db_id": db_id,
            "expires_at": {"$gt": datetime.utcnow()}
        })
        return doc  # None if not cached or expired

    def set(self, user_id: str, db_id: str, schema_data: Dict):
        db = get_project_db()
        now = datetime.utcnow()
        db.schema_cache.update_one(
            {"user_id": user_id, "db_id": db_id},
            {"$set": {
                **schema_data,
                "user_id": user_id,
                "db_id": db_id,
                "cached_at": now,
                "expires_at": now + timedelta(minutes=self.TTL_MINUTES)
            }},
            upsert=True
        )

    def invalidate(self, user_id: str, db_id: str):
        db = get_project_db()
        db.schema_cache.delete_one({"user_id": user_id, "db_id": db_id})

schema_cache_service = SchemaCacheService()
