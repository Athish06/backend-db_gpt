import os
import json
import redis
from rq import Queue

redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
redis_conn = redis.from_url(redis_url)

# RQ queue for background tasks
task_queue = Queue('agent_tasks', connection=redis_conn)

def get_redis_connection():
    return redis_conn

def set_scratchpad(job_id: str, data: str, ttl_seconds: int = 3600):
    """Store short-term ReAct scratchpad state."""
    redis_conn.setex(f"scratchpad:{job_id}", ttl_seconds, data)

def get_scratchpad(job_id: str) -> str:
    """Retrieve scratchpad state."""
    data = redis_conn.get(f"scratchpad:{job_id}")
    return data.decode("utf-8") if data else None

def set_job_status(job_id: str, status: dict, ttl_seconds: int = 3600):
    """Store the status and final result of a background job."""
    redis_conn.setex(f"job_status:{job_id}", ttl_seconds, json.dumps(status))

def get_job_status(job_id: str) -> dict:
    """Retrieve the status of a background job."""
    data = redis_conn.get(f"job_status:{job_id}")
    return json.loads(data) if data else None
