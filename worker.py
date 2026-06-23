import os
import sys
from dotenv import load_dotenv

# Load env variables before anything else
load_dotenv()

from rq import Worker, Queue, Connection
from services.redis_client import redis_conn

listen = ['agent_tasks']

if __name__ == '__main__':
    with Connection(redis_conn):
        worker = Worker(map(Queue, listen))
        print("Starting RQ worker for agent tasks...")
        worker.work()
