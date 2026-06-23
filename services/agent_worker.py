import asyncio
import time
from bson import ObjectId
from db.mongo_client import get_project_db
from services.connector_factory import get_connector
from services.db_connector import DBConfig, DBType
from services.schema_cache import schema_cache_service
from services.conversation import conversation_manager
from services.agent import run_chat_turn
from services.redis_client import set_job_status, get_job_status

def _get_user_db_config(user_id: str, db_id: str) -> DBConfig:
    db = get_project_db()
    user = db.users.find_one({"_id": ObjectId(user_id)})
    if not user:
        raise ValueError("User not found")
        
    databases = user.get("databases", [])
    db_entry = next((db for db in databases if str(db["db_id"]) == db_id), None)
    
    if not db_entry:
        raise ValueError("Database not found")
        
    db_type_str = db_entry["type"]
    db_type = DBType.POSTGRESQL if db_type_str == "postgresql" else DBType.MONGODB if db_type_str == "mongodb" else DBType.SUPABASE
    
    from services.encryption import encryption_service
    password = encryption_service.decrypt(db_entry["password_encrypted"]) if db_entry.get("password_encrypted") else ""
    user_name = encryption_service.decrypt(db_entry["username_encrypted"]) if db_entry.get("username_encrypted") else ""
    connection_string = encryption_service.decrypt(db_entry["connection_string_encrypted"]) if db_entry.get("connection_string_encrypted") else ""

    return DBConfig(
        type=db_type,
        host=db_entry.get("host", ""),
        port=db_entry.get("port", 5432),
        database_name=db_entry.get("database_name", ""),
        username=user_name,
        password=password,
        ssl_required=db_entry.get("ssl_required", False),
        connection_string=connection_string
    )

def execute_agent_task(job_id: str, user_id: str, db_id: str, target: str, message: str, conversation_id: str, groq_api_key: str, resume_state: dict = None):
    """
    Background worker task to run the agent loop.
    """
    try:
        set_job_status(job_id, {"status": "running"})
        
        db_config = _get_user_db_config(user_id, db_id)
        
        if conversation_id:
            conversation = conversation_manager.get_conversation(user_id, conversation_id)
        else:
            conversation = conversation_manager.create_new_conversation(
                user_id, db_id, db_config.database_name, db_config.type.value if hasattr(db_config.type, 'value') else db_config.type, target
            )
            conversation_id = str(conversation["_id"])
            
        if not resume_state:
            conversation_manager.append_message(conversation_id, "user", message)
        
        schema_data = schema_cache_service.get(user_id, db_id)
        connector = get_connector(db_config)
        
        if not schema_data:
            tables = connector.get_tables_or_collections()
            schemas = {}
            for t in tables:
                try:
                    schemas[t] = connector.get_schema(t)
                except Exception as e:
                    print(f"Failed to fetch schema for {t}: {e}")
                
            schema_data = {
                "sql_schemas": schemas if db_config.type in (DBType.POSTGRESQL, DBType.SUPABASE) else {},
                "mongo_schemas": schemas if db_config.type == DBType.MONGODB else {}
            }
            schema_cache_service.set(user_id, db_id, schema_data)

        # Run agent loop
        # We pass job_id so the agent can update the short-term scratchpad
        result = asyncio.run(run_chat_turn(
            message,
            {**db_config.__dict__, "type": db_config.type.value if hasattr(db_config.type, 'value') else db_config.type},
            target,
            schema_data,
            conversation,
            groq_api_key,
            connector,
            job_id=job_id,
            resume_state=resume_state
        ))
        
        connector.close()

        if result.get("status") == "paused_rate_limit":
            # State frozen. Save to DB.
            conversation_manager.save_paused_state(conversation_id, result)
            set_job_status(job_id, {
                "status": "paused_rate_limit",
                "conversation_id": conversation_id
            }, ttl_seconds=3600)
            return
            
        # If it was a resume, clear the paused state upon success
        if resume_state:
            conversation_manager.clear_paused_state(conversation_id)
        
        conversation_manager.append_message(
            conversation_id, 
            "assistant", 
            result["reply"], 
            {
                "generated_query": result.get("generated_query"),
                "query_type": result.get("query_type"),
                "result_row_count": result.get("result_row_count"),
                "execution_time_ms": result.get("execution_time_ms"),
                "error": result.get("error")
            }
        )
        
        conversation_manager.maybe_compress(conversation_id, groq_api_key)
        
        # Mark as complete and store final result to be polled by frontend
        set_job_status(job_id, {
            "status": "completed",
            "conversation_id": conversation_id,
            "result": result
        }, ttl_seconds=3600)
        
    except Exception as e:
        print(f"Agent Task Error: {e}")
        set_job_status(job_id, {"status": "failed", "error": str(e)}, ttl_seconds=3600)
