import os
from dotenv import load_dotenv
load_dotenv()

from flask import Flask, request, jsonify, g
from flask_cors import CORS
from bson import ObjectId, json_util
import json
from datetime import datetime

from auth import login_api, signup_api, require_auth
from db.mongo_client import get_project_db, init_db_indexes
from services.connector_factory import get_connector
from services.db_connector import DBConfig, DBType
from services.encryption import encryption_service
from services.schema_cache import schema_cache_service
import asyncio
from services.conversation import conversation_manager
from services.redis_client import task_queue, get_job_status
import uuid

app = Flask(__name__)
FRONTEND_URL = os.environ.get("FRONTEND_URL")
if FRONTEND_URL:
    CORS(app, supports_credentials=True, origins=[FRONTEND_URL])
else:
    CORS(app, supports_credentials=True)

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy"}), 200

# Initialize MongoDB collections and indexes lazily to prevent Werkzeug Windows socket crashes
_indexes_initialized = False

@app.before_request
def initialize_indexes_once():
    global _indexes_initialized
    if not _indexes_initialized:
        init_db_indexes()
        _indexes_initialized = True

def _get_user_db_config(user_id: str, db_id: str) -> DBConfig:
    db = get_project_db()
    user = db.users.find_one({"_id": ObjectId(user_id)})
    if not user:
        raise ValueError("User not found")
        
    db_entry = next((d for d in user.get("databases", []) if str(d["db_id"]) == db_id), None)
    if not db_entry:
        raise ValueError("Database not found")
        
    return DBConfig(
        type=DBType(db_entry["type"]),
        host=db_entry["host"],
        port=db_entry["port"],
        database_name=db_entry["database_name"],
        username=encryption_service.decrypt(db_entry["username_encrypted"]) if db_entry.get("username_encrypted") else "",
        password=encryption_service.decrypt(db_entry["password_encrypted"]) if db_entry.get("password_encrypted") else "",
        ssl_required=db_entry.get("ssl_required", False),
        connection_string=(
            encryption_service.decrypt(db_entry["connection_string_encrypted"])
            if db_entry.get("connection_string_encrypted") else None
        )
    )

@app.route('/connect_db', methods=['POST'])
@require_auth
def connect_db():
    data = request.get_json()
    db_id = data.get("db_id")
    if not db_id:
        return jsonify({"status": "error", "message": "Missing db_id"}), 400
        
    try:
        db_config = _get_user_db_config(g.user_id, db_id)
        connector = get_connector(db_config)
        success, error = connector.test_connection()
        connector.close()
        
        if success:
            return jsonify({"status": "success", "message": "Database connection successful."}), 200
        else:
            return jsonify({"status": "error", "message": error}), 400
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 400

@app.route('/tables', methods=['POST'])
@require_auth
def view_tables():
    """
    Expects JSON with db_id.
    Returns list of table/collection names.
    """
    data = request.get_json()
    db_id = data.get("db_id")
    if not db_id:
        return jsonify({"error": "Missing db_id"}), 400
        
    try:
        db_config = _get_user_db_config(g.user_id, db_id)
        connector = get_connector(db_config)
        tables = connector.get_tables_or_collections()
        connector.close()
        return jsonify({"tables": tables})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/db_summary', methods=['POST'])
@require_auth
def view_db_summary():
    """
    Expects JSON with db_id.
    Returns summary of all tables (name, column count, row count).
    """
    data = request.get_json()
    db_id = data.get("db_id")
    if not db_id:
        return jsonify({"error": "Missing db_id"}), 400
        
    try:
        db_config = _get_user_db_config(g.user_id, db_id)
        schema_data = schema_cache_service.get(g.user_id, db_id)
        
        if not schema_data:
            connector = get_connector(db_config)
            tables = connector.get_tables_or_collections()
            schemas = {}
            for t in tables:
                try:
                    schemas[t] = connector.get_schema(t)
                except Exception as e:
                    print(f"Failed to fetch schema for {t}: {e}")
            connector.close()
            schema_data = {
                "sql_schemas": schemas if db_config.type in (DBType.POSTGRESQL, DBType.SUPABASE) else {},
                "mongo_schemas": schemas if db_config.type == DBType.MONGODB else {}
            }
            schema_cache_service.set(g.user_id, db_id, schema_data)

        summary = []
        if db_config.type in (DBType.POSTGRESQL, DBType.SUPABASE):
            for t_name, s_data in schema_data.get("sql_schemas", {}).items():
                summary.append({
                    "table_name": t_name,
                    "columns_count": len(s_data.get("columns", [])),
                    "row_count": s_data.get("row_count_approx", 0)
                })
        else:
            for c_name, s_data in schema_data.get("mongo_schemas", {}).items():
                summary.append({
                    "table_name": c_name,
                    "columns_count": len(s_data.get("fields", {}).keys()),
                    "row_count": s_data.get("document_count", 0)
                })
                
        return jsonify({"summary": summary})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/table/<table_name>', methods=['POST'])
@require_auth
def view_table_data(table_name):
    """
    Expects JSON with db_id.
    Returns data from the specified table.
    """
    data = request.get_json()
    db_id = data.get("db_id")
    if not db_id:
        return jsonify({"error": "Missing db_id"}), 400
        
    try:
        db_config = _get_user_db_config(g.user_id, db_id)
        connector = get_connector(db_config)
        
        if db_config.type in (DBType.POSTGRESQL, DBType.SUPABASE):
            rows, _ = connector.execute_sql(f"SELECT * FROM {table_name} LIMIT 100")
            connector.close()
            return jsonify(rows)
        elif db_config.type == DBType.MONGODB:
            rows, _ = connector.execute_mongodb_find(table_name, {}, {}, {}, 100)
            connector.close()
            # Safely serialize nested BSON types like ObjectId and datetimes
            safe_rows = json.loads(json_util.dumps(rows))
            return jsonify(safe_rows)
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/add_data', methods=['POST'])
@require_auth
def add_data_api():
    data = request.get_json()
    db_id = data.get('db_id')
    table_name = data.get('table_name')
    row_data = data.get('data')
    
    if not db_id or not table_name or not row_data:
        return jsonify({"success": False, "error": "Missing required fields"}), 400
        
    try:
        db_config = _get_user_db_config(g.user_id, db_id)
        connector = get_connector(db_config)
        success, error = connector.insert_row(table_name, row_data)
        connector.close()
        
        if success:
            return jsonify({"success": True}), 200
        else:
            return jsonify({"success": False, "error": error}), 500
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/login', methods=['POST'])
def login():
    return login_api()

@app.route('/signup', methods=['POST'])
def signup():
    return signup_api()

@app.route('/databases', methods=['GET'])
@require_auth
def view_databases():
    """
    Returns list of database details for the current user.
    """
    try:
        db = get_project_db()
        user = db.users.find_one({"_id": ObjectId(g.user_id)})
        
        if not user:
            return jsonify({"error": "User not found"}), 404
            
        databases = user.get("databases", [])
        safe_databases = []
        for db_entry in databases:
            safe_db = {
                "db_id": str(db_entry["db_id"]),
                "display_name": db_entry.get("display_name", db_entry["database_name"]),
                "type": db_entry["type"],
                "database_name": db_entry["database_name"],
                "host": db_entry["host"],
                "port": db_entry["port"],
                "ssl_required": db_entry.get("ssl_required", False),
                "connection_status": db_entry.get("connection_status", "unknown")
            }
            safe_databases.append(safe_db)
            
        return jsonify({"databases": safe_databases})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/logout_cleanup', methods=['POST'])
def logout_cleanup():
    # No longer needed with stateless connections, just clear cookie
    response = jsonify({"status": "cleared"})
    response.set_cookie('jwt_token', '', expires=0)
    return response

@app.route('/add_db', methods=['POST'])
@require_auth
def add_db():
    data = request.get_json()
    print(data)
    # Required fields for PostgreSQL/Supabase
    # Or connection_string for MongoDB
    
    new_db_id = ObjectId()
    db_type = data.get("type", "postgresql")
    
    db_entry = {
        "db_id": new_db_id,
        "display_name": data.get("display_name", data.get("database_name", "My DB")),
        "type": db_type,
        "database_name": data.get("database_name", ""),
        "host": data.get("host", ""),
        "port": int(data.get("port") or 5432),
        "username_encrypted": encryption_service.encrypt(data.get("user_name", "")) if data.get("user_name") else "",
        "password_encrypted": encryption_service.encrypt(data.get("password", "")) if data.get("password") else "",
        "ssl_required": data.get("ssl_required", False),
        "connection_string_encrypted": encryption_service.encrypt(data.get("connection_string", "")) if data.get("connection_string") else "",
        "created_at": datetime.utcnow(),
        "connection_status": "unknown"
    }
    try:
        db = get_project_db()
        db.users.update_one(
            {"_id": ObjectId(g.user_id)},
            {"$push": {"databases": db_entry}}
        )
        return jsonify({"status": "success", "message": "Database added successfully."}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/chat', methods=['POST'])
@require_auth
def api_chat():
    data = request.get_json()
    db_id = data.get("db_id")
    target = data.get("target")  # table or collection name
    message = data.get("message")
    conversation_id = data.get("conversation_id")
    overwrite_last = data.get("overwrite_last", False)
    
    if not all([db_id, target, message]):
        return jsonify({"error": "Missing db_id, target, or message"}), 400
        
    if overwrite_last and conversation_id:
        conversation_manager.remove_last_turn(conversation_id)
        
    try:
        db = get_project_db()
        user = db.users.find_one({"_id": ObjectId(g.user_id)})
        groq_key_encrypted = user.get("groq_api_key_encrypted")
        if not groq_key_encrypted:
            return jsonify({"error": "Groq API key not set in user settings"}), 403
            
        groq_api_key = encryption_service.decrypt(groq_key_encrypted)
        # Enqueue the background task
        job_id = f"j_{g.user_id}_{db_id}_{target}_{str(uuid.uuid4())[:8]}"
        task_queue.enqueue(
            'services.agent_worker.execute_agent_task',
            kwargs={
                'job_id': job_id,
                'user_id': g.user_id,
                'db_id': db_id,
                'target': target,
                'message': message,
                'conversation_id': conversation_id,
                'groq_api_key': groq_api_key
            },
            job_timeout=600  # 10 minutes max for the whole ReAct loop
        )
        
        return jsonify({
            "status": "accepted",
            "job_id": job_id,
            "message": "Task queued successfully"
        }), 202

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/chat/status/<job_id>', methods=['GET'])
@require_auth
def api_chat_status(job_id):
    try:
        status_data = get_job_status(job_id)
        if not status_data:
            return jsonify({"status": "pending"})
            
        return jsonify(status_data), 200
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/chat/resume/<conversation_id>', methods=['POST'])
@require_auth
def api_chat_resume(conversation_id):
    try:
        db = get_project_db()
        user = db.users.find_one({"_id": ObjectId(g.user_id)})
        groq_key_encrypted = user.get("groq_api_key_encrypted")
        if not groq_key_encrypted:
            return jsonify({"error": "Groq API key not set in user settings"}), 403
            
        groq_api_key = encryption_service.decrypt(groq_key_encrypted)
        
        conversation = conversation_manager.get_conversation(g.user_id, conversation_id)
        if not conversation:
            return jsonify({"error": "Conversation not found"}), 404
            
        paused_state = conversation.get("paused_state")
        if not paused_state:
            return jsonify({"error": "No paused state found for this conversation"}), 400
            
        # Enqueue the background task with the resume state
        job_id = f"j_{g.user_id}_{conversation['db_id']}_{conversation['target']}_{str(uuid.uuid4())[:8]}"
        task_queue.enqueue(
            'services.agent_worker.execute_agent_task',
            kwargs={
                'job_id': job_id,
                'user_id': g.user_id,
                'db_id': conversation["db_id"],
                'target': conversation["target"],
                'message': "Resume Request", # The agent ignores this anyway when resume_state is provided
                'conversation_id': conversation_id,
                'groq_api_key': groq_api_key,
                'resume_state': paused_state
            },
            job_timeout=600
        )
        
        return jsonify({
            "status": "accepted",
            "job_id": job_id,
            "message": "Resume task queued successfully"
        }), 202

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/chat/cancel/<conversation_id>', methods=['POST'])
@require_auth
def api_chat_cancel(conversation_id):
    try:
        conversation = conversation_manager.get_conversation(g.user_id, conversation_id)
        if not conversation:
            return jsonify({"error": "Conversation not found"}), 404
            
        conversation_manager.clear_paused_state(conversation_id)
        return jsonify({"status": "success", "message": "Analysis cancelled successfully"}), 200
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/settings/groq-key', methods=['POST'])
@require_auth
def update_groq_key():
    data = request.get_json()
    key = data.get("groq_api_key")
    if not key:
        return jsonify({"error": "Missing groq_api_key"}), 400
        
    try:
        db = get_project_db()
        encrypted_key = encryption_service.encrypt(key)
        db.users.update_one(
            {"_id": ObjectId(g.user_id)},
            {"$set": {"groq_api_key_encrypted": encrypted_key}}
        )
        return jsonify({"status": "success"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/settings', methods=['GET'])
@require_auth
def get_settings():
    db = get_project_db()
    user = db.users.find_one({"_id": ObjectId(g.user_id)})
    has_key = bool(user.get("groq_api_key_encrypted"))
    settings = user.get("settings", {})
    return jsonify({"has_groq_key": has_key, "settings": settings})

@app.route('/api/conversations', methods=['GET'])
@require_auth
def list_conversations():
    db = get_project_db()
    query = {"user_id": g.user_id}
    
    db_id = request.args.get("db_id")
    target = request.args.get("target")
    if db_id:
        query["db_id"] = db_id
    if target:
        query["target"] = target

    cursor = db.conversations.find(
        query,
        {"messages": 0} # Exclude full message history
    ).sort("updated_at", -1)
    
    convs = []
    for c in cursor:
        c["_id"] = str(c["_id"])
        convs.append(c)
    return jsonify(convs)

@app.route('/api/conversations/<conversation_id>', methods=['GET'])
@require_auth
def get_conversation_api(conversation_id):
    conv = conversation_manager.get_conversation(g.user_id, conversation_id)
    if not conv:
        return jsonify({"error": "Conversation not found"}), 404
        
    conv["_id"] = str(conv["_id"])
    for msg in conv.get("messages", []):
        if "message_id" in msg:
            msg["message_id"] = str(msg["message_id"])
    return jsonify(conv)

@app.route('/api/conversations/<conversation_id>', methods=['DELETE'])
@require_auth
def delete_conversation_api(conversation_id):
    success = conversation_manager.delete_conversation(g.user_id, conversation_id)
    if success:
        return jsonify({"success": True}), 200
    return jsonify({"error": "Conversation not found or deletion failed"}), 404
@app.route('/api/storage', methods=['GET'])
@require_auth
def get_storage_stats():
    import os
    from services.redis_client import get_redis_connection
    from services.parquet_manager import CACHE_DIR
    
    db = get_project_db()
    
    # 1. MongoDB Schema Cache Stats
    schema_cursor = db.schema_cache.find({"user_id": g.user_id})
    schema_cache_stats = []
    for doc in schema_cursor:
        # Approximate size of BSON
        approx_size = len(str(doc))
        schema_cache_stats.append({
            "db_id": doc["db_id"],
            "target": "schema",
            "size_bytes": approx_size,
            "expires_at": doc.get("expires_at", None)
        })
        
    # 2. DuckDB Parquet Files
    parquet_stats = []
    if os.path.exists(CACHE_DIR):
        for filename in os.listdir(CACHE_DIR):
            if filename.startswith(f"c_{g.user_id}_"):
                filepath = os.path.join(CACHE_DIR, filename)
                try:
                    size = os.path.getsize(filepath)
                    parts = filename.split("_")
                    if len(parts) >= 5:
                        db_id = parts[2]
                        target = parts[3]
                        parquet_stats.append({
                            "db_id": db_id,
                            "target": target,
                            "filename": filename,
                            "size_bytes": size
                        })
                except Exception:
                    pass
                    
    # 3. Redis Queue Stats
    redis_conn = get_redis_connection()
    redis_stats = []
    
    def scan_redis(prefix, type_name):
        for key in redis_conn.scan_iter(f"{prefix}:j_{g.user_id}_*"):
            key_str = key.decode("utf-8")
            parts = key_str.split("_")
            if len(parts) >= 5:
                db_id = parts[2]
                target = parts[3]
                try:
                    size = redis_conn.memory_usage(key) or 0
                    redis_stats.append({
                        "db_id": db_id,
                        "target": target,
                        "type": type_name,
                        "key": key_str,
                        "size_bytes": size
                    })
                except Exception:
                    pass
                    
    scan_redis("scratchpad", "scratchpad")
    scan_redis("job_status", "job_status")
    
    # Group by database and target
    grouped = {}
    
    def add_to_group(item, category):
        k = f"{item['db_id']}_{item['target']}"
        if k not in grouped:
            grouped[k] = {
                "db_id": item['db_id'],
                "target": item['target'],
                "schema_cache": {"count": 0, "size_bytes": 0},
                "parquet_files": {"count": 0, "size_bytes": 0},
                "redis_keys": {"count": 0, "size_bytes": 0}
            }
        grouped[k][category]["count"] += 1
        grouped[k][category]["size_bytes"] += item["size_bytes"]

    for item in schema_cache_stats: add_to_group(item, "schema_cache")
    for item in parquet_stats: add_to_group(item, "parquet_files")
    for item in redis_stats: add_to_group(item, "redis_keys")

    return jsonify(list(grouped.values()))
if __name__ == "__main__":
    # Disable Werkzeug reloader on Windows to prevent PyMongo socket clashes (WinError 10038)
    app.run(debug=True, use_reloader=False)