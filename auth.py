import os
import jwt
import datetime
import bcrypt
from functools import wraps
from flask import request, jsonify, g
from dotenv import dotenv_values
from db.mongo_client import get_project_db
from bson import ObjectId
from services.connector_factory import get_connector
from services.db_connector import DBConfig, DBType
from services.encryption import encryption_service

# Load .env variables
env = dotenv_values(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

JWT_SECRET = env.get("JWT_SECRET", "your_jwt_secret")
JWT_ALGORITHM = env.get("JWT_ALGORITHM", "HS256")

def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.cookies.get('jwt_token')
        if not token:
            # Fallback to Authorization header if provided
            auth_header = request.headers.get('Authorization')
            if auth_header and auth_header.startswith('Bearer '):
                token = auth_header.split(' ')[1]
                
        if not token:
            return jsonify({"error": "Missing or invalid token"}), 401
        try:
            payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
            g.user_id = payload['user_id']
            g.email = payload['email']
        except jwt.ExpiredSignatureError:
            return jsonify({"error": "Token has expired"}), 401
        except jwt.InvalidTokenError:
            return jsonify({"error": "Invalid token"}), 401
            
        return f(*args, **kwargs)
    return decorated

def login_api():
    """
    JWT authentication endpoint.
    Expects JSON: { "email": "...", "password": "..." }
    Returns: { "token": "...", "user": { ... }, "databases": [ ... ] }
    """
    data = request.get_json()
    email = data.get("email")
    password = data.get("password")
    if not email or not password:
        return jsonify({"error": "Missing email or password"}), 400

    try:
        db = get_project_db()
        user = db.users.find_one({"email": email})
        
        if not user or not bcrypt.checkpw(password.encode('utf-8'), user['password_hash'].encode('utf-8')):
            return jsonify({"error": "Invalid credentials"}), 401

        # Fetch all database connections for this user
        databases = user.get("databases", [])
        
        connection_statuses = []
        for db_entry in databases:
            db_config = DBConfig(
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
            
            connector = None
            try:
                connector = get_connector(db_config)
                success, _ = connector.test_connection()
            except Exception:
                success = False
            finally:
                if connector:
                    connector.close()
            
            # Safe DB entry without passwords
            safe_db = {
                "db_id": str(db_entry["db_id"]),
                "display_name": db_entry.get("display_name", db_entry["database_name"]),
                "type": db_entry["type"],
                "database_name": db_entry["database_name"],
                "host": db_entry["host"],
                "port": db_entry["port"],
                "ssl_required": db_entry.get("ssl_required", False),
                "created_at": db_entry.get("created_at"),
                "connection_status": "connected" if success else "failed"
            }
            connection_statuses.append(safe_db)

        # Generate JWT
        payload = {
            "user_id": str(user["_id"]),
            "email": email,
            "exp": datetime.datetime.utcnow() + datetime.timedelta(hours=24)
        }
        token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

        response = jsonify({
            "user": {
                "id": str(user["_id"]),
                "email": email
            },
            "databases": connection_statuses
        })
        
        # Set HttpOnly cookie
        response.set_cookie(
            "jwt_token",
            token,
            httponly=True,
            samesite="Lax",
            secure=False,  # Set to True in production with HTTPS
            max_age=86400  # 24 hours
        )
        return response, 200

    except Exception as e:
        print("Login error:", e)
        return jsonify({"error": str(e)}), 500

def signup_api():
    """
    User registration endpoint.
    Expects JSON: { "email": "...", "password": "..." }
    Returns: { "status": "success" } or error.
    """
    data = request.get_json()
    email = data.get("email")
    password = data.get("password")
    if not email or not password:
        return jsonify({"error": "Missing email or password"}), 400

    try:
        db = get_project_db()
        # Check if user already exists
        if db.users.find_one({"email": email}):
            return jsonify({"error": "User already exists"}), 409

        # Insert new user
        hashed_pw = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())
        
        new_user = {
            "email": email,
            "password_hash": hashed_pw.decode('utf-8'),
            "created_at": datetime.datetime.utcnow(),
            "updated_at": datetime.datetime.utcnow(),
            "databases": [],
            "groq_api_key_encrypted": None,
            "settings": {
                "default_result_limit": 100,
                "max_context_messages": 10,
                "preferred_model": "llama-3.3-70b-versatile"
            }
        }
        
        db.users.insert_one(new_user)
        return jsonify({"status": "success"}), 201
    except Exception as e:
        print("Signup error:", e)
        return jsonify({"error": str(e)}), 500