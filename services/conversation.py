from typing import Dict, List, Optional
from datetime import datetime
from bson import ObjectId
from db.mongo_client import get_project_db

class ConversationManager:
    MAX_CONTEXT_MESSAGES = 10
    COMPRESSION_TRIGGER = 20  # Compress when total messages exceed this

    def get_or_create(self, user_id: str, db_id: str,
                       db_name: str, db_type: str, target: str) -> Dict:
        """Get the current open conversation or create a new one."""
        db = get_project_db()
        conv = db.conversations.find_one(
            {
                "user_id": user_id,
                "db_id": db_id,
                "target": target
            },
            sort=[("updated_at", -1)]
        )
        if conv:
            return conv
        return self.create_new_conversation(user_id, db_id, db_name, db_type, target)

    def create_new_conversation(self, user_id: str, db_id: str,
                                db_name: str, db_type: str, target: str) -> Dict:
        """Create a completely new conversation."""
        db = get_project_db()
        new_conv = {
            "user_id": user_id,
            "db_id": db_id,
            "db_name": db_name,
            "db_type": db_type,
            "target": target,
            "messages": [],
            "context_summary": None,
            "message_count": 0,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }
        result = db.conversations.insert_one(new_conv)
        new_conv["_id"] = result.inserted_id
        return new_conv

    def get_conversation(self, user_id: str, conversation_id: str) -> Optional[Dict]:
        """Get a specific conversation."""
        db = get_project_db()
        return db.conversations.find_one({
            "_id": ObjectId(conversation_id),
            "user_id": user_id
        })

    def delete_conversation(self, user_id: str, conversation_id: str) -> bool:
        """Delete a specific conversation."""
        db = get_project_db()
        result = db.conversations.delete_one({
            "_id": ObjectId(conversation_id),
            "user_id": user_id
        })
        return result.deleted_count > 0

    def save_paused_state(self, conversation_id: str, state: Dict):
        """Save a frozen ReAct loop state when a rate limit is hit."""
        db = get_project_db()
        db.conversations.update_one(
            {"_id": ObjectId(conversation_id)},
            {
                "$set": {
                    "paused_state": state,
                    "updated_at": datetime.utcnow()
                }
            }
        )

    def clear_paused_state(self, conversation_id: str):
        """Clear the frozen ReAct loop state."""
        db = get_project_db()
        db.conversations.update_one(
            {"_id": ObjectId(conversation_id)},
            {
                "$unset": {"paused_state": ""},
                "$set": {"updated_at": datetime.utcnow()}
            }
        )

    def append_message(self, conversation_id: str, role: str,
                        content: str, metadata: Optional[Dict] = None):
        db = get_project_db()
        message = {
            "message_id": ObjectId(),
            "role": role,
            "content": content,
            "timestamp": datetime.utcnow(),
            "metadata": metadata or {}
        }
        db.conversations.update_one(
            {"_id": ObjectId(conversation_id)},
            {
                "$push": {"messages": message},
                "$inc": {"message_count": 1},
                "$set": {"updated_at": datetime.utcnow()}
            }
        )

    def remove_last_turn(self, conversation_id: str):
        """Remove the last user message and any subsequent messages (like assistant responses)."""
        db = get_project_db()
        conv = db.conversations.find_one({"_id": ObjectId(conversation_id)})
        if not conv or not conv.get("messages"):
            return

        messages = conv["messages"]
        new_messages = []
        user_popped = False

        for msg in reversed(messages):
            if not user_popped:
                if msg["role"] == "user":
                    user_popped = True
                continue
            new_messages.insert(0, msg)

        db.conversations.update_one(
            {"_id": ObjectId(conversation_id)},
            {
                "$set": {
                    "messages": new_messages,
                    "message_count": len(new_messages),
                    "updated_at": datetime.utcnow()
                }
            }
        )


    def get_context_for_llm(self, conversation: Dict) -> List[Dict]:
        """Return the last N messages formatted for Groq API."""
        messages = conversation.get("messages", [])
        summary = conversation.get("context_summary")

        # Select recent window
        recent = messages[-self.MAX_CONTEXT_MESSAGES:]

        formatted = []

        # If there is a compressed summary of older messages, prepend it
        if summary and len(messages) > self.MAX_CONTEXT_MESSAGES:
            formatted.append({
                "role": "user",
                "content": (
                    f"[Summary of earlier conversation: {summary}]"
                )
            })
            formatted.append({
                "role": "assistant",
                "content": "Understood. I have context from our earlier discussion."
            })

        for msg in recent:
            content = msg["content"]
            # For assistant messages, append query metadata as context
            if msg["role"] == "assistant":
                meta = msg.get("metadata", {})
                if meta.get("generated_query"):
                    content += f"\n[Used query: {str(meta['generated_query'])[:150]}]"
                if meta.get("result_row_count") is not None:
                    content += f"\n[Result: {meta['result_row_count']} records]"
            formatted.append({
                "role": msg["role"],
                "content": content
            })

        return formatted

    def maybe_compress(self, conversation_id: str,
                        groq_api_key: str) -> Optional[str]:
        """
        Compress old messages into a summary if conversation is long.
        Returns the new summary string, or None if no compression was done.
        """
        from groq import Groq
        import httpx

        db = get_project_db()
        conv = db.conversations.find_one({"_id": ObjectId(conversation_id)})
        if not conv:
            return None

        messages = conv.get("messages", [])
        if len(messages) < self.COMPRESSION_TRIGGER:
            return None

        # Only compress the older messages, keep last MAX_CONTEXT_MESSAGES intact
        old_messages = messages[:-self.COMPRESSION_TRIGGER]
        if not old_messages:
            return None

        text = "\n".join([
            f"{m['role'].upper()}: {m['content'][:200]}"
            for m in old_messages
        ])

        groq = Groq(api_key=groq_api_key, http_client=httpx.Client())
        response = groq.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{
                "role": "user",
                "content": (
                    f"Summarize this database chat in 2-3 sentences. "
                    f"Focus on: what was queried, key findings, established context.\n\n{text}"
                )
            }],
            max_tokens=150,
            temperature=0.3
        )
        summary = response.choices[0].message.content.strip()

        db.conversations.update_one(
            {"_id": ObjectId(conversation_id)},
            {"$set": {"context_summary": summary}}
        )
        return summary

conversation_manager = ConversationManager()
