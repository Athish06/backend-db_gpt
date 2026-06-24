import json
import re
import time
import asyncio
from typing import Dict, List, Optional
from groq import AsyncGroq
import httpx

from services.db_connector import BaseDBConnector
from services.result_processor import result_processor
from services.conversation import conversation_manager
from services.analytics_engine import execute_analytics_query
from services.redis_client import set_scratchpad

def format_schema_for_prompt(schema_cache: Dict, target_table: str, db_type: str) -> str:
    if db_type in ("postgresql", "supabase"):
        schemas = schema_cache.get("sql_schemas", {})
        target_tables = list(schemas.keys()) if target_table == "__all__" else [target_table]
        
        ddls = []
        for t in target_tables:
            if t not in schemas:
                ddls.append(f"-- Table '{t}' schema not available")
                continue
            cols = schemas[t]["columns"]
            ddl = f"CREATE TABLE {t} (\n"
            col_parts = []
            for col in cols:
                pk = " PRIMARY KEY" if col.get("is_primary_key") else ""
                nullable = "" if col.get("nullable") == "YES" else " NOT NULL"
                col_parts.append(f"    {col['name']} {col['data_type']}{pk}{nullable}")
            ddl += ",\n".join(col_parts) + "\n);"
            ddls.append(ddl)
        return "\n\n".join(ddls)
        
    elif db_type == "mongodb":
        mongo_schemas = schema_cache.get("mongo_schemas", {})
        target_cols = list(mongo_schemas.keys()) if target_table == "__all__" else [target_table]
        
        blocks = []
        for c in target_cols:
            if c not in mongo_schemas:
                blocks.append(f"-- Collection '{c}' schema not available")
                continue
            fields = mongo_schemas[c]["fields"]
            lines = [f"Collection: {c}", "Fields (inferred from document samples):"]
            for fname, fmeta in fields.items():
                ptype = fmeta.get("primary_type", "mixed")
                samples = fmeta.get("sample_values", [])
                sample_str = f"  (e.g. {samples[0]})" if samples else ""
                array_flag = " [array]" if fmeta.get("is_array") else ""
                lines.append(f"  {fname}: {ptype}{array_flag}{sample_str}")
            blocks.append("\n".join(lines))
        return "\n\n".join(blocks)
        
    return "Schema unavailable"


SYSTEM_PROMPT_TEMPLATE = """You are DB-GPT, an ultra-advanced database agent.

DATABASE:
Type: {db_type}
Name: {db_name}
Target: {target}

SCHEMA:
{schema_ddl}

You run in a loop of THOUGHT, ACTION, and ACTION_INPUT to solve complex analytical questions.

INITIAL FILTERING RULE:
Before executing any database queries, you MUST evaluate if the user's question is relevant to the provided SCHEMA.
If the question is completely irrelevant to the database (e.g., asking for a recipe, general knowledge, or data that clearly does not exist in the schema), you MUST immediately use the FINAL_ANSWER action to politely reject the request, explaining that it falls outside the scope of the connected database.

ANALYTICAL & SQL DIRECTIVES:
When generating SQL queries (either for the primary database or DuckDB), you must strictly adhere to the following data engineering rules to ensure mathematical accuracy:

1. Prevent Cartesian Aggregation Skew:
Never calculate an AVG(), SUM(), or COUNT(DISTINCT) on a parent table AFTER joining it to a 1-to-many child table. This creates duplicate rows and skews the math.
Incorrect: SELECT AVG(users.age) FROM users JOIN actions...
Correct: Aggregate the child table in a CTE first, OR use a subquery to isolate unique parent IDs before averaging.

2. Independent Event Calculations (No Strict Funnels):
Unless the user explicitly asks for a chronological funnel, treat different action types as mathematically independent. Do not use JOIN conditions that require a user to have performed Action A in order to count their Action B. Calculate independent events using Conditional Aggregation (SUM(CASE WHEN...)) on the base table.

3. Division Safety & Type Casting:
Whenever calculating a ratio, percentage, or division:
ALWAYS cast the numerator to a float/numeric type to prevent integer division zeroing (e.g., COUNT(x)::FLOAT / COUNT(y)).
ALWAYS wrap the denominator in NULLIF(denominator, 0) to prevent fatal "Division by Zero" errors.

4. Events vs. Entities:
Pay close attention to semantic phrasing.
If asked for "Total Lost Profit," calculate the profit lost for every specific event occurrence (frequency), not just the sum of unique products.
If asked for "Unique Users," explicitly use DISTINCT user_id.

ALLOWED ACTIONS:
1. EXECUTE_PRIMARY_QUERY
- Use this to fetch data from the primary database ({db_type}).
- For SQL (Postgres/Supabase), provide valid SQL.
- For MongoDB, provide a JSON dict with find/aggregate operations.
- ACTION_INPUT format for SQL: {{"sql": "SELECT ..."}}
- ACTION_INPUT format for MongoDB: {{"mongodb_find": {{"collection": "...", "filter": {{}}}}, "mongodb_pipeline": []}}

2. ANALYZE_CACHE
- Use this when an observation tells you data is saved to cache.
- The cache uses DuckDB! You MUST write DuckDB SQL dialect (not Postgres, not MongoDB).
- ALWAYS use the cache_id directly as the table name in your DuckDB query.
- If the observation says CACHE_EXPIRED, the data was lost due to ephemeral scaling. You MUST run EXECUTE_PRIMARY_QUERY again to re-fetch the data.
- ACTION_INPUT format: {{"cache_id": "cache_xyz", "sql": "SELECT ... FROM cache_xyz"}}

3. FINAL_ANSWER
- Use this when you have gathered all necessary information.
- ACTION_INPUT format: {{"reply": "The final natural language answer..."}}

RESPONSE FORMAT (Return ONLY valid JSON):
{{
  "thought": "Your reasoning about what to do next",
  "action": "EXECUTE_PRIMARY_QUERY | ANALYZE_CACHE | FINAL_ANSWER",
  "action_input": {{}}
}}

EXAMPLES OF A PERFECT LOOP:
Example 1:
User: "What is the average age?"
Assistant: {{"thought": "I need to fetch user data.", "action": "EXECUTE_PRIMARY_QUERY", "action_input": {{"sql": "SELECT age FROM users"}}}}
Observation: {{"status": "success", "cache_id": "cache_123", "message": "Result too large. Saved to cache. Use ANALYZE_CACHE action with cache_id 'cache_123' and DuckDB SQL."}}
Assistant: {{"thought": "Data cached. I will average it with DuckDB.", "action": "ANALYZE_CACHE", "action_input": {{"cache_id": "cache_123", "sql": "SELECT AVG(age) FROM cache_123"}}}}
Observation: [{{"AVG(age)": 34.5}}]
Assistant: {{"thought": "Got the answer.", "action": "FINAL_ANSWER", "action_input": {{"reply": "The average age is 34.5."}}}}
"""

def get_groq_client(api_key: str) -> AsyncGroq:
    return AsyncGroq(api_key=api_key, http_client=httpx.AsyncClient())

def _extract_json(text: str) -> Dict:
    text = text.strip()
    text = re.sub(r'^```(?:json)?\s*', '', text)
    text = re.sub(r'\s*```$', '', text)
    start = text.find('{')
    end = text.rfind('}')
    if start == -1 or end == -1:
        raise ValueError(f"No JSON found: {text[:100]}")
    return json.loads(text[start:end+1])

async def compress_observation(groq: AsyncGroq, raw_obs: str) -> str:
    """Uses a cheap LLM call to summarize massive errors or data dumps to save context window."""
    if len(raw_obs) < 2000:
        return raw_obs
    try:
        response = await groq.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[
                {"role": "system", "content": "Summarize this massive database observation/error log into a concise 3-sentence summary for another LLM. Keep key statistics and the exact error root cause."},
                {"role": "user", "content": raw_obs[:15000]} # Truncate so compressor doesn't OOM
            ],
            temperature=0.0,
            max_tokens=300
        )
        return "COMPRESSED OBSERVATION: " + response.choices[0].message.content
    except Exception:
        return raw_obs[:2000] + "...[truncated due to length]"

async def run_chat_turn(
    user_message: str,
    db_config: Dict,
    target: str,
    schema_cache: Dict,
    conversation: Dict,
    groq_api_key: str,
    connector: BaseDBConnector,
    job_id: str = None,
    resume_state: Dict = None
) -> Dict:
    
    groq = get_groq_client(groq_api_key)
    db_type = db_config.get("type", "")
    db_name = db_config.get("database_name", "")

    schema_ddl = format_schema_for_prompt(schema_cache, target, db_type)
    system_prompt = SYSTEM_PROMPT_TEMPLATE.format(
        db_type=db_type,
        db_name=db_name,
        target=target,
        schema_ddl=schema_ddl
    )

    if resume_state:
        scratchpad = resume_state.get("scratchpad", [])
        # Wake-Up Prompt Injection
        scratchpad.append({
            "role": "user",
            "content": "OBSERVATION: The system was temporarily paused due to API rate limits, but has now been resumed with a new key. Do NOT repeat your previous action if it was already successful. Review your scratchpad and execute the NEXT logical step to reach the FINAL_ANSWER."
        })
        final_generated_query = resume_state.get("metrics", {}).get("generated_query")
        final_query_type = resume_state.get("metrics", {}).get("query_type", "none")
        final_result_count = resume_state.get("metrics", {}).get("result_row_count", 0)
        total_exec_time = resume_state.get("metrics", {}).get("execution_time_ms", 0)
    else:
        # Long-Term Memory
        history_messages = conversation_manager.get_context_for_llm(conversation)
        
        # Short-Term Scratchpad
        scratchpad = [
            {"role": "system", "content": system_prompt},
            *history_messages,
            {"role": "user", "content": user_message}
        ]
        
        # Track metrics for final payload
        final_generated_query = None
        final_query_type = "none"
        final_result_count = 0
        total_exec_time = 0

    TIME_BUDGET = 45.0  # seconds
    start_time = time.time()

    while True:
        elapsed = time.time() - start_time
        if elapsed > TIME_BUDGET:
            scratchpad.append({
                "role": "user", 
                "content": "OBSERVATION: SYSTEM TIMEOUT (45s budget reached). You MUST call FINAL_ANSWER immediately with whatever insights you have gathered."
            })

        # Save scratchpad state to Redis (for debug/recovery if needed)
        if job_id:
            try:
                set_scratchpad(job_id, json.dumps(scratchpad))
            except: pass

        # 1. Call LLM
        try:
            llm_response = await asyncio.wait_for(
                groq.chat.completions.create(
                    model="llama-3.3-70b-versatile",
                    messages=scratchpad,
                    temperature=0.0,
                    max_tokens=1000,
                    response_format={"type": "json_object"}
                ),
                timeout=15.0
            )
            raw_llm_text = llm_response.choices[0].message.content
            parsed = _extract_json(raw_llm_text)
            
            # Append AI's raw thought to scratchpad
            scratchpad.append({"role": "assistant", "content": raw_llm_text})
            
        except asyncio.TimeoutError:
            scratchpad.append({"role": "user", "content": "OBSERVATION: LLM Timeout. The inference took too long."})
            continue
        except Exception as e:
            error_str = str(e)
            if "429" in error_str or "rate limit" in error_str.lower() or "too_many_requests" in error_str.lower():
                return {
                    "status": "paused_rate_limit",
                    "scratchpad": scratchpad,
                    "metrics": {
                        "generated_query": final_generated_query,
                        "query_type": final_query_type,
                        "result_row_count": final_result_count,
                        "execution_time_ms": total_exec_time
                    }
                }
            scratchpad.append({"role": "user", "content": f"OBSERVATION: Parse/API Error: {error_str}. Fix the JSON."})
            continue

        action = parsed.get("action")
        action_input = parsed.get("action_input", {})
        
        # 2. Handle FINAL_ANSWER
        if action == "FINAL_ANSWER":
            reply = action_input.get("reply", "No reply provided.")
            return {
                "reply": reply,
                "generated_query": final_generated_query,
                "query_type": final_query_type,
                "result_row_count": final_result_count,
                "execution_time_ms": total_exec_time,
                "error": None
            }
            
        # 3. Handle EXECUTE_PRIMARY_QUERY
        elif action == "EXECUTE_PRIMARY_QUERY":
            query_type = "sql" if "sql" in action_input else "mongodb_find" if "mongodb_find" in action_input else "mongodb_aggregate"
            final_query_type = query_type
            
            raw_results = []
            total_count = 0
            exec_error = None
            
            q_start = time.time()
            try:
                if query_type == "sql":
                    q = action_input.get("sql")
                    final_generated_query = q
                    # Use run_in_executor to not block the asyncio event loop with sync driver calls
                    raw_results, total_count = await asyncio.to_thread(connector.execute_sql, q)
                elif query_type == "mongodb_find":
                    op = action_input.get("mongodb_find", {})
                    final_generated_query = json.dumps(op)
                    raw_results, total_count = await asyncio.to_thread(
                        connector.execute_mongodb_find,
                        op.get("collection", target),
                        op.get("filter", {}),
                        op.get("projection", {}),
                        op.get("sort", {}),
                        op.get("limit", 100)
                    )
                elif query_type == "mongodb_aggregate":
                    pipeline = action_input.get("mongodb_pipeline", [])
                    final_generated_query = json.dumps(pipeline)
                    collection = action_input.get("mongodb_find", {}).get("collection", target)
                    raw_results, total_count = await asyncio.to_thread(
                        connector.execute_mongodb_aggregate, collection, pipeline
                    )
            except Exception as e:
                exec_error = str(e)
                
            q_end = time.time()
            total_exec_time += int((q_end - q_start) * 1000)
            
            if exec_error:
                obs = f"Query Execution Error: {exec_error}"
            else:
                final_result_count = total_count
                processed = result_processor.process(raw_results, total_count, hint="full")
                obs = json.dumps(processed, default=str)
                
            # Compress if needed
            obs = await compress_observation(groq, obs)
            scratchpad.append({"role": "user", "content": f"OBSERVATION: {obs}"})

        # 4. Handle ANALYZE_CACHE
        elif action == "ANALYZE_CACHE":
            cache_id = action_input.get("cache_id")
            duckdb_sql = action_input.get("sql")
            
            q_start = time.time()
            try:
                # DuckDB execute
                res = await asyncio.to_thread(execute_analytics_query, cache_id, duckdb_sql)
                obs = json.dumps(res, default=str)
            except Exception as e:
                obs = f"DuckDB Execution Error: {str(e)}"
            
            q_end = time.time()
            total_exec_time += int((q_end - q_start) * 1000)
            
            obs = await compress_observation(groq, obs)
            scratchpad.append({"role": "user", "content": f"OBSERVATION: {obs}"})

        # 5. Invalid Action
        else:
            scratchpad.append({"role": "user", "content": f"OBSERVATION: Invalid action '{action}'. Allowed: EXECUTE_PRIMARY_QUERY, ANALYZE_CACHE, FINAL_ANSWER."})
