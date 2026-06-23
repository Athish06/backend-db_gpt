import duckdb
import os
from services.parquet_manager import get_parquet_filepath

def execute_analytics_query(cache_id: str, sql_query: str) -> dict:
    """
    Executes a DuckDB SQL query against the cached parquet file.
    """
    filepath = get_parquet_filepath(cache_id)
    if not os.path.exists(filepath):
        return {
            "error": "CACHE_EXPIRED", 
            "message": f"The cached data for '{cache_id}' was cleared (ephemeral disk wipe). You MUST use EXECUTE_PRIMARY_QUERY to fetch the data from the database again."
        }
    
    try:
        con = duckdb.connect(database=':memory:')
        
        # Create an in-memory view mapped to the parquet file
        # This allows the LLM to write: SELECT * FROM cache_xyz
        con.execute(f"CREATE VIEW {cache_id} AS SELECT * FROM '{filepath}'")
        
        # Execute the query
        result_df = con.execute(sql_query).df()
        
        # Convert NaN/NaT to None for JSON serialization
        result_df = result_df.where(result_df.notnull(), None)
        records = result_df.to_dict(orient="records")
        
        return {"status": "success", "rows": len(records), "data": records}
        
    except Exception as e:
        return {"error": str(e)}
