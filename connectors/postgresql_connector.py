import psycopg2
import psycopg2.extras
from typing import List, Dict, Tuple
from services.db_connector import BaseDBConnector, DBConfig, DBType

class PostgreSQLConnector(BaseDBConnector):
    def __init__(self, config: DBConfig):
        super().__init__(config)
        self._conn = None

    def _get_connection(self):
        if self._conn is None or self._conn.closed:
            if self.config.connection_string:
                # Use connection string if provided (e.g. for Supabase pooler)
                params = {
                    "dsn": self.config.connection_string,
                    "cursor_factory": psycopg2.extras.RealDictCursor
                }
                if self.config.ssl_required or self.config.type == DBType.SUPABASE:
                    params["sslmode"] = "require"
                self._conn = psycopg2.connect(**params)
            else:
                # Fallback to individual fields
                params = {
                    "host": self.config.host,
                    "port": self.config.port,
                    "database": self.config.database_name,
                    "user": self.config.username,
                    "password": self.config.password,
                    "connect_timeout": 10,
                    "cursor_factory": psycopg2.extras.RealDictCursor
                }
                # Supabase requires SSL; respect explicit ssl_required flag
                if self.config.ssl_required or self.config.type == DBType.SUPABASE:
                    params["sslmode"] = "require"
                self._conn = psycopg2.connect(**params)
        return self._conn

    def test_connection(self) -> Tuple[bool, str]:
        try:
            conn = self._get_connection()
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            return True, ""
        except Exception as e:
            return False, str(e)

    def get_tables_or_collections(self) -> List[str]:
        conn = self._get_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """)
            return [row['table_name'] for row in cur.fetchall()]

    def get_schema(self, table_name: str) -> Dict:
        conn = self._get_connection()
        with conn.cursor() as cur:
            # Columns with type and nullability
            cur.execute("""
                SELECT
                    c.column_name,
                    c.data_type,
                    c.is_nullable,
                    c.column_default,
                    CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END as is_primary_key,
                    ccu.table_name AS foreign_table,
                    ccu.column_name AS foreign_column
                FROM information_schema.columns c
                LEFT JOIN (
                    SELECT kcu.column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                        ON tc.constraint_name = kcu.constraint_name
                    WHERE tc.constraint_type = 'PRIMARY KEY'
                    AND tc.table_name = %s
                ) pk ON c.column_name = pk.column_name
                LEFT JOIN information_schema.referential_constraints rc
                    ON rc.constraint_name = (
                        SELECT constraint_name FROM information_schema.key_column_usage
                        WHERE table_name = %s AND column_name = c.column_name LIMIT 1
                    )
                LEFT JOIN information_schema.constraint_column_usage ccu
                    ON ccu.constraint_name = rc.unique_constraint_name
                WHERE c.table_name = %s AND c.table_schema = 'public'
                ORDER BY c.ordinal_position
            """, (table_name, table_name, table_name))
            columns = cur.fetchall()

            # Approximate row count (fast, avoids full scan)
            cur.execute("""
                SELECT reltuples::bigint AS estimate
                FROM pg_class WHERE relname = %s
            """, (table_name,))
            count_row = cur.fetchone()
            row_count = count_row['estimate'] if count_row else 0

        return {
            "table_name": table_name,
            "columns": [dict(col) for col in columns],
            "row_count_approx": row_count
        }

    def execute_sql(self, sql: str) -> Tuple[List[Dict], int]:
        conn = self._get_connection()
        with conn.cursor() as cur:
            cur.execute(sql)
            if cur.description:
                rows = [dict(row) for row in cur.fetchall()]
                return rows, len(rows)
            else:
                conn.commit()
                return [], cur.rowcount

    def execute_mongodb_find(self, collection: str, filter_: Dict,
                              projection: Dict, sort: Dict, limit: int) -> Tuple[List[Dict], int]:
        raise NotImplementedError("Not a MongoDB database")

    def execute_mongodb_aggregate(self, collection: str,
                                   pipeline: List[Dict]) -> Tuple[List[Dict], int]:
        raise NotImplementedError("Not a MongoDB database")

    def insert_row(self, target: str, data: Dict) -> Tuple[bool, str]:
        conn = self._get_connection()
        with conn.cursor() as cur:
            columns = data.keys()
            values = [data[column] for column in columns]
            insert_statement = f"INSERT INTO {target} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(values))})"
            try:
                cur.execute(insert_statement, tuple(values))
                conn.commit()
                return True, ""
            except Exception as e:
                conn.rollback()
                return False, str(e)

    def close(self):
        if self._conn and not self._conn.closed:
            self._conn.close()
            self._conn = None
