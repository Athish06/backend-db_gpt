from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Dict, Any, Tuple, Optional
from enum import Enum

class DBType(Enum):
    POSTGRESQL = "postgresql"
    SUPABASE = "supabase"
    MONGODB = "mongodb"

@dataclass
class DBConfig:
    type: DBType
    host: str
    port: int
    database_name: str
    username: str          # plaintext (decrypted before passing here)
    password: str          # plaintext (decrypted before passing here)
    ssl_required: bool = False
    connection_string: Optional[str] = None  # MongoDB Atlas URI, plaintext

class BaseDBConnector(ABC):
    def __init__(self, config: DBConfig):
        self.config = config

    @abstractmethod
    def test_connection(self) -> Tuple[bool, str]:
        """Returns (success, error_message)."""

    @abstractmethod
    def get_tables_or_collections(self) -> List[str]:
        """Return all table or collection names in the target database."""

    @abstractmethod
    def get_schema(self, target: str) -> Dict:
        """Return full schema/structure for a specific table or collection."""

    def get_all_schemas(self) -> Dict[str, Dict]:
        """Return schemas for all tables/collections."""
        pass

    def get_preview_data(self, target: str, limit: int = 10) -> Dict:
        """Return {schema, rows} or {schema, documents} for table viewer."""
        pass

    @abstractmethod
    def execute_sql(self, sql: str) -> Tuple[List[Dict], int]:
        """Execute raw SQL. Returns (rows, total_count)."""

    @abstractmethod
    def execute_mongodb_find(self, collection: str, filter_: Dict,
                              projection: Dict, sort: Dict, limit: int) -> Tuple[List[Dict], int]:
        """Execute a MongoDB find operation."""

    @abstractmethod
    def execute_mongodb_aggregate(self, collection: str,
                                   pipeline: List[Dict]) -> Tuple[List[Dict], int]:
        """Execute a MongoDB aggregation pipeline."""

    @abstractmethod
    def insert_row(self, target: str, data: Dict) -> Tuple[bool, str]:
        """Insert one row/document. Returns (success, error)."""

    @abstractmethod
    def close(self):
        """Release connection resources."""
