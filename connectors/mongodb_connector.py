from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
from typing import List, Dict, Tuple, Optional
from services.db_connector import BaseDBConnector, DBConfig

class MongoDBConnector(BaseDBConnector):
    def __init__(self, config: DBConfig):
        super().__init__(config)
        self._client: Optional[MongoClient] = None

    def _get_client(self) -> MongoClient:
        if self._client is None:
            if self.config.connection_string:
                self._client = MongoClient(
                    self.config.connection_string,
                    serverSelectionTimeoutMS=5000
                )
            else:
                self._client = MongoClient(
                    host=self.config.host,
                    port=self.config.port,
                    username=self.config.username or None,
                    password=self.config.password or None,
                    serverSelectionTimeoutMS=5000
                )
        return self._client

    def _get_db(self, client: MongoClient):
        if self.config.database_name:
            return client[self.config.database_name]
        try:
            return client.get_default_database()
        except Exception:
            # Fallback if no database name is provided anywhere
            return client["test"]

    def test_connection(self) -> Tuple[bool, str]:
        try:
            client = self._get_client()
            client.server_info()
            return True, ""
        except ServerSelectionTimeoutError as e:
            return False, f"Cannot reach MongoDB server: {e}"
        except Exception as e:
            return False, str(e)

    def get_tables_or_collections(self) -> List[str]:
        client = self._get_client()
        db = self._get_db(client)
        return sorted(db.list_collection_names())

    def get_schema(self, collection_name: str) -> Dict:
        """Sample 100 documents to infer schema. MongoDB is schemaless."""
        client = self._get_client()
        db = self._get_db(client)
        collection = db[collection_name]

        sample = list(collection.aggregate([{"$sample": {"size": 100}}]))
        doc_count = collection.estimated_document_count()

        schema_map: Dict = {}
        for doc in sample:
            for key, value in doc.items():
                if key == '_id':
                    continue
                type_name = type(value).__name__
                if key not in schema_map:
                    schema_map[key] = {
                        "types": {},
                        "sample_values": [],
                        "is_array": False
                    }
                schema_map[key]["types"][type_name] = (
                    schema_map[key]["types"].get(type_name, 0) + 1
                )
                if isinstance(value, list):
                    schema_map[key]["is_array"] = True
                if len(schema_map[key]["sample_values"]) < 3:
                    schema_map[key]["sample_values"].append(
                        str(value)[:80]
                    )

        # Determine primary type for each field
        for field_meta in schema_map.values():
            if field_meta["types"]:
                field_meta["primary_type"] = max(
                    field_meta["types"],
                    key=field_meta["types"].get
                )

        return {
            "collection_name": collection_name,
            "fields": schema_map,
            "document_count": doc_count,
            "sampled": len(sample)
        }

    def execute_sql(self, sql: str) -> Tuple[List[Dict], int]:
        raise NotImplementedError("Not a SQL database")

    def execute_mongodb_find(self, collection: str, filter_: Dict,
                              projection: Dict, sort: Dict, limit: int) -> Tuple[List[Dict], int]:
        client = self._get_client()
        db = self._get_db(client)
        coll = db[collection]

        total = coll.count_documents(filter_)
        cursor = coll.find(filter_, projection or None)
        if sort:
            cursor = cursor.sort(list(sort.items()))
        cursor = cursor.limit(min(limit, 1000))

        rows = []
        for doc in cursor:
            doc['_id'] = str(doc['_id'])  # Serialize ObjectId
            rows.append(doc)

        return rows, total

    def execute_mongodb_aggregate(self, collection: str,
                                   pipeline: List[Dict]) -> Tuple[List[Dict], int]:
        client = self._get_client()
        db = self._get_db(client)
        coll = db[collection]

        results = list(coll.aggregate(pipeline))
        for doc in results:
            if '_id' in doc:
                doc['_id'] = str(doc['_id'])

        return results, len(results)

    def insert_row(self, target: str, data: Dict) -> Tuple[bool, str]:
        client = self._get_client()
        db = self._get_db(client)
        try:
            db[target].insert_one(data)
            return True, ""
        except Exception as e:
            return False, str(e)

    def close(self):
        if self._client:
            self._client.close()
            self._client = None
