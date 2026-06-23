from services.db_connector import DBConfig, DBType, BaseDBConnector
from connectors.postgresql_connector import PostgreSQLConnector
from connectors.mongodb_connector import MongoDBConnector

def get_connector(config: DBConfig) -> BaseDBConnector:
    if config.type in (DBType.POSTGRESQL, DBType.SUPABASE):
        return PostgreSQLConnector(config)
    elif config.type == DBType.MONGODB:
        return MongoDBConnector(config)
    else:
        raise ValueError(f"Unsupported DB type: {config.type}")
