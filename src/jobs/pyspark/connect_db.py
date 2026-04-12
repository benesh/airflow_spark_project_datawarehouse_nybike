from pydantic import BaseModel
from sqlmodel import create_engine, Session, select,SQLModel,Field
import json
import os

def get_db_config():
    return {
        'user': os.environ.get("POSTGRES_USER"),
        'password': os.environ.get("POSTGRES_PASSWORD"),
        'host': os.environ.get("POSTGRES_HOST"),
        'port': os.environ.get("POSTGRES_PORT"),
        'database': os.environ.get("DATABASE_ETL_METADATA"),
        'db_prefix': os.environ.get("DB_PREFIX"),
    }

class ConfigDB(BaseModel):
    user: str
    password: str
    host: str
    port: str
    database: str
    db_prefix : str

    def create_engine(self):
        db_url = f"{self.db_prefix}://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        return create_engine(db_url, echo=False, pool_pre_ping=True)

class ConnectDB:
    def __init__(self):
        config_db_dict = get_db_config()
        config_db:ConfigDB = ConfigDB(**config_db_dict)
        self.engine = config_db.create_engine()

    def get_session(self):
        return Session(self.engine)
    
    def close_session(self,session:Session):
        session.close()

    def __exit__(self, exc_type, exc_value, traceback):
        self.engine.dispose()

class SinkDB:
    def __init__(self):
        self.connect_db :ConnectDB = ConnectDB()

    def insert_or_update_row(self,metadata:SQLModel):
        with Session(self.connect_db.engine) as session:
            session.add(metadata)
            session.commit()
            session.refresh(metadata)

class QueryDB:
    def __init__(self):
        self.connect_db :ConnectDB = ConnectDB()

    def read_from_db(self,statement):
        with Session(self.connect_db.engine) as session:
            result = session.exec(statement).all()
            return result
