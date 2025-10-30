from sqlmodel import Session, select,SQLModel,Field
from sqlalchemy.types import JSON
from sqlalchemy.dialects.postgresql import JSONB

from pydantic import BaseModel
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from connect_db import SinkDB, QueryDB



class Data_To_Process(SQLModel,table=True):
    # __table_args__={"schema":"etl_metadata","table_name":"data_to_process"}
    __table_args__={"schema":"etl_metadata"}

    id:int = Field(primary_key=True)
    data_source_name:str # NY_BIKE_DATA,etc
    process_period:str # ANNUAL, MONTHLY
    bucket_path:str
    files:str
    year:int
    month:int
    period_tag:str
    status:str   #value TO_STAGE_DATABASE,FAILURE_TO_STAGE,TO_PROD_DATABASE,FAILURE_PROD,SUCCES_PRODCUTION
    created_at:datetime
    updated_at:datetime
    branch_bronze:str
    branch_silver:str
    branch_gold:str


# Define the metadata model
class Audit_Batch_Etl(SQLModel, table=True):
    # __table_args__={"schema":"etl_metadata","table_name":"audit_batch_etl"}
    __table_args__={"schema":"etl_metadata"}

    id:int = Field(default=None, primary_key=True)
    process_name: str
    start_time: datetime
    end_time:  Optional[datetime] = None
    duration: Optional[timedelta] = None
    rows_processed: Optional[int] = None
    status: str   #value IN_PROGRESS,FAILURE,SUCCESS
    process_period:str 
    year: int
    month:int
    data_to_process_id_fk:int
    error_message: Optional[str] = None
    details_result: Optional[ Dict[str, Any]] = Field(default_factory=dict, sa_type=JSONB)

def get_by_id_audit_batch_etl(id:int) -> Audit_Batch_Etl:
    query_db = QueryDB()
    statement = select(Audit_Batch_Etl).where(Audit_Batch_Etl.data_to_process_id_fk == id)
    result = query_db.read_from_db(statement)
    return result[0]

# Function to log metadata to PostgreSQL
def log_to_audit_metadata(metadata: Audit_Batch_Etl):
    sink_db = SinkDB()
    sink_db.insert_or_update_row(metadata)

        
def log_to_data_to_porcess(data: Data_To_Process):
    sink_db = SinkDB()
    sink_db.insert_or_update_row(data)


# def get_by_id_data_to_porcess(id: int):
#     with Session(engine) as session:
#         statement = select(List_Data_To_Process).where(List_Data_To_Process.list_data_id==id)
#         result = session.exec(statement)
#         data = result.one()
#         return data

def get_data_to_process(status:str):
    query_db = QueryDB()
    statement = select(Data_To_Process).where(Data_To_Process.status==status).order_by(Data_To_Process.id)
    result = query_db.read_from_db(statement)
    return result

def get_row_to_process(retry_status:str,new_data_status):
    """
    Get the right row to process.
    Read the database, verifying if there isn't a row to retry 
    and then continue with the new data to process 
    """
    result = get_data_to_process(retry_status)
    if len(result) > 0:
        return result[0]
    result = get_data_to_process(new_data_status)
    if len(result) > 0:
        return result[0]
    return None


# def read_etl_meatada(query_db:QueryDB):
#     with Session(engine) as session:
#         statement = select(ETL_Metadata).where(ETL_Metadata.status =="STAGE_STEP")
#         result = session.exec(statement).all()
#         return result


# def get_by_id_etl_meatada(id:int):
#     with Session(engine) as session:
#         statement = select(ETL_Metadata).where(ETL_Metadata.data_to_process_id_fk == id)\
#             .where(ETL_Metadata.process_name=="ETL_TO_STAGE_DATABASE")\
#                 .where(ETL_Metadata.status=="SUCCESS")
#         result = session.exec(statement).all()
#         return result[0]