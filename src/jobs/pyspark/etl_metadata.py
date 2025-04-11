from pyspark.sql import SparkSession
from sqlmodel import create_engine, Session, select,SQLModel,Field
from datetime import datetime, timedelta
from typing import Optional
import os

# Database connection details
PG_USER = "postgres"
PG_PASSWORD = "postgres"
PG_HOST = "postgres"  # Replace with your host
PG_PORT = "5432"       # Default PostgreSQL port
PG_DATABASE = "postgres"

PG_URL = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"

# Create the engine
engine = create_engine(PG_URL)

class Data_To_Process(SQLModel,table=True):
    __table_args__={"schema":"process_report"}

    id:int = Field(default=None,primary_key=True)
    data_source_name:str # NY_BIKE_DATA,etc
    process_period:str # ANNUAL, MONTHLY
    path_csv:str
    year:int
    month:int
    period_tag:str
    status:str   #value TO_STAGE_DATABASE,FAILURE_TO_STAGE,TO_PROD_DATABASE,FAILURE_PROD,SUCCES_PRODCUTION
    created_at:datetime
    updated_at:datetime


# Define the metadata model
class ETL_Metadata(SQLModel, table=True):
    __table_args__={"schema":"process_report"}

    id: int = Field(default=None, primary_key=True)
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


# Function to log metadata to PostgreSQL
def log_etl_metadata(metadata: ETL_Metadata):
    with Session(engine) as session:
        session.add(metadata)
        session.commit()
        session.refresh(metadata)

        
def update_data_to_porcess(data: Data_To_Process):
    with Session(engine) as session:
        session.add(data)
        session.commit()
        # session.refresh(metadata)

# def get_by_id_data_to_porcess(id: int):
#     with Session(engine) as session:
#         statement = select(List_Data_To_Process).where(List_Data_To_Process.list_data_id==id)
#         result = session.exec(statement)
#         data = result.one()
#         return data

def get_data_to_process(status:str):
    with Session(engine) as session:
        statement = select(Data_To_Process).where(Data_To_Process.status==status)
        result = session.exec(statement).all()
        return result


def read_etl_meatada():
    with Session(engine) as session:
        statement = select(ETL_Metadata).where(ETL_Metadata.status =="STAGE_STEP")
        result = session.exec(statement).all()
        return result
    
def get_by_id_etl_meatada(id:int):
    with Session(engine) as session:
        statement = select(ETL_Metadata).where(ETL_Metadata.data_to_process_id_fk == id)\
            .where(ETL_Metadata.process_name=="ETL_TO_STAGE_DATABASE")\
                .where(ETL_Metadata.status=="SUCCESS")
        result = session.exec(statement).all()
        return result[0]