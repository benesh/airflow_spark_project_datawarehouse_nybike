from pyspark.sql import SparkSession
from sqlmodel import create_engine, Session, select,SQLModel,Field
from datetime import datetime, timedelta
import os

# Database connection details
PG_USER = "root"
PG_PASSWORD = "your-password"
PG_HOST = "db"  # Replace with your host
PG_PORT = "5432"       # Default PostgreSQL port
PG_DATABASE = "postgres"

PG_URL = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"

# Create the engine
engine = create_engine(PG_URL)

class Data_To_Process(SQLModel,table=True):
    id:int = Field(default=None,primary_key=True)
    data_source_name:str # NY_BIKE_DATA,etc
    process_period:str # ANNUAL, MONTHLY
    path_csv:str
    year:int
    month:int
    status:str   #value TO_STAGE_DATABASE,FAILURE_TO_STAGE,TO_PROD_DATABASE,FAILURE_PROD,SUCCES_PRODCUTION
    reader:str   # ReaderCSVLocal



# Define the metadata model
class ETL_Metadata(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    process_name: str
    start_time: datetime
    end_time: datetime | None = None
    duration: timedelta | None = None
    rows_processed: int | None = None
    status: str   #value IN_PROGRESS,FAILURE,SUCCESS
    process_period:str
    year: int
    month:int
    data_to_process_id_fk:int
    # data_nybike_id_fk:int = Field(default=None,foreign_key="data_to_process.id")
    error_message: str | None = None


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




# # Main ETL process
# if __name__ == "__main__":
#     try:
#         # Step 1: Capture start time and initialize metadata
#         start_time = datetime.utcnow()
#         metadata = ETL_Metadata(
#             process_name="Customer Data ETL",
#             start_time=start_time,
#             status="IN_PROGRESS"
#         )
#         log_etl_metadata(metadata)  # Log initial metadata

#         # Step 2: Perform ETL operations
#         print("Starting ETL process...")
#         # Example: Read data from a source (e.g., CSV file)
#         input_data = spark.read.csv("path/to/input.csv", header=True, inferSchema=True)

#         # Example: Transform data
#         transformed_data = input_data.filter(input_data["age"] > 18)

#         # Example: Write data to a destination (e.g., another CSV file)
#         transformed_data.write.mode("overwrite").csv("path/to/output.csv")

#         # Step 3: Capture end time and update metadata
#         end_time = datetime.utcnow()
#         rows_processed = transformed_data.count()
#         duration = end_time - start_time

#         metadata.end_time = end_time
#         metadata.duration = duration
#         metadata.rows_processed = rows_processed
#         metadata.status = "SUCCESS"
#         log_etl_metadata(metadata)  # Update metadata

#         print("ETL process completed successfully.")

#     except Exception as e:
#         # Step 4: Handle errors and update metadata
#         end_time = datetime.utcnow()
#         duration = end_time - start_time

#         metadata.end_time = end_time
#         metadata.duration = duration
#         metadata.status = "FAILURE"
#         metadata.error_message = str(e)
#         log_etl_metadata(metadata)  # Update metadata with error details

#         print(f"ETL process failed: {e}")