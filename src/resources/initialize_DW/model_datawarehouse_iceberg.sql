/*
	Initialisation of the database
	differente schema  for the layer 
*/

CREATE SCHEMA bronze;
CREATE SCHEMA sylver;
CREATE SCHEMA gold;

CREATE SCHEMA process_report;

CREATE TABLE process_report.data_to_process (
    id SERIAL PRIMARY KEY,
    data_source_name VARCHAR(255) NOT NULL,
    process_period VARCHAR(50),
    path_csv VARCHAR(250),
    year INTEGER,
    month INTEGER,
    period_tag VARCHAR(50),
    status VARCHAR(50),
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now()
);

CREATE OR REPLACE FUNCTION update_datetime_report()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = current_timestamp();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_article_modtime
BEFORE UPDATE ON process_report.data_to_process
FOR EACH ROW
EXECUTE FUNCTION update_modified_column();


INSERT INTO process_report.data_to_process (data_source_name,process_period,path_csv,year,month,period_tag,status)
VALUES ('NY_BIKE_DATA','ANNUAL','2013-citibike-tripdata/*/*.csv',2013,0,'2013','TO_BRONZE_LAYER' ),
       ('NY_BIKE_DATA','ANNUAL','2014-citibike-tripdata/*/*.csv',2014,0,'2014','TO_BRONZE_LAYER' ),
       ('NY_BIKE_DATA','ANNUAL','2015-citibike-tripdata/*/*.csv',2015,0,'2015','TO_BRONZE_LAYER' ),
       ('NY_BIKE_DATA','ANNUAL','2016-citibike-tripdata/*/*.csv',2016,0,'2016','TO_BRONZE_LAYER' ),
       ('NY_BIKE_DATA','ANNUAL','2017-citibike-tripdata/*/*.csv',2017,0,'2017','TO_BRONZE_LAYER' ),
       ('NY_BIKE_DATA','ANNUAL','2018-citibike-tripdata/*/*.csv',2018,0,'2018','TO_BRONZE_LAYER' ),
       ('NY_BIKE_DATA','ANNUAL','2019-citibike-tripdata/*/*.csv',2019,0,'2019','TO_BRONZE_LAYER' ),
       ('NY_BIKE_DATA','ANNUAL','2020-citibike-tripdata/*/*.csv',2020,0,'2020','TO_BRONZE_LAYER' ),
       ('NY_BIKE_DATA','ANNUAL','2021-citibike-tripdata/*/*.csv',2021,0,'2021','TO_BRONZE_LAYER' ),
       ('NY_BIKE_DATA','ANNUAL','2022-citibike-tripdata/*/*.csv',2022,0,'2022','TO_BRONZE_LAYER' ),
       ('NY_BIKE_DATA','ANNUAL','2023-citibike-tripdata/*/*.csv',2023,0,'2023','TO_BRONZE_LAYER' ),
       ('NY_BIKE_DATA','MONTH','2024-citibike-tripdata/01/*.csv',2024,1,'200401','TO_BRONZE_LAYER' ),
       ('NY_BIKE_DATA','MONTH','2024-citibike-tripdata/02/*.csv',2024,2,'200402','TO_BRONZE_LAYER' ),
       ('NY_BIKE_DATA','MONTH','2024-citibike-tripdata/03/*.csv',2024,3,'200403','TO_BRONZE_LAYER' ),
       ('NY_BIKE_DATA','MONTH','2024-citibike-tripdata/04/*.csv',2024,4,'200404','TO_BRONZE_LAYER' ),
       ('NY_BIKE_DATA','MONTH','2024-citibike-tripdata/05/*.csv',2024,5,'200405','TO_BRONZE_LAYER' ),
       ('NY_BIKE_DATA','MONTH','2024-citibike-tripdata/06/*.csv',2024,6,'200406','TO_BRONZE_LAYER' ),
       ('NY_BIKE_DATA','MONTH','2024-citibike-tripdata/07/*.csv',2024,7,'200407','TO_BRONZE_LAYER' ),
       ('NY_BIKE_DATA','MONTH','2024-citibike-tripdata/08/*.csv',2024,8,'200408','TO_BRONZE_LAYER' ),
       ('NY_BIKE_DATA','MONTH','2024-citibike-tripdata/09/*.csv',2024,9,'200409','TO_BRONZE_LAYER' ),
       ('NY_BIKE_DATA','MONTH','2024-citibike-tripdata/10/*.csv',2024,10,'200410','TO_BRONZE_LAYER' ),
       ('NY_BIKE_DATA','MONTH','2024-citibike-tripdata/11/*.csv',2024,11,'200411','TO_BRONZE_LAYER' ),
       ('NY_BIKE_DATA','MONTH','2024-citibike-tripdata/12/*.csv',2024,12,'200412','TO_BRONZE_LAYER' ),
       ('NY_BIKE_DATA','MONTH','2025-citibike-tripdata/01/*.csv',2025,01,'200501','TO_BRONZE_LAYER' )
;
 
CREATE TABLE process_report.etl_metadata (
    id SERIAL PRIMARY KEY,
    process_name VARCHAR(255) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    duration INTERVAL,
    rows_processed BIGINT,
    status VARCHAR(50),
    process_period VARCHAR(50) NOT NULL,
    year INTEGER ,
    month INTEGER ,
    data_to_process_id_fk INTEGER,
    error_message TEXT
    -- CONSTRAINT data_to_process_fk
    -- FOREIGN KEY (list_data_id)
    -- REFERRENCES list_data_to_process(list_data_id)
);


CREATE TABLE bronze.trip_data_nybike(
    dw_period_tag VARCHAR(50),
    ride_id VARCHAR,
	start_station_id VARCHAR,
	start_station_name VARCHAR(4000),
	start_station_latitude DOUBLE PRECISION,
	start_station_longitude DOUBLE PRECISION,
	end_station_id VARCHAR,
	end_station_name VARCHAR(1000),
	end_station_latitude DOUBLE PRECISION,
	end_station_longitude DOUBLE PRECISION,
	user_type VARCHAR(255),
    gender VARCHAR(255),
	customer_year_birth VARCHAR(255),
    bike_id INTEGER,
	rideable_type VARCHAR(255),
	start_at TIMESTAMP,
	stop_at TIMESTAMP,
	trip_duration DOUBLE PRECISION,
);

--Creating an indew for the tag period 
CREATE INDEX trip_data_tag_period ON bronze.trip_data_nybike(dw_period_tag);

CREATE TABLE sylver.trip_data_nybike(
    trip_uuid UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dw_period_tag VARCHAR(50),
	start_station_id VARCHAR(250),
	start_station_name VARCHAR(4000),
	start_station_latitude DOUBLE PRECISION,
	start_station_longitude DOUBLE PRECISION,
	end_station_id VARCHAR(250),
	end_station_name VARCHAR(4000),
	end_station_latitude DOUBLE PRECISION,
	end_station_longitude DOUBLE PRECISION,
	bike_id INTEGER,
    customer_type VARCHAR(255),
    enr_gender VARCHAR(255),
	customer_year_birth  VARCHAR(255),
	rideable_type VARCHAR(2500),
	start_at TIMESTAMP,
	stop_at TIMESTAMP,
	trip_duration DOUBLE PRECISION,
    quarter INTEGER,
    quarter_name VARCHAR(255),
    month INTEGER,
    month_name VARCHAR(255),
    day INTEGER,
    weekday INTEGER,
    weekday_name VARCHAR(255)
);


 ---- sql for iceberg 

CREATE DATABASE dw_nybike;

USE dw_nybike;
CREATE SCHEMA IF NOT EXISTS  bronze;
CREATE SCHEMA IF NOT EXISTS sylver ;
CREATE SCHEMA IF NOT EXISTS gold ;

CREATE TABLE dw_nybike.bronze.trip_data_nybike(
    dw_period_tag string,
    ride_id string,
	start_station_id string,
	start_station_name string,
	start_station_latitude DOUBLE ,
	start_station_longitude DOUBLE ,
	end_station_id string,
	end_station_name string,
	end_station_latitude double,
	end_station_longitude double,
	user_type string,
    gender string,
	customer_year_birth string,
    bike_id bigint,
	rideable_type string,
	start_at timestamp,
	stop_at timestamp, 
	trip_duration DOUBLE
    )
PARTITIONED BY (dw_period_tag)
;

-- sylver database 

CREATE TABLE dw_nybike.sylver.trip_data_nybike(
    trip_uuid string,
    dw_period_tag string,
	start_station_id string,
	start_station_name string,
	start_station_latitude double,
	start_station_longitude double,
	end_station_id string,
	end_station_name string,
	end_station_latitude double,
	end_station_longitude double,
	bike_id string,
    customer_type string,
    enr_gender string,
	customer_year_birth  string,
	rideable_type string,
	start_at timestamp,
	stop_at timestamp,
	trip_duration double,
    quarter int,
    quarter_name string,
    month int,
    month_name string,
    day int,
    weekday int,
    weekday_name string
    )
PARTITIONED BY (dw_period_tag)
    ;