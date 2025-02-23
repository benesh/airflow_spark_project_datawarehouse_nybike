


CREATE TABLE data_to_process (
    id SERIAL PRIMARY KEY,
    data_source_name VARCHAR(255) NOT NULL,
    process_period VARCHAR(50),
    path_csv VARCHAR(250),
    year INTEGER,
    month INTEGER,
    status VARCHAR(50),   # TO_BRONZE_DW, TO_SYVERS_DW,TO_GOLD_DW
    reader VARCHAR(50)
);

INSERT INTO data_to_process (data_source_name,process_period,path_csv,year,month,status,reader)
VALUES ('NY_BIKE_DATA','ANNUAL','2013-citibike-tripdata/*/*.csv',2013,0,'TO_BRONZE_DW','ReaderCSVLocal'),
       ('NY_BIKE_DATA','ANNUAL','2014-citibike-tripdata/*/*.csv',2014,0,'TO_BRONZE_DW','ReaderCSVLocal'),
       ('NY_BIKE_DATA','ANNUAL','2015-citibike-tripdata/*/*.csv',2015,0,'TO_BRONZE_DW','ReaderCSVLocal'),
       ('NY_BIKE_DATA','ANNUAL','2016-citibike-tripdata/*/*.csv',2016,0,'TO_BRONZE_DW','ReaderCSVLocal'),
       ('NY_BIKE_DATA','ANNUAL','2017-citibike-tripdata/*/*.csv',2017,0,'TO_BRONZE_DW','ReaderCSVLocal'),
       ('NY_BIKE_DATA','ANNUAL','2018-citibike-tripdata/*/*.csv',2018,0,'TO_BRONZE_DW','ReaderCSVLocal'),
       ('NY_BIKE_DATA','ANNUAL','2019-citibike-tripdata/*/*.csv',2019,0,'TO_BRONZE_DW','ReaderCSVLocal'),
       ('NY_BIKE_DATA','ANNUAL','2020-citibike-tripdata/*/*.csv',2020,0,'TO_BRONZE_DW','ReaderCSVLocal'),
       ('NY_BIKE_DATA','ANNUAL','2021-citibike-tripdata/*/*.csv',2021,0,'TO_BRONZE_DW','ReaderCSVLocal'),
       ('NY_BIKE_DATA','ANNUAL','2022-citibike-tripdata/*/*.csv',2022,0,'TO_BRONZE_DW','ReaderCSVLocal'),
       ('NY_BIKE_DATA','ANNUAL','2023-citibike-tripdata/*/*.csv',2023,0,'TO_BRONZE_DW','ReaderCSVLocal'),
       ('NY_BIKE_DATA','MONTH','2024-citibike-tripdata/01/*.csv',2024,1,'TO_BRONZE_DW','ReaderCSVLocal'),
       ('NY_BIKE_DATA','MONTH','2024-citibike-tripdata/02/*.csv',2024,2,'TO_BRONZE_DW','ReaderCSVLocal'),
       ('NY_BIKE_DATA','MONTH','2024-citibike-tripdata/03/*.csv',2024,3,'TO_BRONZE_DW','ReaderCSVLocal'),
       ('NY_BIKE_DATA','MONTH','2024-citibike-tripdata/04/*.csv',2024,4,'TO_BRONZE_DW','ReaderCSVLocal'),
       ('NY_BIKE_DATA','MONTH','2024-citibike-tripdata/05/*.csv',2024,5,'TO_BRONZE_DW','ReaderCSVLocal'),
       ('NY_BIKE_DATA','MONTH','2024-citibike-tripdata/06/*.csv',2024,6,'TO_BRONZE_DW','ReaderCSVLocal'),
       ('NY_BIKE_DATA','MONTH','2024-citibike-tripdata/07/*.csv',2024,7,'TO_BRONZE_DW','ReaderCSVLocal'),
       ('NY_BIKE_DATA','MONTH','2024-citibike-tripdata/08/*.csv',2024,8,'TO_BRONZE_DW','ReaderCSVLocal'),
       ('NY_BIKE_DATA','MONTH','2024-citibike-tripdata/09/*.csv',2024,9,'TO_BRONZE_DW','ReaderCSVLocal'),
       ('NY_BIKE_DATA','MONTH','2024-citibike-tripdata/10/*.csv',2024,10,'TO_BRONZE_DW','ReaderCSVLocal'),
       ('NY_BIKE_DATA','MONTH','2024-citibike-tripdata/11/*.csv',2024,11,'TO_BRONZE_DW','ReaderCSVLocal'),
       ('NY_BIKE_DATA','MONTH','2024-citibike-tripdata/12/*.csv',2024,12,'TO_BRONZE_DW','ReaderCSVLocal'),
       ('NY_BIKE_DATA','MONTH','2025-citibike-tripdata/01/*.csv',2025,01,'TO_BRONZE_DW','ReaderCSVLocal')
;


CREATE TABLE etl_metadata (
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

