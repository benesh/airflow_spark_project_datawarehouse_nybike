
CREATE TABLE IF NOT EXISTS status_etl (
    id SERIAL PRIMARY KEY,
    status_name VARCHAR(100) ,
);

CREATE TABLE data_to_process (
    id SERIAL PRIMARY KEY,
    data_source_name VARCHAR(255) NOT NULL,
    process_period VARCHAR(50),
    bucket_path VARCHAR(250),
    files VARCHAR(250),
    year INTEGER,
    month INTEGER,
    status VARCHAR(50),   # TO_BRONZE_DW, TO_SYVERS_DW,TO_GOLD_DW
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    branch_bronze VARCHAR(100),
    branch_silver VARCHAR(100),
    branch_gold VARCHAR(100)
);

INSERT INTO data_to_process (data_source_name,process_period,bucket_path,files,year,month,period_tag,status)
VALUES ('NY_BIKE_DATA','ANNUAL','s3//:bucket-raw-data/raw_data_nybike','s3a://bucket-raw-data/raw_data_nybike/2013-citibike-tripdata/*/*.csv',2013,0,'2013','TO_BRONZE_LAYER'),
       ('NY_BIKE_DATA','ANNUAL','s3//:bucket-raw-data/raw_data_nybike','s3a://bucket-raw-data/raw_data_nybike/2014-citibike-tripdata/*/*.csv',2014,0,'2014','TO_BRONZE_LAYER'),
       ('NY_BIKE_DATA','ANNUAL','s3//:bucket-raw-data/raw_data_nybike','s3a://bucket-raw-data/raw_data_nybike/2015-citibike-tripdata/*/*.csv',2015,0,'2015','TO_BRONZE_LAYER'),
       ('NY_BIKE_DATA','ANNUAL','s3//:bucket-raw-data/raw_data_nybike','s3a://bucket-raw-data/raw_data_nybike/2016-citibike-tripdata/*/*.csv',2016,0,'2016','TO_BRONZE_LAYER'),
       ('NY_BIKE_DATA','ANNUAL','s3//:bucket-raw-data/raw_data_nybike','s3a://bucket-raw-data/raw_data_nybike/2017-citibike-tripdata/*/*.csv',2017,0,'2017','TO_BRONZE_LAYER'),
       ('NY_BIKE_DATA','ANNUAL','s3//:bucket-raw-data/raw_data_nybike','s3a://bucket-raw-data/raw_data_nybike/2018-citibike-tripdata/*/*.csv',2018,0,'2018','TO_BRONZE_LAYER'),
       ('NY_BIKE_DATA','ANNUAL','s3//:bucket-raw-data/raw_data_nybike','s3a://bucket-raw-data/raw_data_nybike/2019-citibike-tripdata/*/*.csv',2019,0,'2019','TO_BRONZE_LAYER'),
       ('NY_BIKE_DATA','ANNUAL','s3//:bucket-raw-data/raw_data_nybike','s3a://bucket-raw-data/raw_data_nybike/2020-citibike-tripdata/*/*.csv',2020,0,'2020','TO_BRONZE_LAYER'),
       ('NY_BIKE_DATA','ANNUAL','s3//:bucket-raw-data/raw_data_nybike','s3a://bucket-raw-data/raw_data_nybike/2021-citibike-tripdata/*/*.csv',2021,0,'2021','TO_BRONZE_LAYER'),
       ('NY_BIKE_DATA','ANNUAL','s3//:bucket-raw-data/raw_data_nybike','s3a://bucket-raw-data/raw_data_nybike/2022-citibike-tripdata/*/*.csv',2022,0,'2022','TO_BRONZE_LAYER'),
       ('NY_BIKE_DATA','ANNUAL','s3//:bucket-raw-data/raw_data_nybike','s3a://bucket-raw-data/raw_data_nybike/2023-citibike-tripdata/*/*.csv',2023,0,'2023','TO_BRONZE_LAYER'),
       ('NY_BIKE_DATA','MONTH','s3//:bucket-raw-data/raw_data_nybike','s3a://bucket-raw-data/raw_data_nybike/2024-citibike-tripdata/01/*.csv',2024,1,'202401','TO_BRONZE_LAYER'),
       ('NY_BIKE_DATA','MONTH','s3//:bucket-raw-data/raw_data_nybike','s3a://bucket-raw-data/raw_data_nybike/2024-citibike-tripdata/02/*.csv',2024,2,'202402','TO_BRONZE_LAYER'),
       ('NY_BIKE_DATA','MONTH','s3//:bucket-raw-data/raw_data_nybike','s3a://bucket-raw-data/raw_data_nybike/2024-citibike-tripdata/03/*.csv',2024,3,'202403','TO_BRONZE_LAYER'),
       ('NY_BIKE_DATA','MONTH','s3//:bucket-raw-data/raw_data_nybike','s3a://bucket-raw-data/raw_data_nybike/2024-citibike-tripdata/04/*.csv',2024,4,'202404','TO_BRONZE_LAYER'),
       ('NY_BIKE_DATA','MONTH','s3//:bucket-raw-data/raw_data_nybike','s3a://bucket-raw-data/raw_data_nybike/2024-citibike-tripdata/05/*.csv',2024,5,'202405','TO_BRONZE_LAYER'),
       ('NY_BIKE_DATA','MONTH','s3//:bucket-raw-data/raw_data_nybike','s3a://bucket-raw-data/raw_data_nybike/2024-citibike-tripdata/06/*.csv',2024,6,'202406','TO_BRONZE_LAYER'),
       ('NY_BIKE_DATA','MONTH','s3//:bucket-raw-data/raw_data_nybike','s3a://bucket-raw-data/raw_data_nybike/2024-citibike-tripdata/07/*.csv',2024,7,'202407','TO_BRONZE_LAYER'),
       ('NY_BIKE_DATA','MONTH','s3//:bucket-raw-data/raw_data_nybike','s3a://bucket-raw-data/raw_data_nybike/2024-citibike-tripdata/08/*.csv',2024,8,'202408','TO_BRONZE_LAYER'),
       ('NY_BIKE_DATA','MONTH','s3//:bucket-raw-data/raw_data_nybike','s3a://bucket-raw-data/raw_data_nybike/2024-citibike-tripdata/09/*.csv',2024,9,'202409','TO_BRONZE_LAYER'),
       ('NY_BIKE_DATA','MONTH','s3//:bucket-raw-data/raw_data_nybike','s3a://bucket-raw-data/raw_data_nybike/2024-citibike-tripdata/10/*.csv',2024,10,'202410','TO_BRONZE_LAYER'),
       ('NY_BIKE_DATA','MONTH','s3//:bucket-raw-data/raw_data_nybike','s3a://bucket-raw-data/raw_data_nybike/2024-citibike-tripdata/11/*.csv',2024,11,'202411','TO_BRONZE_LAYER'),
       ('NY_BIKE_DATA','MONTH','s3//:bucket-raw-data/raw_data_nybike','s3a://bucket-raw-data/raw_data_nybike/2024-citibike-tripdata/12/*.csv',2024,12,'202412','TO_BRONZE_LAYER'),
       ('NY_BIKE_DATA','MONTH','s3//:bucket-raw-data/raw_data_nybike','s3a://bucket-raw-data/raw_data_nybike/2025-citibike-tripdata/01/*.csv',2025,1,'202501','TO_BRONZE_LAYER'),
       ('NY_BIKE_DATA','MONTH','s3//:bucket-raw-data/raw_data_nybike','s3a://bucket-raw-data/raw_data_nybike/2025-citibike-tripdata/02/*.csv',2025,2,'202502','TO_BRONZE_LAYER'),
       ('NY_BIKE_DATA','MONTH','s3//:bucket-raw-data/raw_data_nybike','s3a://bucket-raw-data/raw_data_nybike/2025-citibike-tripdata/03/*.csv',2025,3,'202503','TO_BRONZE_LAYER'),
       ('NY_BIKE_DATA','MONTH','s3//:bucket-raw-data/raw_data_nybike','s3a://bucket-raw-data/raw_data_nybike/2025-citibike-tripdata/04/*.csv',2025,4,'202504','TO_BRONZE_LAYER'),
       ('NY_BIKE_DATA','MONTH','s3//:bucket-raw-data/raw_data_nybike','s3a://bucket-raw-data/raw_data_nybike/2025-citibike-tripdata/05/*.csv',2025,5,'202505','TO_BRONZE_LAYER'),
       ('NY_BIKE_DATA','MONTH','s3//:bucket-raw-data/raw_data_nybike','s3a://bucket-raw-data/raw_data_nybike/2025-citibike-tripdata/06/*.csv',2025,6,'202506','TO_BRONZE_LAYER'),
       ('NY_BIKE_DATA','MONTH','s3//:bucket-raw-data/raw_data_nybike','s3a://bucket-raw-data/raw_data_nybike/2025-citibike-tripdata/07/*.csv',2025,7,'202507','TO_BRONZE_LAYER'),
       ('NY_BIKE_DATA','MONTH','s3//:bucket-raw-data/raw_data_nybike','s3a://bucket-raw-data/raw_data_nybike/2025-citibike-tripdata/08/*.csv',2025,8,'202508','TO_BRONZE_LAYER'),
       ('NY_BIKE_DATA','MONTH','s3//:bucket-raw-data/raw_data_nybike','s3a://bucket-raw-data/raw_data_nybike/2025-citibike-tripdata/09/*.csv',2025,9,'202509','TO_BRONZE_LAYER'),
       ('NY_BIKE_DATA','MONTH','s3//:bucket-raw-data/raw_data_nybike','s3a://bucket-raw-data/raw_data_nybike/2025-citibike-tripdata/10/*.csv',2025,10,'202510','TO_BRONZE_LAYER')
;



INSERT INTO data_to_process (data_source_name,process_period,bucket_path,files,year,month,period_tag,status)
VALUES ('NY_BIKE_DATA','ANNUAL','s3//:bucket-raw-data/raw_data_nybike','s3a://bucket-raw-data/raw_data_nybike/2013-citibike-tripdata/*/*.csv',2013,0,'2013','TO_BRONZE_LAYER'),
       ('NY_BIKE_DATA','ANNUAL','s3//:bucket-raw-data/raw_data_nybike','s3a://bucket-raw-data/raw_data_nybike/2014-citibike-tripdata/*/*.csv',2014,0,'2014','TO_BRONZE_LAYER'),
       ('NY_BIKE_DATA','ANNUAL','s3//:bucket-raw-data/raw_data_nybike','s3a://bucket-raw-data/raw_data_nybike/2015-citibike-tripdata/*/*.csv',2015,0,'2015','TO_BRONZE_LAYER'),
       ('NY_BIKE_DATA','ANNUAL','s3//:bucket-raw-data/raw_data_nybike','s3a://bucket-raw-data/raw_data_nybike/2016-citibike-tripdata/*/*.csv',2016,0,'2016','TO_BRONZE_LAYER'),
       ('NY_BIKE_DATA','ANNUAL','s3//:bucket-raw-data/raw_data_nybike','s3a://bucket-raw-data/raw_data_nybike/2017-citibike-tripdata/*/*.csv',2017,0,'2017','TO_BRONZE_LAYER'),
       ('NY_BIKE_DATA','ANNUAL','s3//:bucket-raw-data/raw_data_nybike','s3a://bucket-raw-data/raw_data_nybike/2018-citibike-tripdata/*/*.csv',2018,0,'2018','TO_BRONZE_LAYER'),
       ('NY_BIKE_DATA','ANNUAL','s3//:bucket-raw-data/raw_data_nybike','s3a://bucket-raw-data/raw_data_nybike/2019-citibike-tripdata/*/*.csv',2019,0,'2019','TO_BRONZE_LAYER'),
       ('NY_BIKE_DATA','ANNUAL','s3//:bucket-raw-data/raw_data_nybike','s3a://bucket-raw-data/raw_data_nybike/2020-citibike-tripdata/*/*.csv',2020,0,'2020','TO_BRONZE_LAYER'),
       ('NY_BIKE_DATA','ANNUAL','s3//:bucket-raw-data/raw_data_nybike','s3a://bucket-raw-data/raw_data_nybike/2021-citibike-tripdata/*/*.csv',2021,0,'2021','TO_BRONZE_LAYER'),
       ('NY_BIKE_DATA','ANNUAL','s3//:bucket-raw-data/raw_data_nybike','s3a://bucket-raw-data/raw_data_nybike/2022-citibike-tripdata/*/*.csv',2022,0,'2022','TO_BRONZE_LAYER'),
       ('NY_BIKE_DATA','ANNUAL','s3//:bucket-raw-data/raw_data_nybike','s3a://bucket-raw-data/raw_data_nybike/2023-citibike-tripdata/*/*.csv',2023,0,'2023','TO_BRONZE_LAYER'),
       ('NY_BIKE_DATA','ANNUAL','s3//:bucket-raw-data/raw_data_nybike','s3a://bucket-raw-data/raw_data_nybike/2024-citibike-tripdata/*/*.csv',2024,0,'2024','TO_BRONZE_LAYER'),
       ('NY_BIKE_DATA','MULTIMONTH','s3//:bucket-raw-data/raw_data_nybike','s3a://bucket-raw-data/raw_data_nybike/2025-citibike-tripdata/*/*.csv',2025,0,'2025','TO_BRONZE_LAYER'),

;




CREATE TABLE audit (
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

-- "s3a://bucket-raw-data/raw_data_nybike/2021-citibike-tripdata/*/*.csv"