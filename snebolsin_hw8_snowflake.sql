USE WAREHOUSE SNEBOLSIN_WH
USE DATABASE SNEBOLSIN_DB

// Create storage integration
CREATE OR REPLACE STORAGE INTEGRATION s3_int_ss
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::554739427960:role/snowflake-read-only-ssnebolsin-role'
  STORAGE_ALLOWED_LOCATIONS = ('*');

// check storage integration metadata
  DESC INTEGRATION s3_int_ss;

// Create csv file format
  CREATE OR REPLACE FILE FORMAT csv_format
    TYPE = 'CSV'
    COMPRESSION = 'AUTO'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    TRIM_SPACE = TRUE,
    FIELD_OPTIONALLY_ENCLOSED_BY='"'
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    ;

// Create parquet file format
    CREATE OR REPLACE FILE FORMAT parquet_format
    TYPE = 'PARQUET'
    COMPRESSION = 'AUTO';

// Create S3 Stage 
  CREATE OR REPLACE STAGE s3_ss_stage
  STORAGE_INTEGRATION = s3_int_ss
  URL = 's3://robot-dreams-source-data/'
  FILE_FORMAT = csv_format;

// Check stage files
  LIST @s3_ss_stage;

  SELECT t.$1, t.$2, t.$3, t.$4 FROM @s3_ss_stage/home-work-1/nyc_taxi/taxi_zone_lookup.csv as t;


// Create Zone Lookup table
  CREATE OR REPLACE TABLE TAXI_ZONE_LOOKUP (
    LOCATIONID VARCHAR,
    BOROUGH VARCHAR,
    ZONE VARCHAR,
    SERIVCE_ZONE VARCHAR
  );

// Copy external csv into Zone Lookup table
  COPY INTO TAXI_ZONE_LOOKUP
  FROM @s3_ss_stage/home-work-1/nyc_taxi/taxi_zone_lookup.csv;

// check zone_lookup table
  select *
  from TAXI_ZONE_LOOKUP;
  
// check data directly from s3
  select top 100 *
  FROM @s3_ss_stage/home-work-1/nyc_taxi/yellow/2014/yellow_tripdata_2014-01.parquet (FILE_FORMAT => 'parquet_format')

// create yellow_raw table
  CREATE OR REPLACE TABLE YELLOW_RAW (
    VendorID                BIGINT,
    tpep_pickup_datetime    BIGINT,
    tpep_dropoff_datetime   BIGINT,
    passenger_count         DOUBLE,
    trip_distance           DOUBLE,
    RatecodeID              DOUBLE,
    store_and_fwd_flag      STRING,
    PULocationID            BIGINT,
    DOLocationID            BIGINT,
    payment_type            BIGINT,
    fare_amount             DOUBLE,
    extra                   DOUBLE,
    mta_tax                 DOUBLE,
    tip_amount              DOUBLE,
    tolls_amount            DOUBLE,
    improvement_surcharge   DOUBLE,
    total_amount            DOUBLE,
    congestion_surcharge    DOUBLE,
    airport_fee             DOUBLE,
    lpep_pickup_datetime    STRING,  
    lpep_dropoff_datetime   STRING,
    ehail_fee               STRING,  
    trip_type               STRING
);

// create green_raw table
CREATE OR REPLACE TABLE GREEN_RAW (
    VendorID BIGINT,
    lpep_pickup_datetime BIGINT,
    lpep_dropoff_datetime BIGINT,
    store_and_fwd_flag STRING,
    RatecodeID DOUBLE,
    PULocationID BIGINT,
    DOLocationID BIGINT,
    passenger_count BIGINT,
    trip_distance DOUBLE,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    ehail_fee DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE,
    payment_type BIGINT,
    trip_type DOUBLE,
    congestion_surcharge DOUBLE
);

// copy data in yellow_raw
    COPY INTO YELLOW_RAW
        FROM @s3_ss_stage/home-work-1/nyc_taxi/yellow
        FILE_FORMAT = (TYPE = 'PARQUET')
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
        
// copy data in green_raw
    COPY INTO GREEN_RAW
        FROM @s3_ss_stage/home-work-1/nyc_taxi/green
        FILE_FORMAT = (TYPE = 'PARQUET')
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
        
//check data
    select top 1000 *
  from YELLOW_RAW;

  select top 1000 *
  from GREEN_RAW;

  select *
  from TAXI_ZONE_LOOKUP;
  

  // create green enriched
  CREATE OR REPLACE TABLE green_enriched as 
  Select 
  g.*, 
  z1.zone as pickup_zone_name,
  z2.zone as dropoff_zone_name
  from GREEN_RAW as g
  left join TAXI_ZONE_LOOKUP as z1
  on g.pulocationid = z1.locationid
  left join TAXI_ZONE_LOOKUP as z2
  on g.dolocationid = z2.locationid
  
  // create yellow enriched
  CREATE OR REPLACE TABLE  yellow_enriched as 
  Select
  y.*, 
  z1.zone as pickup_zone_name,
  z2.zone as dropoff_zone_name
  from YELLOW_RAW as y
  left join TAXI_ZONE_LOOKUP as z1
  on y.pulocationid = z1.locationid
  left join TAXI_ZONE_LOOKUP as z2
  on y.dolocationid = z2.locationid

  
  
-- * Фільтруйте записи:
--   * trip_distance > 0
--   * total_amount > 0
--   * passenger_count між 1 та 6

ALTER TABLE green_enriched CLUSTER BY (passenger_count);

  select top 100 * 
  from green_enriched
  where trip_distance > 0
  and total_amount > 0
  and passenger_count between 1 and 6

  
  -- Додайте колонку trip_category:
  --  Short (до 2 км), Medium (2–10 км), Long (>10 км)

  select top 100 *,
  case 
    when trip_distance < 2 then 'Short'
    when trip_distance between 2 and 10 then 'Medium'
    when trip_distance > 10 then 'Long' end as trip_category
  from green_enriched

  
 -- Додайте колонку pickup_hour: годину з pickup_datetime

 select top 100 *,
    EXTRACT(HOUR FROM TO_TIMESTAMP(lpep_pickup_datetime)) as pickup_hour
 from green_enriched

-- Створіть агреговану таблицю з підрахунком по зонах

 select
 pickup_zone_name,
 dropoff_zone_name,
 count(*) as total_cnt,
 sum(total_amount) as total_amount,
 avg(fare_amount) as avg_fare_amount
 from green_enriched
 group by 1,2
  

  
-- * Видаліть кілька записів зі збагаченої таблиці (наприклад, green_enriched)
DELETE FROM green_enriched
where pickup_zone_name = 'Greenpoint'

-- * За допомогою Time Travel:
--   * Перевірте стару версію таблиці (через AT або BEFORE)

SELECT * FROM green_enriched BEFORE(STATEMENT => LAST_QUERY_ID())
where pickup_zone_name = 'Greenpoint';

--   * Відновіть видалені записи у нову таблицю або у ту ж (через INSERT SELECT)

select  *
from  table(information_schema.query_history())
where database_name = 'SNEBOLSIN_DB'

SHOW TABLES LIKE 'GREEN_ENRICHED';

INSERT INTO green_enriched
SELECT *
FROM GREEN_ENRICHED
AT (STATEMENT => '01be4dd7-0001-64eb-0001-600a000ab91a')
where pickup_zone_name = 'Greenpoint';


-- * Створіть Stream на таблиці yellow_enriched

CREATE STREAM yellow_enriched_stream ON TABLE yellow_enriched;

select * from yellow_enriched
where tip_amount = 1.35 and tpep_pickup_datetime = 1432337641000000 and pickup_zone_name is null

-- * Додайте нові записи вручну або через COPY INTO

insert into yellow_enriched
select *, null, null from yellow_raw
where tip_amount = 1.35 and tpep_pickup_datetime = 1432337641000000


delete from yellow_enriched
where tip_amount = 1.35 and tpep_pickup_datetime = 1432337641000000 and pickup_zone_name is null


-- * Перевірте, що Stream відображає INSERT/UPDATE зміни

select * from yellow_enriched_stream


-- * Створіть цільову таблицю yellow_changes_log для зберігання змін

CREATE TABLE yellow_changes_log LIKE yellow_enriched;

ALTER TABLE yellow_changes_log
ADD COLUMN ACTION_NAME STRING;

-- * Створіть Task, який щогодини:
--   * Зчитує зміни зі Stream
--   * Вставляє нові записи у yellow_changes_log
-- * Використайте SCHEDULE = '1 HOUR' або WAREHOUSE = '...' для запуску


CREATE OR REPLACE TASK save_changes
  WAREHOUSE = SNEBOLSIN_WH
  SCHEDULE = '1 HOUR'
  AS
    INSERT INTO yellow_changes_log
    SELECT * EXCLUDE(METADATA$ISUPDATE, METADATA$ROW_ID)
     FROM yellow_enriched_stream;

EXECUTE TASK save_changes


select * from yellow_changes_log


-- * Зробіть окремий Task для агрегованої статистики:
--   * Середня відстань
--   * Середня ціна
--   * Кількість поїздок
--   * Зберігайте у таблицю zone_hourly_stats
-- * Використайте SCHEDULE = '1 HOUR' або WAREHOUSE = '...' для запуску


CREATE OR REPLACE TABLE zone_hourly_agg AS
select
 pickup_zone_name,
 dropoff_zone_name,
 avg(trip_distance) as avg_distance,
 avg(fare_amount) as avg_fare_amount,
 count(*) as trips_cnt
 from green_enriched
 group by 1,2

CREATE TABLE zone_hourly_stats LIKE zone_hourly_agg;
 

 ALTER TABLE zone_hourly_stats
ADD COLUMN ACTION_NAME STRING;

ALTER TABLE zone_hourly_stats
ADD COLUMN IS_UPDATED STRING;

CREATE STREAM agg_green_stream ON TABLE zone_hourly_agg;
 

CREATE OR REPLACE TASK save_hourly_stats
  WAREHOUSE = SNEBOLSIN_WH
  SCHEDULE = '1 HOUR'
  AS
    INSERT INTO zone_hourly_stats
    SELECT * EXCLUDE(METADATA$ROW_ID)
     FROM agg_green_stream;

EXECUTE TASK save_hourly_stats


select * from zone_hourly_agg

select * from zone_hourly_stats

UPDATE zone_hourly_agg
SET avg_distance = 99999
where pickup_zone_name = 'Williamsburg (North Side)' and dropoff_zone_name = 'Financial District North'

select * from zone_hourly_stats




-- * Об’єднайте yellow_enriched та green_enriched у таблицю all_trips

select top 1 * from yellow_enriched

CREATE TABLE all_trips as 
SELECT 
    VENDORID,
    TPEP_PICKUP_DATETIME,
    TPEP_DROPOFF_DATETIME,
    PASSENGER_COUNT,
    TRIP_DISTANCE,
    RATECODEID,
    STORE_AND_FWD_FLAG,
    PULOCATIONID,
    DOLOCATIONID,
    PAYMENT_TYPE,
    FARE_AMOUNT,
    EXTRA,
    MTA_TAX,
    TIP_AMOUNT,
    TOLLS_AMOUNT,
    IMPROVEMENT_SURCHARGE,
    TOTAL_AMOUNT,
    CONGESTION_SURCHARGE,
    AIRPORT_FEE,
    LPEP_PICKUP_DATETIME,
    LPEP_DROPOFF_DATETIME,
    EHAIL_FEE,
    TRIP_TYPE
FROM yellow_enriched
UNION ALL 
SELECT 
    VENDORID,
    NULL AS TPEP_PICKUP_DATETIME,
    NULL AS TPEP_DROPOFF_DATETIME,
    PASSENGER_COUNT,
    TRIP_DISTANCE,
    RATECODEID,
    STORE_AND_FWD_FLAG,
    PULOCATIONID,
    DOLOCATIONID,
    PAYMENT_TYPE,
    FARE_AMOUNT,
    EXTRA,
    MTA_TAX,
    TIP_AMOUNT,
    TOLLS_AMOUNT,
    IMPROVEMENT_SURCHARGE,
    TOTAL_AMOUNT,
    CONGESTION_SURCHARGE,
    NULL AS AIRPORT_FEE,
    LPEP_PICKUP_DATETIME,
    LPEP_DROPOFF_DATETIME,
    EHAIL_FEE,
    TRIP_TYPE
FROM green_enriched;


-- * Створіть Stored Procedure:
--   * Перевірка на дублікати перед вставкою
--   * Запис результату в лог-таблицю


// create test data tables with Zero-Copy Cloning
CREATE TABLE zone_hourly_agg_source CLONE zone_hourly_agg

CREATE TABLE zone_hourly_agg_target as
select * from zone_hourly_agg
where pickup_zone_name = 'Bushwick South' and dropoff_zone_name = 'Murray Hill';


insert into zone_hourly_agg_target
select * from zone_hourly_agg
where pickup_zone_name = 'Bushwick South' and dropoff_zone_name = 'Murray Hill'


// create logs table
CREATE TABLE zone_hourly_agg_target_logs LIKE zone_hourly_agg_target;
 

 ALTER TABLE zone_hourly_agg_target_logs
ADD COLUMN ACTION_NAME STRING;

ALTER TABLE zone_hourly_agg_target_logs
ADD COLUMN IS_UPDATED STRING;


// create stream
CREATE STREAM zone_hourly_agg_target_stream ON TABLE zone_hourly_agg_target;
 

// create proc
CREATE OR REPLACE PROCEDURE insert_data(
pickup_zone_name string,
dropoff_zone_name string
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE 
 check_cnt INT;
BEGIN
    with cte_tbl as (
    select * from zone_hourly_agg_target
    union all
    select * from zone_hourly_agg_source
    where pickup_zone_name = :pickup_zone_name 
        and dropoff_zone_name = :dropoff_zone_name
    ),
    cte_tbl2 as (
        select *, count(*) as cnt
        from cte_tbl
        group by all
        having count(*) > 1
    )
    select count(*) INTO check_cnt
    from cte_tbl2;

    IF (check_cnt > 0) THEN
        RETURN 'Duplicate found. Row not inserted.';
    ELSE
        INSERT INTO zone_hourly_agg_target 
        select * from zone_hourly_agg_source
        where pickup_zone_name = :pickup_zone_name 
        and dropoff_zone_name = :dropoff_zone_name;

        INSERT INTO zone_hourly_agg_target_logs
            SELECT * EXCLUDE(METADATA$ROW_ID)
             FROM zone_hourly_agg_target_stream;

        RETURN 'Row inserted successfully. Logs updated successfully';
    END IF;

    
    
END;
$$;

// test proc
CALL insert_data('Bushwick South','Murray Hill')

CALL insert_data('Test3','Test4')

// test data
select * from zone_hourly_agg_target
select * from zone_hourly_agg_target_logs


// insert data into source table for testing    
INSERT INTO zone_hourly_agg_source (
    PICKUP_ZONE_NAME,
    DROPOFF_ZONE_NAME,
    AVG_DISTANCE,
    AVG_FARE_AMOUNT,
    TRIPS_CNT
)
VALUES (
    'Test3',
    'Test4',
    2222,
    2222,
    2222
);


-- * Налаштуйте Zero-Copy Cloning бази для створення середовища taxi_dev

CREATE DATABASE taxi_dev CLONE SNEBOLSIN_DB;



  