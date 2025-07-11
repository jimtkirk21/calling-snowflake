--------------------------------------------------------------------------------
-- 0. Setup: DB, Schema, Warehouses
--------------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS CITIBIKE;
GRANT OWNERSHIP ON DATABASE CITIBIKE TO ROLE SYSADMIN REVOKE CURRENT GRANTS;

USE ROLE SYSADMIN;
USE DATABASE CITIBIKE;
CREATE SCHEMA IF NOT EXISTS WORK;
USE SCHEMA WORK;

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

CREATE WAREHOUSE IF NOT EXISTS MULTI_COMPUTE_WH
  WAREHOUSE_SIZE = SMALL
  MIN_CLUSTER_COUNT = 1
  MAX_CLUSTER_COUNT = 3
  SCALING_POLICY = ECONOMY
  AUTO_SUSPEND = 180
  AUTO_RESUME = TRUE;

--------------------------------------------------------------------------------
-- 1. File Format & Stage
--------------------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT FF_CSV
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 0
  FIELD_OPTIONALLY_ENCLOSED_BY = '"';

CREATE OR REPLACE STAGE S_CITIBIKE_TRIPS
  URL = 's3://snowflake-workshop-lab/citibike-trips-csv/'
  FILE_FORMAT = FF_CSV;

LIST @S_CITIBIKE_TRIPS;

--------------------------------------------------------------------------------
-- 2. Main TRIPS Table + Load
--------------------------------------------------------------------------------
CREATE OR REPLACE TABLE TRIPS (
  tripduration INTEGER,
  starttime TIMESTAMP,
  stoptime TIMESTAMP,
  start_station_id INTEGER,
  start_station_name STRING,
  start_station_latitude FLOAT,
  start_station_longitude FLOAT,
  end_station_id INTEGER,
  end_station_name STRING,
  end_station_latitude FLOAT,
  end_station_longitude FLOAT,
  bikeid INTEGER,
  membership_type STRING,
  usertype STRING,
  birth_year INTEGER,
  gender INTEGER
) DATA_RETENTION_TIME_IN_DAYS = 1;

USE WAREHOUSE COMPUTE_WH;

COPY INTO TRIPS FROM @S_CITIBIKE_TRIPS ON_ERROR = CONTINUE;
CREATE OR REPLACE TABLE TRIPS_BACKUP CLONE TRIPS;
CREATE OR REPLACE TABLE TRIPS_CLONED CLONE TRIPS;
SELECT COUNT(*) FROM TRIPS;

--------------------------------------------------------------------------------
-- 3. Time Travel Safe Test
--------------------------------------------------------------------------------
CREATE OR REPLACE TABLE TRIPS_BACKUP CLONE TRIPS;

UPDATE TRIPS SET start_station_name = 'Central Park S & 6 Ave TMP'
WHERE start_station_id = 2006;

SELECT DISTINCT start_station_name 
FROM TRIPS AT (OFFSET => -10) WHERE start_station_id = 2006;

SELECT DISTINCT start_station_name FROM TRIPS_BACKUP WHERE start_station_id = 2006;
SELECT DISTINCT start_station_name FROM TRIPS_CLONED WHERE start_station_id = 2006;

CREATE OR REPLACE TABLE TRIPS_CLONED CLONE TRIPS AT (OFFSET => -10);

--------------------------------------------------------------------------------
-- 4. RBAC: Roles & Grants
--------------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

GRANT SELECT ON TABLE TRIPS_BACKUP TO ROLE SYSADMIN;

CREATE OR REPLACE ROLE ANALYST;
GRANT OWNERSHIP ON ROLE ANALYST TO ROLE SECURITYADMIN REVOKE CURRENT GRANTS;

CREATE OR REPLACE ROLE DEVELOPER;
GRANT ROLE ANALYST TO ROLE DEVELOPER;
GRANT ROLE DEVELOPER TO ROLE SYSADMIN;

GRANT USAGE ON DATABASE CITIBIKE TO ROLE ANALYST;
GRANT USAGE ON SCHEMA CITIBIKE.WORK TO ROLE ANALYST;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE ANALYST;
GRANT SELECT ON TABLE CITIBIKE.WORK.TRIPS TO ROLE ANALYST;
GRANT CREATE TABLE ON SCHEMA CITIBIKE.WORK TO ROLE ANALYST;

GRANT USAGE ON DATABASE CITIBIKE TO ROLE DEVELOPER;
GRANT USAGE ON SCHEMA CITIBIKE.WORK TO ROLE DEVELOPER;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA CITIBIKE.WORK TO ROLE DEVELOPER;

GRANT SELECT ON FUTURE TABLES IN SCHEMA CITIBIKE.WORK TO ROLE ANALYST;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA CITIBIKE.WORK TO ROLE DEVELOPER;

--------------------------------------------------------------------------------
-- 5. TMP Scratch Table Pattern
--------------------------------------------------------------------------------
USE ROLE SYSADMIN;
USE DATABASE CITIBIKE; 
USE SCHEMA WORK;

DROP TABLE IF EXISTS TMP;
CREATE TABLE TMP AS SELECT * FROM TRIPS WHERE 1=0;

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE TMP TO ROLE ANALYST;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE TMP TO ROLE DEVELOPER;

--------------------------------------------------------------------------------
-- 6. JSON: Sample + Flatten
--------------------------------------------------------------------------------
CREATE OR REPLACE TABLE JSON_SAMPLE (value VARIANT);

INSERT INTO JSON_SAMPLE SELECT PARSE_JSON('{"id":1, "name":"Alice"}');
INSERT INTO JSON_SAMPLE SELECT PARSE_JSON('{"id":2, "name":"Bob"}');

CREATE OR REPLACE VIEW JSON_SAMPLE_VIEW AS
SELECT value:id::INTEGER AS id, value:name::STRING AS name FROM JSON_SAMPLE;

SELECT * FROM JSON_SAMPLE_VIEW;

--------------------------------------------------------------------------------
-- 7. Advanced JSON: Trips per station & flatten
--------------------------------------------------------------------------------
CREATE OR REPLACE TABLE json_trips_per_station AS
WITH individual_trips AS (
  SELECT OBJECT_CONSTRUCT(
    'startStation', start_station_name,
    'duration', tripduration,
    'endStation', end_station_name,
    'membershipType', membership_type,
    'userDetails', OBJECT_CONSTRUCT('userType', usertype, 'userbirthYear', birth_year)
  ) t, start_station_name, starttime
  FROM trips WHERE DATE_TRUNC('day', starttime) BETWEEN '2018-06-01' AND '2018-06-07'
)
SELECT OBJECT_CONSTRUCT(
  'stationName', start_station_name,
  'day', DATE_TRUNC('day', starttime),
  'trips', ARRAY_AGG(t) OVER (PARTITION BY start_station_name, DATE_TRUNC('day', starttime))
) AS json FROM individual_trips;

SELECT
  t.json:day::timestamp,
  t.json:stationName::varchar,
  f.value:duration::number
FROM json_trips_per_station t,
LATERAL FLATTEN(input => t.json:trips) f
LIMIT 10;

--------------------------------------------------------------------------------
-- 8. Streams & Serverless Tasks
--------------------------------------------------------------------------------
CREATE OR REPLACE TABLE TRIPS_MONTHLY LIKE TRIPS;
CREATE OR REPLACE STREAM STR_TRIPS_MONTHLY ON TABLE TRIPS_MONTHLY;

INSERT INTO TRIPS_MONTHLY SELECT * FROM TRIPS WHERE DATE_TRUNC('month', starttime) = '2018-04-01T00:00:00Z';
SELECT SYSTEM$STREAM_HAS_DATA('STR_TRIPS_MONTHLY');

USE ROLE ACCOUNTADMIN;
GRANT EXECUTE MANAGED TASK ON ACCOUNT TO ROLE SYSADMIN;

USE ROLE SYSADMIN;

CREATE OR REPLACE TASK T_RIDES_AGG
  SCHEDULE = '1 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('STR_TRIPS_MONTHLY')
  AS
  INSERT INTO TRIPS_MONTHLY SELECT * FROM TRIPS WHERE STARTTIME >= CURRENT_DATE();

ALTER TASK T_RIDES_AGG RESUME;

--------------------------------------------------------------------------------
-- 9. Storage Related Features
--------------------------------------------------------------------------------
USE ROLE SYSADMIN;
USE DATABASE CITIBIKE;
USE SCHEMA WORK;

ALTER TABLE TRIPS SET DATA_RETENTION_TIME_IN_DAYS = 1;

CREATE OR REPLACE TABLE TRIPS_BACKUP CLONE TRIPS;

UPDATE TRIPS
SET start_station_name = 'Central Park S & 6 Ave TMP'
WHERE start_station_id = 2006;

SELECT DISTINCT start_station_name
FROM TRIPS_BACKUP
WHERE start_station_id = 2006;

SELECT DISTINCT start_station_name 
FROM TRIPS AT (OFFSET => -10)
WHERE start_station_id = 2006;

SHOW TABLES IN SCHEMA CITIBIKE.WORK;

CREATE OR REPLACE TABLE TRIPS_CLONED CLONE TRIPS AT (OFFSET => -10);

SELECT DISTINCT start_station_name FROM TRIPS_CLONED WHERE start_station_id = 2006;

UPDATE TRIPS SET start_station_name = (
    SELECT DISTINCT start_station_name 
    FROM TRIPS AT (OFFSET => -10)
    WHERE start_station_id = 2006
) WHERE start_station_id = 2006;

DROP TABLE TRIPS;
ALTER TABLE TRIPS_CLONED RENAME TO TRIPS;

--------------------------------------------------------------------------------
-- 10. Semi-Structured Data: Exercise #1
--------------------------------------------------------------------------------
CREATE OR REPLACE TABLE JSON_SAMPLE (value VARIANT);

INSERT INTO JSON_SAMPLE SELECT PARSE_JSON('{"id":1,"first_name":"Madelena","last_name":"Bastiman","email":"mbastiman0@washington.edu","gender":"Female","ip_address":"47.136.171.159","language":"Bislama","city":"Lagunas","street":"Nevada","street_number":"2","phone":"+51 224 307 3778"}');
INSERT INTO JSON_SAMPLE SELECT PARSE_JSON('{"id":2,"first_name":"Jasmine","last_name":"Hayth","email":"jhayth1@soup.io","gender":"Female","ip_address":"44.117.102.69","language":"Papiamento","city":"Sikeshu","street":"Dovetail","street_number":"7922","phone":"+86 710 521 7096"}');

--running further insert statements
insert into json_sample
select parse_json('
{"id":1,"first_name":"Madelena","last_name":"Bastiman","email":"mbastiman0@washington.edu","gender":"Female","ip_address":"47.136.171.159","language":"Bislama","city":"Lagunas","street":"Nevada","street_number":"2","phone":"+51 224 307 3778"}');
insert into json_sample
select parse_json('
{"id":2,"first_name":"Jasmine","last_name":"Hayth","email":"jhayth1@soup.io","gender":"Female","ip_address":"44.117.102.69","language":"Papiamento","city":"Sikeshu","street":"Dovetail","street_number":"7922","phone":"+86 710 521 7096"}');
insert into json_sample
select parse_json('
{"id":3,"first_name":"Doria","last_name":"Brownjohn","email":"dbrownjohn2@unblog.fr","gender":"Female","ip_address":"242.221.58.251","language":"Greek","city":"Trollhättan","street":"Dayton","street_number":"8348","phone":"+46 207 829 2153"}');
insert into json_sample
select parse_json('
{"id":4,"first_name":"Gaylor","last_name":"Enderson","email":"genderson3@istockphoto.com","gender":"Bigender","ip_address":"169.138.17.143","language":"Swahili","city":"Velizh","street":"Sugar","street_number":"1","phone":"+7 659 246 7831"}');
insert into json_sample
select parse_json('
{"id":5,"first_name":"Gaile","last_name":"Elcombe","email":"gelcombe4@japanpost.jp","gender":"Male","ip_address":"13.24.168.205","language":"Haitian Creole","city":"L’vovskiy","street":"Surrey","street_number":"3751","phone":"+7 641 563 0389"}');

SELECT * FROM JSON_SAMPLE;

CREATE OR REPLACE VIEW JSON_SAMPLE_VIEW AS
SELECT
    value:id::INTEGER AS id,
    value:email::STRING AS email,
    value:first_name::STRING AS first_name,
    value:ip_address::STRING AS ip_address,
    value:language::STRING AS language,
    value:city::STRING AS city,
    value:street::STRING AS street,
    value:street_number::INTEGER AS street_number,
    value:phone::STRING AS phone_number
FROM JSON_SAMPLE;

SELECT * FROM JSON_SAMPLE_VIEW;

--------------------------------------------------------------------------------
-- 11. Exercise #2: Create JSON from relational data
--------------------------------------------------------------------------------
SELECT OBJECT_CONSTRUCT(
    'StartStationName', start_station_name,
    'day', DATE_TRUNC('day', starttime),
    'userType', ARRAY_AGG(DISTINCT usertype) OVER (PARTITION BY DATE_TRUNC('day', starttime), start_station_id),
    'tripDetails', OBJECT_CONSTRUCT(
        'endStationName', end_station_name,
        'duration', tripduration
    )
) FROM TRIPS
WHERE DATE_TRUNC('day', starttime) BETWEEN '2018-06-09' AND '2018-06-10'
  AND start_station_id = 239
LIMIT 100;

WITH individual_trips AS (
  SELECT OBJECT_CONSTRUCT(
      'duration', tripduration,
      'endStation', end_station_name,
      'userbirthYear', birth_year,
      'membershipType', membership_type,
      'userType', usertype
  ) AS t,
  start_station_name,
  starttime
  FROM TRIPS
  WHERE DATE_TRUNC('day', starttime) BETWEEN '2018-06-09' AND '2018-06-10'
    AND start_station_id = 239
)
SELECT OBJECT_CONSTRUCT(
    'stationName', start_station_name,
    'day', DATE_TRUNC('day', starttime),
    'trips', ARRAY_AGG(t) OVER (PARTITION BY start_station_name, DATE_TRUNC('day', starttime))
) AS json
FROM individual_trips
LIMIT 100;

--------------------------------------------------------------------------------
-- 12. Exercise #3: Flatten JSON Trips per Station
--------------------------------------------------------------------------------
CREATE OR REPLACE TABLE JSON_TRIPS_PER_STATION AS
WITH individual_trips AS (
  SELECT OBJECT_CONSTRUCT(
      'startStation', start_station_name,
      'duration', tripduration,
      'endStation', end_station_name,
      'membershipType', membership_type,
      'userDetails', OBJECT_CONSTRUCT(
          'userType', usertype,
          'userbirthYear', birth_year
      )
  ) AS t,
  start_station_name,
  starttime
  FROM TRIPS
  WHERE DATE_TRUNC('day', starttime) BETWEEN '2018-06-01' AND '2018-06-07'
)
SELECT OBJECT_CONSTRUCT(
    'stationName', start_station_name,
    'day', DATE_TRUNC('day', starttime),
    'trips', ARRAY_AGG(t) OVER (PARTITION BY start_station_name, DATE_TRUNC('day', starttime))
) AS json
FROM individual_trips;

SELECT
    t.json:day::TIMESTAMP AS start_time,
    t.json:stationName::VARCHAR AS start_station,
    f.value:duration::NUMBER AS duration,
    f.value:endStation::VARCHAR AS end_station,
    f.value:membershipType::VARCHAR AS membership_Type,
    f.value:userDetails:userType::VARCHAR AS user_Type,
    f.value:userDetails:userbirthYear::VARCHAR AS user_Birth_Year
FROM JSON_TRIPS_PER_STATION t,
LATERAL FLATTEN(input => t.json:trips) f
LIMIT 10;