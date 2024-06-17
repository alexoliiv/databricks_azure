-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Create circuits table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(circuitId INT,
circuitRef STRING,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING
)

USING csv
OPTIONS(path "/mnt/formula1dlspalex/bronze/circuits.csv", header true)

-- COMMAND ----------

SELECT *
FROM f1_raw.circuits

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Create races table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.races(raceId INT,
year INT,
round INT,
circuitId INT,
name STRING,
date DATE,
time STRING,
url STRING
)

USING csv
OPTIONS(path "/mnt/formula1dlspalex/bronze/races.csv", header true)

-- COMMAND ----------

SELECT *
FROM f1_raw.races

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Create constructors table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
  constructorId INT,
  constructorRef STRING,
  name STRING,
  nationality STRING,
  url STRING)
  USING json
  OPTIONS(path "/mnt/formula1dlspalex/bronze/constructors.json")


-- COMMAND ----------

SELECT *
FROM f1_raw.constructors;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create drivers table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
  driverId STRING,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename: STRING, surname: STRING>,
  dob DATE,
  nationality STRING,
  url STRING
)
USING json
OPTIONS (path "/mnt/formula1dlspalex/bronze/drivers.json")

-- COMMAND ----------

SELECT *
FROM f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC #### Create results table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
    resultId INT NOT NULL,
    raceId INT,
    driverId INT,
    constructorId INT,
    number INT,
    grid INT,
    position INT,
    positionText STRING,
    positionOrder INT,
    points FLOAT,
    laps INT,
    time STRING,
    milliseconds INT,
    fastestLap INT,
    rank INT,
    fastestLapTime STRING,
    fastestLapSpeed STRING,
    statusId INT
)
USING json
OPTIONS (path "/mnt/formula1dlspalex/bronze/results.json")


-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Create pit stops table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops (
    raceId INT NOT NULL,
    driverId INT,
    stop STRING,
    lap INT,
    time STRING,
    duration STRING,
    milliseconds INT
)
USING json
OPTIONS(path "/mnt/formula1dlspalex/bronze/pit_stops.json",
        'multiLine' =  'true')

-- COMMAND ----------

SELECT *
FROM f1_raw.pit_stops;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create tables for list of files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times (
    raceId INT ,
    driverId INT,
    lap INT,
    position INT,
    time STRING,
    milliseconds int
)
USING csv
OPTIONS(path "/mnt/formula1dlspalex/bronze/lap_times/")

-- COMMAND ----------

SELECT *
FROM f1_raw.lap_times;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Create qualifying table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying (
    qualifyId INT ,
    raceId INT,
    driverId INT,
    constructorId INT,
    number STRING,
    position int,
    q1 STRING,
    q2 STRING,
    q3 STRING
)
USING json
OPTIONS("path" = "/mnt/formula1dlspalex/bronze/qualifying/",
"multiLine" =  "true")

-- COMMAND ----------

SELECT *
FROM f1_raw.qualifying;

-- COMMAND ----------

