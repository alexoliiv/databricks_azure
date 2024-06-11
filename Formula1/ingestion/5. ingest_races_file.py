# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### Ingest results.json file

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC #### 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType,FloatType
from pyspark.sql.functions import current_timestamp,concat,lit

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step 2 - Define data structure

# COMMAND ----------

results_schema = StructType(
    fields=[
        StructField("resultId"  , IntegerType(), False),
        StructField("raceId"    , IntegerType (), True),
        StructField("driverId"  , IntegerType(), True),
        StructField("constructorId", IntegerType(), True),
        StructField("number"    , IntegerType(), True),
        StructField("grid"      , IntegerType(), True),
        StructField("position"  , IntegerType(), True),
        StructField("positionText", StringType(), True),
        StructField("positionOrder", IntegerType(), True),
        StructField("points"    , FloatType(), True),
        StructField("laps"      , IntegerType(), True),
        StructField("time"      , StringType(), True),
        StructField("milliseconds", IntegerType(), True),
        StructField("fastestLap", IntegerType(), True),
        StructField("rank"      , IntegerType(), True),
        StructField("fastestLapTime", StringType(), True),
        StructField("fastestLapSpeed", StringType(), True),
        StructField("statusId"  , IntegerType(), True),

    ]
)

# COMMAND ----------

results_df = spark.read \
.schema(results_schema) \
    .json("/mnt/formula1dlspalex/raw/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Renaming columns

# COMMAND ----------

results_renamed_df = (
    results_df.withColumnRenamed("resultId", "result_id")
    .withColumnRenamed("raceId", "race_id")
    .withColumnRenamed("driverId", "driver_id")
    .withColumnRenamed("constructorId", "constructor_id")
    .withColumnRenamed("positionText", "position_text")
    .withColumnRenamed("positionOrder", "position_order")
    .withColumnRenamed("fastestLap","fastest_lap")
    .withColumnRenamed("fastestLapTime","fastest_lap_time")
    .withColumnRenamed("fastestLapSpeed","fastest_lap_speed")
    .withColumn("ingestion_date", current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Drop unwanted columns from the dataframe

# COMMAND ----------

results_final_df = results_renamed_df.drop(results_renamed_df['statusId'])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Write output to parquet file

# COMMAND ----------

results_final_df.write.mode("overwrite").partitionBy("race_id").parquet("/mnt/formula1dlspalex/processed/results")

# COMMAND ----------


