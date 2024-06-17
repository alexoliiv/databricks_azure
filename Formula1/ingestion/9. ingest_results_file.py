# Databricks notebook source
# MAGIC
# MAGIC %md
# MAGIC #### Libraries and configuration

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType,FloatType
from pyspark.sql.functions import current_timestamp,concat,lit

# COMMAND ----------

# MAGIC %run "../includes/configuration"   

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step 2 - Defining data structure and reading .json

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
    .json(f"{bronze_folder_path}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Renaming columns and adding two columns

# COMMAND ----------

results_renamed_df = (
    add_ingestion_date(results_df) 
    
    .withColumnRenamed("resultId", "result_id")
    .withColumnRenamed("raceId", "race_id")
    .withColumnRenamed("driverId", "driver_id")
    .withColumnRenamed("constructorId", "constructor_id")
    .withColumnRenamed("positionText", "position_text")
    .withColumnRenamed("positionOrder", "position_order")
    .withColumnRenamed("fastestLap","fastest_lap")
    .withColumnRenamed("fastestLapTime","fastest_lap_time")
    .withColumnRenamed("fastestLapSpeed","fastest_lap_speed")
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

results_final_df.write.mode("overwrite").parquet(f"{silver_folder_path}/results")
##results_final_df.write.mode("overwrite").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

