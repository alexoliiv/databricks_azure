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
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step 2 - Define data structure

# COMMAND ----------

pitstops_schema = StructType(
    fields=[
        StructField("raceId", IntegerType(), False),
        StructField("driverId", IntegerType(), True),
        StructField("stop", StringType(), True),
        StructField("lap", IntegerType(), True),
        StructField("time", StringType(), True),
        StructField("duration", StringType(), True),
        StructField("milliseconds", IntegerType(), True),
    ]
)

# COMMAND ----------

pitstops_df = spark.read \
.schema(pitstops_schema) \
    .json("/mnt/formula1dlspalex/raw/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Renaming columns

# COMMAND ----------

pitstops_renamed_df = (
    pitstops_df.withColumnRenamed("raceid", "race_id")
    .withColumnRenamed("driverId", "driver_id")
    .withColumn("ingestion_date", current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Write output to parquet file

# COMMAND ----------

pitstops_renamed_df.write.mode("overwrite").partitionBy("race_id").parquet("/mnt/formula1dlspalex/processed/pit_stops")

# COMMAND ----------


