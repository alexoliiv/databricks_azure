# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### Ingest lap_times.csv files

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC #### 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType,FloatType
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step 2 - Define data structure and read csv folder

# COMMAND ----------

lap_times_schema = StructType(
    fields=[
        StructField("raceId", IntegerType(), False),
        StructField("driverId", IntegerType(), True),
        StructField("lap", IntegerType(), True),
        StructField("position", IntegerType(), True),
        StructField("time", StringType(), True),
        StructField("milliseconds", IntegerType(), True),
    ]
)

# COMMAND ----------

lap_times_df = spark.read \
.schema(lap_times_schema) \
    .csv("/mnt/formula1dlspalex/raw/lap_times/")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Renaming columns

# COMMAND ----------

lap_times_renamed_df = (
    lap_times_df.withColumnRenamed("raceid", "race_id")
    .withColumnRenamed("driverId", "driver_id")
    .withColumn("ingestion_date", current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Write output to parquet file

# COMMAND ----------

lap_times_renamed_df.write.mode("overwrite").parquet("/mnt/formula1dlspalex/processed/lap_times")
