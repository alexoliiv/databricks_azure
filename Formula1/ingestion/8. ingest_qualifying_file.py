# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### Ingest qualifying.json files

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

qualifying_schema = StructType(
    fields=[
        StructField("qualifyId", IntegerType(), False),
        StructField("raceId", IntegerType(), True),
        StructField("driverId", IntegerType(), True),
        StructField("constructorId", IntegerType(), True),
        StructField("number", IntegerType(), True),
        StructField("position", IntegerType(), True),
        StructField("q1", StringType(), True),
        StructField("q2", StringType(), True),
        StructField("q3", StringType(), True),
    ]
)

# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
    .json("/mnt/formula1dlspalex/raw/qualifying")

# COMMAND ----------

display(qualifying_df['q3'].contains('/N'))

# COMMAND ----------



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
