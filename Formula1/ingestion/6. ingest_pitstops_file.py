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
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# MAGIC %run "../includes/configuration"   

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

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
    .json(f"{bronze_folder_path}/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Renaming columns

# COMMAND ----------

pitstops_renamed_df = (
    add_ingestion_date(pitstops_df)
    .withColumn("source",lit(v_data_source))
    .withColumnRenamed("raceid", "race_id")
    .withColumnRenamed("driverId", "driver_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Write output to parquet file

# COMMAND ----------

pitstops_renamed_df.write.mode("overwrite").partitionBy("race_id").parquet(f"{silver_folder_path}/pit_stops")
##pitstops_renamed_df.write.mode("overwrite").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

