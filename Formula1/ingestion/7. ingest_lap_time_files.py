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
    .csv(f"{bronze_folder_path}/lap_times/")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Renaming columns

# COMMAND ----------

lap_times_renamed_df = (
    add_ingestion_date(lap_times_df)
    .withColumn("source",lit(v_data_source))
    .withColumnRenamed("raceid", "race_id")
    .withColumnRenamed("driverId", "driver_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Write output to parquet file

# COMMAND ----------

lap_times_renamed_df.write.mode("overwrite").parquet(f"{silver_folder_path}/lap_times")
##lap_times_renamed_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")