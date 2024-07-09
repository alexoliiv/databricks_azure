# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### Ingest qualifying.json files

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC #### Libraries and configuration

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
# MAGIC #### Step 2 - Define data structure and read .json folder

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

qualifying_df = (
    spark.read.schema(qualifying_schema)
    .option("multiLine", True)
    .json(f"{bronze_folder_path}/qualifying")
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Renaming and adding two columns

# COMMAND ----------

qualifying_renamed_df = (
    add_ingestion_date(qualifying_df)
    .withColumnRenamed("raceid", "race_id")
    .withColumnRenamed("driverId", "driver_id")
    .withColumn("source", lit(v_data_source))
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Write output to parquet file

# COMMAND ----------

qualifying_renamed_df.write.mode("overwrite").parquet(f"{silver_folder_path}/qualifying")
##qualifying_renamed_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

