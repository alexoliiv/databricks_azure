# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### Libraries and configuration

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
)
from pyspark.sql.functions import col, current_timestamp, lit, to_timestamp, concat

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("v_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("v_file_date", "2024-07-09")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 1 - Define schema and read csv file

# COMMAND ----------

circuits_schema = StructType(
    fields=[
        StructField("circuitId", IntegerType(), False),
        StructField("circuitRef", StringType(), True),
        StructField("name", StringType(), True),
        StructField("location", StringType(), True),
        StructField("country", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lng", DoubleType(), True),
        StructField("alt", IntegerType(), True),
        StructField("url", StringType(), True),
    ]
)

# COMMAND ----------

circuits_df = (
    spark.read.option("header", True)
    .schema(circuits_schema)
    .csv(f"{bronze_folder_path}/{v_file_date}/circuits.csv")
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 2 - Select only the required columns

# COMMAND ----------

circuits_selected_df = circuits_df.select(
    col("circuitId"),
    col("circuitRef"),
    col("name"),
    col("location"),
    col("country").alias("country_race"),
    col("lat"),
    col("lng"),
    col("alt"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 3 - Renaming and adding two columns

# COMMAND ----------

circuits_renamed_df = (
    circuits_selected_df.withColumnRenamed("circuitId", "circuit_id")
    .withColumnRenamed("circuitRef", "circuit_ref")
    .withColumnRenamed("lat", "latitude")
    .withColumnRenamed("lng", "longitude")
    .withColumnRenamed("alt", "altitude")
    .withColumn("source",lit(v_data_source))
    .withColumn("file_date",lit(v_file_date))
)

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 4 - Write data to datalake as parquet

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet(f"{silver_folder_path}/circuits")

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_silver.circuits")

# COMMAND ----------

