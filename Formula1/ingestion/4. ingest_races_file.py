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
    DateType,
)
from pyspark.sql.functions import col, current_timestamp, lit, to_timestamp, concat

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 1 - Defining schema and reading csv

# COMMAND ----------

races_schema = StructType(
    fields=[
        StructField("raceId", IntegerType(), False),
        StructField("year", IntegerType(), True),
        StructField("round", IntegerType(), True),
        StructField("circuitId", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("date", DateType(), True),
        StructField("time", StringType(), True),
        StructField("url", StringType(), True),
    ]
)

# COMMAND ----------

races_df = (
    spark.read.option("header", True)
    .schema(races_schema)
    .csv(f"{bronze_folder_path}/races.csv")
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 2 - Select only the required columns

# COMMAND ----------

races_selected_df = races_df.select(
    races_df["raceId"].alias("race_id"),
    races_df["year"].alias("race_year"),
    races_df["round"],
    races_df["circuitId"].alias("circuit_id"),
    races_df["name"],
    races_df["date"],
    races_df["time"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 3 - Add columns as required

# COMMAND ----------

races_final_df = (
    add_ingestion_date(races_selected_df)
    .withColumn(
        "race_timestamp",
        to_timestamp(concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss"),
    )
    .withColumn("source", lit(v_data_source))
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 4 - Write data to datalake as parquet

# COMMAND ----------

races_final_df.write.mode("overwrite").partitionBy('race_year').parquet(f"{silver_folder_path}/races")
##races_final_df.write.mode("overwrite").partitionBy("race_year").format("parquet").saveAsTable("f1_processed.races")