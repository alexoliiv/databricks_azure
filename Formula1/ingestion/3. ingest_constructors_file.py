# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### Ingest constructors.json file

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC 1. Read the JSON file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructor_scheme = "constructorId INTEGER, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read \
.schema(constructor_scheme) \
    .json("/mnt/formula1dlspalex/raw/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Drop unwanted columns from the dataframe

# COMMAND ----------

constructor_dropped_df = constructor_df.drop(constructor_df['url'])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename columns and add ingestion date

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId","constructor_Id") \
    .withColumnRenamed("constructorRef","constructor_Ref") \
        .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write output to parquet file

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet("/mnt/formula1dlspalex/processed/constructors")

# COMMAND ----------


