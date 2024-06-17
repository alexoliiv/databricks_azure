# Databricks notebook source
# MAGIC
# MAGIC %md 
# MAGIC
# MAGIC #### Libraries and configuration

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

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
# MAGIC #### Step 1 - Definig scheme and reading .json

# COMMAND ----------

constructor_scheme = "constructorId INTEGER, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read \
.schema(constructor_scheme) \
    .json(f"{bronze_folder_path}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Drop unwanted columns from the dataframe

# COMMAND ----------

constructor_dropped_df = constructor_df.drop(constructor_df['url'])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename columns and adding two columns

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId","constructor_Id") \
    .withColumnRenamed("constructorRef","constructor_Ref") \
        .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

constructor_final_df = (
    add_ingestion_date(constructor_final_df)
    .withColumn("source", lit(v_data_source))
    .withColumnRenamed("constructorId", "constructor_Id")
    .withColumnRenamed("constructorRef", "constructor_Ref")
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write output to parquet file

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet(f"{silver_folder_path}/constructors")
##constructor_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructor")

# COMMAND ----------

