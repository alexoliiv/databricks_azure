# Databricks notebook source
# MAGIC
# MAGIC %md
# MAGIC #### Libraries and configuration

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import current_timestamp,concat,lit

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step 1 - Define schema and reading .json

# COMMAND ----------

name_schema = StructType(
    fields=[
        StructField("forename", StringType(), True),
        StructField("surname", StringType(), True),
    ]
)

# COMMAND ----------

drivers_schema = StructType(
    fields=[
        StructField("driverId", IntegerType(), False),
        StructField("driverRef", StringType(), True),
        StructField("number", IntegerType(), True),
        StructField("code", StringType(), True),
        StructField("name", name_schema, True),
        StructField("dob", DateType(), True),
        StructField("nationality", StringType(), True),
        StructField("url", StringType(), True),
    ]
)

# COMMAND ----------

drivers_df = spark.read \
.schema(drivers_schema) \
    .json(f"{bronze_folder_path}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Renaming columns and adding two columns

# COMMAND ----------

drivers_renamed_df = (
    add_ingestion_date(drivers_df)
    .withColumnRenamed("driverId", "driver_Id")
    .withColumnRenamed("driverRef", "driver_Ref")
    .withColumn(
        "name",
        concat(drivers_df["name.forename"], lit(" "), drivers_df["name.surname"]),
    )
    .withColumn("source",lit(v_data_source))
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Drop unwanted columns from the dataframe

# COMMAND ----------

drivers_final_df = drivers_renamed_df.drop(drivers_renamed_df['url'])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write output to parquet file

# COMMAND ----------

drivers_final_df.write.mode("overwrite").parquet(f"{silver_folder_path}/drivers")
##drivers_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")
