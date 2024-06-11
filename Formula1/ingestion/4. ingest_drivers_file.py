# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### Ingest drivers.json file

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC #### 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import current_timestamp,concat,lit

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step 2 - Define dataframe structure

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
    .json("/mnt/formula1dlspalex/raw/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Renaming columns

# COMMAND ----------

drivers_renamed_df = drivers_df.withColumnRenamed("driverId","driver_Id") \
    .withColumnRenamed("driverRef","driver_Ref") \
        .withColumn("ingestion_date",current_timestamp()) \
            .withColumn("name", concat(drivers_df['name.forename'],lit(' '),drivers_df['name.surname']))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Drop unwanted columns from the dataframe

# COMMAND ----------

drivers_final_df = drivers_renamed_df.drop(drivers_renamed_df['url'])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Write output to parquet file

# COMMAND ----------

drivers_final_df.write.mode("overwrite").parquet("/mnt/formula1dlspalex/processed/drivers")
