# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col,current_timestamp,lit,to_timestamp,concat

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId" ,IntegerType(), False),
                                     StructField("circuitRef",StringType(), True ),
                                     StructField("name"      ,StringType(), True ),
                                     StructField("location"  ,StringType(), True ),
                                     StructField("country"   ,StringType(), True ),
                                     StructField("lat"       ,DoubleType(), True ),
                                     StructField("lng"       ,DoubleType(), True ),
                                     StructField("alt"       ,IntegerType(), True ),
                                     StructField("url"       ,StringType(), True )
])

# COMMAND ----------

circuits_df = spark.read \
    .option("header",True) \
        .schema(circuits_schema) \
            .csv("dbfs:/mnt/formula1dlspalex/raw/circuits.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 2 - Select only the required columns

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country").alias("country_race"),col("lat"),col("lng"),col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 3 - Rename the columns as required

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId","circuit_id") \
    .withColumnRenamed("circuitRef","circuit_ref") \
        .withColumnRenamed("lat","latitude") \
            .withColumnRenamed("lng","longitude") \
                .withColumnRenamed("alt","altitude")

# COMMAND ----------

circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 5 - Write data to datalake as parquet

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet("/mnt/formula1dlspalex/processed/circuits")

# COMMAND ----------


