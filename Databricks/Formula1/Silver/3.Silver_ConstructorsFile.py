# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../ConfigAndCommonFunctions/Configuration"

# COMMAND ----------

# MAGIC %run "../ConfigAndCommonFunctions/CommonFunction"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

constructors_schema = StructType(fields=[StructField("constructorId", IntegerType(), False),
                                  StructField("constructorRef", StringType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("nationality", StringType(), True),
                                  StructField("url", StringType(), True)])
#constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"                                

# COMMAND ----------

constructor_df = spark.read \
.schema(constructors_schema) \
.json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

from pyspark.sql.functions import col, lit
constructor_dropped_df = constructor_df.drop(col('url'))

# COMMAND ----------

constructor_final_df = add_ingestion_date(constructor_dropped_df)\
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("constructorRef", "constructor_ref")\
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date))
#display(constructor_final_df)

# COMMAND ----------

constructor_final_df.write\
    .mode("overwrite")\
    .format("delta")\
    .saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.constructors;