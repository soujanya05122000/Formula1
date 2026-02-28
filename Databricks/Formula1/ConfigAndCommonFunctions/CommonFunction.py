# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
  output_df = input_df.withColumn("ingestion_date", current_timestamp())
  return output_df

# COMMAND ----------

def df_column_to_list(input_df, column_name):
  df_row_list = input_df.select(column_name) \
                        .distinct() \
                        .collect()
  
  column_value_list = [row[column_name] for row in df_row_list]
  return column_value_list

# COMMAND ----------

from delta.tables import DeltaTable

def merge_delta_data(input_df, catalog, schema, table_name, merge_condition, partition_column=None):
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")

    full_table_name = f"{catalog}.{schema}.{table_name}"

    if spark.catalog.tableExists(full_table_name):
        delta_table = DeltaTable.forName(spark, full_table_name)
        (delta_table.alias("tgt")
            .merge(input_df.alias("src"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        writer = input_df.write.format("delta").mode("overwrite")
        if partition_column:
            writer = writer.partitionBy(partition_column)
        writer.saveAsTable(full_table_name)