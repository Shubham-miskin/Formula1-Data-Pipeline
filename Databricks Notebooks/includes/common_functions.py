# Databricks notebook source
def add_ingestion_date(df):
    output_df = df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

def rearrange_columns(input_df, partation_name):
    cols = []
    for col_name in input_df.schema.names:
        if col_name != partation_name:
            cols.append(col_name)
    cols.append(partation_name)
    output_df = input_df.select(cols)
    return output_df

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

from pyspark.sql.window import Window