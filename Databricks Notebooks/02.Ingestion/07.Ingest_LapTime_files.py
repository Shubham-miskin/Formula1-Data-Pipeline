# Databricks notebook source
# MAGIC %md
# MAGIC #### Step1 - Accessing the ADLS using the access key

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/config"

# COMMAND ----------

# Asseccting the data lake using the access key
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    f"{storage_account_access_key}"
)

# Getting the list of data in the data lake
files = dbutils.fs.ls(f"{bronze_folder_path}/")

# COMMAND ----------

dbutils.widgets.text("file_date", "2021-03-21")
data = dbutils.widgets.get("file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Reading the Lap time file 

# COMMAND ----------

laptime_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

laptime_data = spark.read.csv(f"{bronze_folder_path}/{data}/lap_times/*.csv", schema = laptime_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Data Transformations

# COMMAND ----------

laptime_data_final = laptime_data.withColumnRenamed("raceId", "race_Id") \
                                 .withColumnRenamed("driverId", "driver_id")


# COMMAND ----------

laptime_data_final = add_ingestion_date(laptime_data_final)\
                     .withColumn("file_date", lit(data))

# COMMAND ----------

df = rearrange_columns(laptime_data_final,"race_Id")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write Ingested Data to Data Lake

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

if spark.catalog.tableExists("work1_cata.f1processed.lap_times"):
    df.write.mode("overwrite").insertInto("work1_cata.f1processed.lap_times")
else:
    df.write.mode("overwrite").partitionBy("race_Id").format("delta").saveAsTable("work1_cata.f1processed.lap_times")

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_Id,count(1) 
# MAGIC from work1_cata.f1processed.lap_times
# MAGIC group by race_Id
# MAGIC order by race_Id desc;

# COMMAND ----------

dbutils.notebook.exit("Success")