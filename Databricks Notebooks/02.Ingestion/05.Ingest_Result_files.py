# Databricks notebook source
# MAGIC %md
# MAGIC #### Step1 - Accessing the ADLS using the access key

# COMMAND ----------

# MAGIC %run "../includes/config"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

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
# MAGIC #### Step 2 - Reading the Result file 

# COMMAND ----------

# Defining the schema
result_schema = """
resultId INT, raceId INT, driverId INT, constructorId INT, number INT, grid INT, position INT, positionText STRING, positionOrder INT, points DOUBLE, laps INT, time STRING, milliseconds INT, fastestLap INT, rank INT, fastestLapTime STRING, fastestLapSpeed DOUBLE, statusId STRING
"""

# COMMAND ----------

result_data = spark.read.json(
    f"{bronze_folder_path}/{data}/results.json",
    schema=result_schema
)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Data Transformations

# COMMAND ----------

# Rename the heading 
result_data_renamed = result_data.withColumnRenamed("resultId", "result_Id") \
    .withColumnRenamed("raceId", "race_Id") \
    .withColumnRenamed("driverId", "driver_Id") \
    .withColumnRenamed("constructorId", "constructor_Id") \
    .withColumnRenamed("positionText", "position_Text") \
    .withColumnRenamed("positionOrder", "position_Order") \
    .withColumnRenamed("fastestLap", "fastest_Lap") \
    .withColumnRenamed("fastestLapTime", "fastest_Lap_Time") \
    .withColumnRenamed("fastestLapSpeed", "fastest_Lap_Speed")

# COMMAND ----------

# Droping the columns
result_data_droped = result_data_renamed.drop("statusId")


# COMMAND ----------

# Adding new coulmns
result_data_final = add_ingestion_date(result_data_droped)\
                    .withColumn("file_date", lit(data))

# COMMAND ----------

# Calling the function rearrange_columns from include/commom_function notebook
df = rearrange_columns(result_data_final,"race_Id")
    

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write Ingested Data to Data Lake

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

if spark.catalog.tableExists("work1_cata.f1processed.results"):
    df.write.mode("overwrite").insertInto("work1_cata.f1processed.results")
else:
    df.write.mode("overwrite").partitionBy("race_Id").format("delta").saveAsTable("work1_cata.f1processed.results")


# COMMAND ----------

# MAGIC %sql
# MAGIC select race_Id,count(1) 
# MAGIC from work1_cata.f1processed.results
# MAGIC group by race_Id
# MAGIC order by race_Id desc;

# COMMAND ----------

dbutils.notebook.exit("Success")