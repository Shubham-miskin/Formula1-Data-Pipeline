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
# MAGIC #### Step 2 - Reading the Qualifying file 

# COMMAND ----------

Qualifying_schema = StructType(fields = [
    StructField("qualifyId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("q1", StringType(), True),
    StructField("q2", StringType(), True),
    StructField("q3", StringType(), True) ])

# COMMAND ----------

Qualifying_data = spark.read.option("multiline", True).schema(Qualifying_schema).json(f"{bronze_folder_path}/{data}/qualifying/*.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Data Transformations

# COMMAND ----------

Qualifying_data_final = Qualifying_data.withColumnRenamed("qualifyId", "qualify_Id") \
    .withColumnRenamed("raceId", "race_Id") \
    .withColumnRenamed("driverId", "driver_Id") \
    .withColumnRenamed("constructorId", "constructor_Id")

# COMMAND ----------

Qualifying_data_final = add_ingestion_date(Qualifying_data_final)\
                        .withColumn("file_date", lit(data))

# COMMAND ----------

df = rearrange_columns(Qualifying_data_final,"race_Id")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write Ingested Data to Data Lake

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

if spark.catalog.tableExists("work1_cata.f1processed.Qualifying"):
    df.write.mode("overwrite").insertInto("work1_cata.f1processed.Qualifying")
else:
    df.write.mode("overwrite").partitionBy("race_Id").format("delta").saveAsTable("work1_cata.f1processed.Qualifying")

# COMMAND ----------

dbutils.notebook.exit("Success")