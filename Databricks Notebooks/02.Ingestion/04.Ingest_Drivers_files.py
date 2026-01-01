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
# MAGIC #### Step 2 - Reading the Drivers file 

# COMMAND ----------

# Defining the Schemas
name_schema = StructType(fields = [
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True)])

driver_schema = StructType(fields = [
    StructField("driverId", IntegerType(), False),
    StructField("driverRef", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name", name_schema),
    StructField("dob", StringType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

drivers_data = spark.read.json(f"{bronze_folder_path}/{data}/drivers.json", schema = driver_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Transformations

# COMMAND ----------

# Renameing the columns and Adding Columns
drivers_data_modified = drivers_data.withColumnRenamed("driverId", "driver_Id") \
                                  .withColumnRenamed("driverRef", "driver_Ref")\
                                  .withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname")))\
                                  .withColumn("file_date",lit(data))

drivers_data_modified = add_ingestion_date(drivers_data_modified)

# COMMAND ----------

# Droping the Specified columns
drivers_data_final = drivers_data_modified.drop("url")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write Ingested Data to Data Lake

# COMMAND ----------

drivers_data_final.write.mode("overwrite").format("delta").saveAsTable("work1_cata.f1processed.drivers")

# COMMAND ----------

dbutils.notebook.exit("Success")