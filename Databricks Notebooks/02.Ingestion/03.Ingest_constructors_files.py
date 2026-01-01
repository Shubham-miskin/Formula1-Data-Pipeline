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
# MAGIC #### Step 2 - Reading the Constructors file 

# COMMAND ----------

# Defining the Schemas by DDL method
constructor_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_data = spark.read.json(f"{bronze_folder_path}/{data}/constructors.json", schema = constructor_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Transfromations on Data

# COMMAND ----------

#Droping a specified columns
constructor_drop = constructor_data.drop("url")

# COMMAND ----------

# Reaname colums and add Ingestion Date
constructor_final = constructor_drop.withColumnRenamed("constructorId","constructor_Id")\
                                    .withColumnRenamed("constructorRef","constructor_Ref")\
                                    .withColumn("file_date",lit(data))

constructor_final = add_ingestion_date(constructor_final)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write Ingested Data to Data Lake

# COMMAND ----------

constructor_final.write.mode("overwrite").format("delta").saveAsTable("work1_cata.f1processed.constructor")

# COMMAND ----------

dbutils.notebook.exit("Success")