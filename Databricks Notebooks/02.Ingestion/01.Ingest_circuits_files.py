# Databricks notebook source
# MAGIC %md
# MAGIC #### Step1 - Accessing the ADLS using service principal

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

#creating a widgets
dbutils.widgets.text("file_date", "2021-03-21")
data = dbutils.widgets.get("file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Reading Data

# COMMAND ----------

cricuit_data = spark.read.csv(f"{bronze_folder_path}/{data}/circuits.csv", header = True, inferSchema = True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Basic Transformations

# COMMAND ----------

#Select Required columns
cricuit_selected = cricuit_data.select("circuitId","circuitRef","name","location","country","lat","lng","alt")

# COMMAND ----------

#Rename Columns As Per Required
cricuit_data_renamed = cricuit_selected.withColumnRenamed("circuitId","circuit_Id") \
                                       .withColumnRenamed("circuitRef","circuit_Ref") \
                                       .withColumnRenamed("lat","latitude") \
                                       .withColumnRenamed("lng","longitude") \
                                       .withColumnRenamed("alt","altitude")

# COMMAND ----------

#Adding the Columns to Dataframe
cricuit_final = add_ingestion_date(cricuit_data_renamed) \
                .withColumn("file_date",lit(data))


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Write Ingested Data to Data Lake

# COMMAND ----------

cricuit_final.write.mode("overwrite").format("delta").saveAsTable("work1_cata.f1processed.cricuit")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

    spark.conf.get("spark.databricks.clusterUsageTags.clusterId")

# COMMAND ----------

