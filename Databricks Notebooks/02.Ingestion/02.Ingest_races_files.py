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
# MAGIC #### Step 2 - Reading the Races file 

# COMMAND ----------

race_data = spark.read.csv(f"{bronze_folder_path}/{data}/races.csv",header=True,inferSchema=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Basic Transformations

# COMMAND ----------

# selecting and renameing the columns
selected_data = race_data.select(col("raceId").alias("race_Id"),col("year").alias("Race_year"),col("round"),col("circuitId").alias("circuit_Id"),col("name"),col("date"),col("time"))

# COMMAND ----------

# adding new Columns
add_columns = (
    selected_data
    .withColumn(
        "race_timestamp",
        to_timestamp(
            concat(col("date"), lit(" "), col("time")),
            "yyyy-MM-dd HH:mm:ss"
        )
    )
)
add_columns = add_ingestion_date(add_columns)

# COMMAND ----------

# TypeCasting the data
final_data = add_columns.select(
    col("race_Id").cast("int"),
    col("Race_year").cast("int"),
    col("round").cast("int"),
    col("circuit_Id").cast("int"),
    col("name").cast("string"),
    col("race_timestamp").cast("timestamp"),
    col("Ingestion_date").cast("timestamp")
).withColumn("file_date", lit(data))



# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write Ingested Data to Data Lake

# COMMAND ----------

# If we need to make the diffrent partation of the data then we can use the below code
final_data.write.mode("overwrite").partitionBy("Race_year").format("delta").saveAsTable("work1_cata.f1processed.races")

# COMMAND ----------

dbutils.notebook.exit("Success")