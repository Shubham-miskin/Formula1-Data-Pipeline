# Databricks notebook source
# MAGIC %md
# MAGIC #### Runing the confing , commom_function files and Creating parameters

# COMMAND ----------

# MAGIC %run "../includes/config"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("file_date", "2021-03-21")
data = dbutils.widgets.get("file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading data as required

# COMMAND ----------

driver_df = spark.sql(
    'select * from work1_cata.f1processed.drivers')\
        .withColumnRenamed("number", "driver_number") \
            .withColumnRenamed("name", "driver_name") \
                .withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

constructor_df = spark.sql(
    'select * from work1_cata.f1processed.constructor')\
        .withColumnRenamed("name", "team")

# COMMAND ----------

cricuit_df = spark.sql(
    'select * from work1_cata.f1processed.cricuit')\
        .withColumnRenamed("location", "circuit_location")

# COMMAND ----------

races_df = spark.sql('select * from work1_cata.f1processed.races')\
    .withColumnRenamed("name", "race_name") \
        .withColumnRenamed("race_timestamp", "race_date")\
            .withColumnRenamed("race_Id","races_race_Id")

# COMMAND ----------

results_df = spark.sql(f'select * from work1_cata.f1processed.results where file_date = "{data}"')\
        .withColumnRenamed("time", "race_time")\
            .withColumnRenamed("race_Id","race_Id")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Method 1 - Using Pure DataFrame API

# COMMAND ----------

final_df = (
    races_df
    .join(cricuit_df, races_df.circuit_Id == cricuit_df.circuit_Id)
    .join(results_df, races_df.races_race_Id == results_df.race_Id)
    .join(constructor_df, results_df.constructor_Id == constructor_df.constructor_Id)
    .join(driver_df, results_df.driver_Id == driver_df.driver_Id)
    ).select(
        results_df.race_Id,
        races_df.Race_year,
        races_df.race_name,
        races_df.race_date,
        cricuit_df.circuit_location,
        driver_df.driver_name,
        driver_df.driver_number,
        driver_df.driver_nationality,
        constructor_df.team,
        results_df.grid,
        results_df.fastest_Lap_Time,
        results_df.race_time,
        results_df.position,
        results_df.points
    ).withColumn("ingestion_date", current_timestamp())\
        .withColumn("file_date", lit(data))

# COMMAND ----------

# Calling the function rearrange_columns from include/commom_function notebook
df = rearrange_columns(final_df,"race_Id")

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

if spark.catalog.tableExists("work1_cata.f1presentation.race_result"):
    df.write.mode("overwrite").insertInto("work1_cata.f1presentation.race_result")
else:
    df.write.mode("overwrite").partitionBy("race_Id").format("delta").saveAsTable("work1_cata.f1presentation.race_result")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Method 2 - Using spark.sql

# COMMAND ----------

# races_df.createOrReplaceTempView("races_df")
# results_df.createOrReplaceTempView("results_df")
# constructor_df.createOrReplaceTempView("constructor_df")
# driver_df.createOrReplaceTempView("driver_df")
# cricuit_df.createOrReplaceTempView("cricuit_df")

# df = spark.sql("""
#                 select dr.driver_name, dr.driver_number, re.grid, re.fastest_Lap_Time, re.race_time, re.position, re.points
#                 from races_df r join cricuit_df ci
#                 on r.circuit_Id = ci.circuit_Id
#                 join results_df re
#                 on r.race_Id = re.race_Id
#                 join constructor_df co
#                 on re.constructor_id = co.constructor_id
#                 join driver_df dr 
#                 on re.driver_id = dr.driver_id
#                 where r.race_year = 2020 and r.race_name = 'Abu Dhabi Grand Prix'
#                 order by points desc, position asc""")  