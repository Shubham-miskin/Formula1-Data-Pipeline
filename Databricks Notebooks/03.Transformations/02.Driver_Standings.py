# Databricks notebook source
# MAGIC %md
# MAGIC #### Driver Standings

# COMMAND ----------

# MAGIC %run "../includes/config"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("file_date", "2021-03-21")
data = dbutils.widgets.get("file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Find the Race_year for which data is to be processed

# COMMAND ----------

race_result_list = spark.sql('select * from work1_cata.f1presentation.race_result')\
    .filter(f"file_date = '{data}'")\
        .select("Race_year")\
            .distinct()\
                .collect()

List = []
for race_years in race_result_list:
    List.append(race_years.Race_year)             

# COMMAND ----------

df = spark.sql('select * from work1_cata.f1presentation.race_result')\
    .filter(col('Race_year').isin(List))

# COMMAND ----------

Driver_standing = df\
    .groupby("Race_year","driver_name", "driver_nationality", "team")\
        .agg(sum("points").alias("total_points"), count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

driver_rank = Window.partitionBy("Race_year").orderBy(desc("total_points"), desc("wins"))


# COMMAND ----------

final_df = Driver_standing.withColumn("rank", rank().over(driver_rank))

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

if spark.catalog.tableExists("work1_cata.f1presentation.Driver_standing"):
    final_df.write.mode("overwrite").insertInto("work1_cata.f1presentation.Driver_standing")
else:
    final_df.write.mode("overwrite").partitionBy("Race_year").format("delta").saveAsTable("work1_cata.f1presentation.Driver_standing")