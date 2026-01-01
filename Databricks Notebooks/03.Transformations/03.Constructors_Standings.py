# Databricks notebook source
# MAGIC %run "../includes/config"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("file_date", "2021-03-21")
data = dbutils.widgets.get("file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Find race year for which the data is to be proccessed

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

Constructors_standings = df.groupBy("Race_year", "team") \
    .agg(
        sum("points").alias("total_points"),
        sum(when(col("position") == 1, 1).otherwise(0)).alias("wins")
    )


# COMMAND ----------

Constructors_rank = Window.partitionBy("Race_year").orderBy(desc("total_points"), desc("wins"))
final_df = Constructors_standings.withColumn("rank",rank().over(Constructors_rank))

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

if spark.catalog.tableExists("work1_cata.f1presentation.Constructors_standings"):
    final_df.write.mode("overwrite").insertInto("work1_cata.f1presentation.Constructors_standings")
else:
    final_df.write.mode("overwrite").partitionBy("Race_year").format("delta").saveAsTable("work1_cata.f1presentation.Constructors_standings")