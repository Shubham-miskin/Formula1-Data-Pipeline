# Databricks notebook source
result = dbutils.notebook.run("01.Ingest_circuits_files", 0, {"file_date":"2021-04-18"})
result

# COMMAND ----------

result = dbutils.notebook.run("02.Ingest_races_files", 0, {"file_date":"2021-04-18"})
result

# COMMAND ----------

result = dbutils.notebook.run("03.Ingest_constructors_files", 0, {"file_date":"2021-04-18"})
result

# COMMAND ----------

result = dbutils.notebook.run("04.Ingest_Drivers_files", 0, {"file_date":"2021-04-18"})
result

# COMMAND ----------

result = dbutils.notebook.run("05.Ingest_Result_files", 0, {"file_date":"2021-04-18"})
result

# COMMAND ----------

result = dbutils.notebook.run("06.Ingest_Pitstop_files", 0, {"file_date":"2021-04-18"})
result

# COMMAND ----------

result = dbutils.notebook.run("07.Ingest_LapTime_files", 0, {"file_date":"2021-04-18"})
result

# COMMAND ----------

result = dbutils.notebook.run("08.Ingest_Qualifying_files", 0, {"file_date":"2021-04-18"})
result