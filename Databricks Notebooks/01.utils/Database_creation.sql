-- Databricks notebook source
drop database if exists f1processed cascade;


-- COMMAND ----------

drop database if exists f1presentation cascade;

-- COMMAND ----------

drop database if exists f1raw cascade;

-- COMMAND ----------

create database if not exists work1_cata.f1raw

-- COMMAND ----------

create database if not exists work1_cata.f1processed
managed location 'abfss://silver@datalakework1.dfs.core.windows.net/'

-- COMMAND ----------

create database if not exists work1_cata.f1presentation
managed location "abfss://gold@datalakework1.dfs.core.windows.net/"