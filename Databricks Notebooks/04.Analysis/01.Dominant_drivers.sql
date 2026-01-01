-- Databricks notebook source
select driver_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
  from  work1_cata.f1presentation.calculate_race_result
group by driver_name
having COUNT(1) >= 50
order by avg_points DESC


-- COMMAND ----------

select driver_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
  from  work1_cata.f1presentation.calculate_race_result
 where race_year between 2001 AND 2010
group by driver_name
having COUNT(1) >= 50
order by avg_points DESC

-- COMMAND ----------

select driver_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
  from work1_cata.f1presentation.calculate_race_result
 where race_year between 2011 AND 2020
group by driver_name
having COUNT(1) >= 50
order by avg_points DESC