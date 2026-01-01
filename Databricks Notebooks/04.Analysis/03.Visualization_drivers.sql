-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Dominant Formula 1 Drivers of All Time</h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_drivers
AS
select driver_name,
       count(*) as total_races,
       sum(calculated_points) as total_points,
       avg(calculated_points) as avg_points,
       rank() over(order by avg(calculated_points) desc) as driver_rank
from work1_cata.f1presentation.calculate_race_result
group by driver_name
having count(1) >= 50
order by avg_points desc;


-- COMMAND ----------

select race_year, 
       driver_name,
       count(1) as total_races,
       sum(calculated_points) as total_points,
       avg(calculated_points) as avg_points
  from  work1_cata.f1presentation.calculate_race_result
 where driver_name in (select driver_name from v_dominant_drivers where driver_rank <= 10)
group by race_year, driver_name
order by race_year, avg_points desc;
