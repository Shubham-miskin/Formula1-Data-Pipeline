-- Databricks notebook source
create table if not exists work1_cata.f1presentation.calculate_race_result
using delta
as
select races.race_year,
     constructor.name AS team_name,
     drivers.driver_id,
     drivers.name AS driver_name,
     races.race_id,
     results.position,
     results.points,
     11 - results.position AS calculated_points
from work1_cata.f1processed.results 
join work1_cata.f1processed.drivers on (results.driver_id = drivers.driver_id)
join work1_cata.f1processed.constructor on (results.constructor_id = constructor.constructor_id)
join work1_cata.f1processed.races on (results.race_id = races.race_id)where results.position <= 10

-- COMMAND ----------

select * from work1_cata.f1presentation.calculate_race_result