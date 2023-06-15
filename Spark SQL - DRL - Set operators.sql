-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Set Operators
-- MAGIC - We use set operators when we want to merge two tables with similar schema
-- MAGIC
-- MAGIC ###### UNION
-- MAGIC   - UNION: merge two tables and removes duplicates if any
-- MAGIC ###### UNION ALL
-- MAGIC   - UNION: merge two tables and keeps the duplicates if any
-- MAGIC ###### INTERSECT
-- MAGIC   - Only the common records from both table will be displayed
-- MAGIC ###### MINUS or EXCEPT
-- MAGIC   - Records from one table not in other table
-- MAGIC   
-- MAGIC - While calling the SELECT class with set operators both select classes should have the same columns

-- COMMAND ----------

-- This will not work

--select eid from emp
--union
--select eid, name from emp1

-- COMMAND ----------

-- MAGIC %fs rm -r /user/hive/warehouse/emp/

-- COMMAND ----------

drop table if exists emp;

create table if not exists emp (eid int, name string);
insert into emp
select 1, "Raj" union all
select 1, "Raj" union all
select 2, "Kiran" union all
select 2, "Kiran" union all
select 3, "Manoj" union all
select 4, "Sridhar";

-- COMMAND ----------

select * from emp;

-- COMMAND ----------

-- MAGIC %fs rm -r /user/hive/warehouse/emp1/

-- COMMAND ----------

drop table if exists emp1;

create table if not exists emp1 (eid int, name string);
insert into emp1
select 4, "Sridhar" union all
select 5, "Krishna" union all
select 6, "Sudha" union all
select 6, "Sudha" union all
select 7, "Anil" union all
select 8, "Singh";

-- COMMAND ----------

select * from emp1;

-- COMMAND ----------

select * from emp
union
select * from emp1

-- COMMAND ----------

select * from emp
union all
select * from emp1

-- COMMAND ----------

select * from emp
intersect
select * from emp1

-- COMMAND ----------

select * from emp
intersect all
select * from emp1

-- COMMAND ----------

select * from emp
minus
select * from emp1

-- COMMAND ----------

select * from emp
except
select * from emp1