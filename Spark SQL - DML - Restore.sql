-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Restore
-- MAGIC - Restore a delta table to an earlier version
-- MAGIC - Restoration based on version or timestamp is supported

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Cloud based data warehouses (Public Data warehouse)
-- MAGIC - Azure SQL DWH/Dedicated SQL Pool/Synapse DWH
-- MAGIC - AWS Redshift
-- MAGIC - Google Big Query
-- MAGIC - Snowflake (DWH as a service)

-- COMMAND ----------

drop table if exists all_employees;

create table if not exists all_employees (
  name string, 
  empid int,
  locid int
  ) using delta;

insert into all_employees 
select 'Ravi',1,1 union all
select 'Raj',2,1 union all
select 'Kiran',3,3 union all
select 'Sudha',4,4 union all
select 'Manoj',5,5 union all
select 'Teja', 6, 2 union all
select 'Ram', 7, 1 union all
select 'Surya', 8, 6 union all
select 'Krishna', 9, 7;

-- COMMAND ----------

describe history all_employees

-- COMMAND ----------

select * from all_employees

-- COMMAND ----------

update all_employees
set locid = 31
where empid=3

-- COMMAND ----------

select * from all_employees

-- COMMAND ----------

describe history all_employees

-- COMMAND ----------

delete from all_employees
where empid=3

-- COMMAND ----------

describe history all_employees

-- COMMAND ----------

select * from all_employees timestamp as of "2023-05-30T11:12:45.000+0000"

-- COMMAND ----------

select * from all_employees

-- COMMAND ----------

restore table all_employees timestamp as of "2023-05-30T11:15:27.000+0000"

-- COMMAND ----------

select * from all_employees