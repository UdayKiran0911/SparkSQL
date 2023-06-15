-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### View
-- MAGIC - View is a window of a table, view only refers to a query and does not store the data 
-- MAGIC - Two types of temporary views
-- MAGIC   - Global temporary view
-- MAGIC   - Temporary view
-- MAGIC
-- MAGIC - A regular views can be stored in a database, 
-- MAGIC a temporary/global temporary view can't be stored in database these are stored in global_temps
-- MAGIC - While temporary views can be accessed directly, global views cannot be.
-- MAGIC - we need to specify the global schema as __global_temp."global temp view name"__

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Difference between 'View, Temporary view and Global temporary view'
-- MAGIC ###### View
-- MAGIC - a normal view can be stored into a specific database
-- MAGIC - scope of a normal view is 'Spark Catalog'
-- MAGIC - that is it can be accessed by any user, any session, any cluster
-- MAGIC
-- MAGIC ###### Temporary View
-- MAGIC - a temporary view cannot be stored in a specific database
-- MAGIC - the scope of temporary view is limited to the user scope, others can't access temporary view
-- MAGIC - temporary views are stored in global_temp schema with istemporary=True
-- MAGIC - if you go to Run >> Clear State, and then try to access the temporary view, you would be able to do so
-- MAGIC
-- MAGIC ###### Global Temporary View
-- MAGIC - a global temporary view like temporary view cannot be stored in a specific database
-- MAGIC - the scope of global temporary view is spark session (cluster) scoped, anyone can access within the cluster
-- MAGIC - global temporary views are stored in global_temp schema with istemporary=False
-- MAGIC - global temporary view can be accessed even when the state is purged (erased), if we restart the cluster we wont be able to access the global temporary view
-- MAGIC

-- COMMAND ----------

describe database sparksql_db

-- COMMAND ----------

select 1, "Ram", 5 union all
select 2, "Krishna", 7 union all
select 3, "Raj", 10;

-- COMMAND ----------

drop table if exists uday_db.all_employees;
create table if not exists uday_db.all_employees (id int, name string, work_yrs int)
location "/user/hive/warehouse/uday_db";
insert into uday_db.all_employees 
select 1, "Ram", 5 union all
select 2, "Krishna", 7 union all
select 3, "Raj", 10 union all
select 4, "Prasad", 15 union all
select 5, "Mohan", 12 union all
select 6, "Manju", 4 union all
select 7, "Sindhu", 2 union all
select 8, "Anitha", 8 union all
select 9, "Sridhar", 9;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("/user/hive/warehouse/uday_db")

-- COMMAND ----------

select * from uday_db.all_employees

-- COMMAND ----------

create or replace view employees_lsid5 
as 
select * from uday_db.all_employees where id<5;

-- COMMAND ----------

describe employees_lsid5

-- COMMAND ----------

select * from employees_lsid5

-- COMMAND ----------

show create table employees_lsid5

-- COMMAND ----------

create or replace view uday_db.employees_gtid5 
as select * from uday_db.all_employees 
where id > 5

-- COMMAND ----------

-- create or replace temporary view uday_db.tv_emps 
-- as select * from uday_db.all_employees 
-- where id > 5 */

-- COMMAND ----------

create or replace temporary view tv_emps 
as select * from uday_db.all_employees 
where id > 5

-- COMMAND ----------

select * from tv_emps

-- COMMAND ----------

create or replace global temporary view gv_emps 
as select * from uday_db.all_employees 
where id < 5

-- COMMAND ----------

select * from gv_emps

-- COMMAND ----------

select * from global_temp.gv_emps

-- COMMAND ----------

show tables in global_temp