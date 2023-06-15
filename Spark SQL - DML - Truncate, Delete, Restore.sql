-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Truncate and Delete
-- MAGIC - Delta tables support DML operations such as Insert, Delete, Merge
-- MAGIC - CSV table do not support Delete Operation, and we may have to use truncate in that case.
-- MAGIC - in general truncate will delete complete table data, however we can specify constraint based deletion as well
-- MAGIC - Similar to constrain based deletion in delta tables, truncate supports partition wise deletion
-- MAGIC - for truncate with partition to work, the table must be created with a partitioned by column.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - Delete, Update and Merge are supported only by delta tables, other formats do not support these operations
-- MAGIC - truncate, insert are supported by other table formats as well

-- COMMAND ----------

drop table if exists students;

create table if not exists students (
  name string, 
  rollno int
  ) using csv
  partitioned by (age int)

-- COMMAND ----------

insert into students 
select 'Ravi',1,36 union all
select 'Raj',2,35 union all
select 'Kiran',3,30 union all
select 'Sudha',4,29 union all
select 'Manoj',5,28;

-- COMMAND ----------

SET spark.databricks.delta.preview.enabled=false

-- COMMAND ----------

drop table if exists students;

create table if not exists students (
  name string, 
  rollno int
  ) using csv
  partitioned by (age int)

-- COMMAND ----------

insert into students 
select 'Ravi',1,36 union all
select 'Raj',2,35 union all
select 'Kiran',3,35 union all
select 'Sudha',4,29 union all
select 'Manoj',5,28;

-- COMMAND ----------

delete from students

-- COMMAND ----------

truncate table students

-- COMMAND ----------

-- MAGIC %fs ls /user/hive/warehouse/students/age=28/

-- COMMAND ----------

select * from students

-- COMMAND ----------

truncate table students partition (age=35)

-- COMMAND ----------

select * from students

-- COMMAND ----------

-- MAGIC %fs ls /user/hive/warehouse/students/age=28/

-- COMMAND ----------

-- MAGIC %fs ls /user/hive/warehouse/students/age=35/

-- COMMAND ----------

drop table if exists students_2;

create table if not exists students_2 (
  name string, 
  rollno int,
  age int
  ) using csv

-- COMMAND ----------

insert into students_2 
select 'Ravi',1,36 union all
select 'Raj',2,35 union all
select 'Kiran',3,35 union all
select 'Sudha',4,29 union all
select 'Manoj',5,28;

-- COMMAND ----------

delete from students_2 where age=35

-- COMMAND ----------

truncate table students_2 partition (age=35)

-- COMMAND ----------

drop table if exists students_3;

create table if not exists students_3 (
  name string, 
  rollno int,
  age int
  ) using delta;

insert into students_3 
select 'Ravi',1,36 union all
select 'Raj',2,35 union all
select 'Kiran',3,35 union all
select 'Sudha',4,29 union all
select 'Manoj',5,28;

-- COMMAND ----------

SET spark.databricks.delta.preview.enabled=true

-- COMMAND ----------

drop table if exists students_3;

create table if not exists students_3 (
  name string, 
  rollno int,
  age int
  ) using delta;

insert into students_3 
select 'Ravi',1,36 union all
select 'Raj',2,35 union all
select 'Kiran',3,35 union all
select 'Sudha',4,29 union all
select 'Manoj',5,28;

-- COMMAND ----------

select * from students_3

-- COMMAND ----------

delete from students_3 where age=35

-- COMMAND ----------

select * from students_3

-- COMMAND ----------

describe history students_3

-- COMMAND ----------

select * from students_3 version as of 1

-- COMMAND ----------

restore table students_3 to version as of 1

-- COMMAND ----------

select * from students_3

-- COMMAND ----------

describe history students_3

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

select * from all_employees

-- COMMAND ----------

delete from all_employees where locid=1 
-- be careful with delete command, if a condition is not given, delete will remove all records

-- COMMAND ----------

select * from all_employees

-- COMMAND ----------

describe history all_employees

-- COMMAND ----------

restore table all_employees to version as of 1

-- COMMAND ----------

select * from all_employees

-- COMMAND ----------

drop table if exists locations_tab;

create table if not exists locations_tab (id int, loc string);
insert into locations_tab
select 1, "Hyderabad" union all
select 2, "Bangalore" union all
select 3, "Chennai";

-- COMMAND ----------

select * from locations_tab

-- COMMAND ----------

select id from locations_tab where loc="Hyderabad"

-- COMMAND ----------

-- inline query deletion or dependency based deletion
delete from all_employees where locid in (select id from locations_tab where loc="Hyderabad")

-- COMMAND ----------

select * from all_employees

-- COMMAND ----------

describe history all_employees

-- COMMAND ----------

restore table all_employees to version as of 3

-- COMMAND ----------

select * from all_employees

-- COMMAND ----------

-- USE EXISTS and NOT EXISTS instead of IN and NOT IN, for performance improvizations
-- especially during data reterival

-- COMMAND ----------

select * from all_employees a where exists (select id from locations_tab b where b.id=a.locid)

-- COMMAND ----------

