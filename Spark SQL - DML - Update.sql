-- Databricks notebook source
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

update all_employees set locid=4 where empid=3

-- COMMAND ----------

select * from all_employees

-- COMMAND ----------

