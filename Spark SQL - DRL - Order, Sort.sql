-- Databricks notebook source
DROP TABLE IF EXISTS person;

CREATE TABLE IF NOT EXISTS person (pid int, pname string, age int);
insert into person values
(100, "Ravi", NULL),
(200, "Prasad", 36),
(300, "Raj", 40),
(400, "Sridhar", 34),
(500, "Mahesh", 35);

-- COMMAND ----------

select * from person;

-- COMMAND ----------

select * from person order by pname
-- default order is ascending order (increasing order)

-- COMMAND ----------

select * from person order by pname desc

-- COMMAND ----------

select * from person order by age
-- null values are displayed first and data will be in ascending order by default

-- COMMAND ----------

select * from person order by age nulls last
-- data is still in ascending order

-- COMMAND ----------

select * from person order by age desc nulls last

-- COMMAND ----------

select * from person sort by age

-- COMMAND ----------

select * from person sort by pname

-- COMMAND ----------

-- Understanding Difference between Sort by and Order by 
DROP TABLE IF EXISTS person_demo;
CREATE TABLE person_demo (zip_code INT, name STRING, age INT);
INSERT INTO person_demo VALUES
    (560043, 'Ravi', NULL),
    (560043, 'Prasad', 36),
    (560043, 'Raj', 40),
    (560016, 'Sridhar', 34),
    (560016, 'Mahesh', 35),
    (560016, 'Anitha', 36),
    (560005, 'Sindhu', NULL),
    (560005, 'Vikranth', 10),
    (560005, 'Reshwanth', 05),
    (560005, 'Ram', 15),
    (560005, 'Ram', 25),
    (560005, 'Ranjith', 30),
    (560005, 'Ranjith', 12);

-- COMMAND ----------

select * from person_demo sort by name

-- COMMAND ----------

select * from person_demo order by name

-- COMMAND ----------

-- Observe that both queries, sort and order by used 1 spark job
-- however the difference appears when there is a partition
-- lets requery both the order by and sort by with zip code as partition hint
-- Also observe the number of jobs created
-- in the case of sort for ordering happend for age for each of the partition.
-- in the case of order while the partition was based on zip code, the final order of the age is ascending, there is no local ordering.

select /*+REPARTITION(3, zip_code)*/ * from person_demo sort by age

-- COMMAND ----------

select /*+REPARTITION(3, zip_code)*/ * from person_demo order by age

-- COMMAND ----------

select 1+1