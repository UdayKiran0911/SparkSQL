-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Constraints
-- MAGIC - Delta tables support standard SQL constraint managament clauses that ensure the quality and integrity of data added to a table is automatically verified. When a constraint is violated, Delta lake throws an DeltaInvariantValidationException to signal that the new data can't be added
-- MAGIC
-- MAGIC - **NOT NULL**: Indicates that values in specific column cannot be null
-- MAGIC - **CHECK**: Indicates that the specified boolean expression must be true for each input row.
-- MAGIC
-- MAGIC Primery Key, Foreign Key constraints are available in unity catalog, and not supported in spark catalog

-- COMMAND ----------

Drop table if exists events;
create table if not exists events (
  id int not null,
  date string not null,
  location string,
  description string
) using delta;

-- COMMAND ----------

alter table events change column date drop not null;

-- COMMAND ----------

show create table events

-- COMMAND ----------

insert into events values(NULL, NULL, "Hyderabad","This is a sample")

-- COMMAND ----------

alter table events add constraint datecheck Check (date > '1900-01-01')

-- COMMAND ----------

show create table events

-- COMMAND ----------

insert into events values(1, '1800-01-01', "Hyderabad","Thos is a sample")

-- COMMAND ----------

alter table events drop constraint datecheck

-- COMMAND ----------

show create table events