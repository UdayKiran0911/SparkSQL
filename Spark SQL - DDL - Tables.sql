-- Databricks notebook source
-- the defualt format of tables in spark sql, is delta table
create table sparksql_db.customers(id int, name string, loc string)

-- COMMAND ----------

select * from sparksql_db.customers

-- COMMAND ----------

show create table sparksql_db.customers

-- COMMAND ----------

describe sparksql_db.customers

-- COMMAND ----------

create table if not exists customers2 (
  id int,
  name string,
  loc string
)
using delta
location 'user/hive/warehouse/'
tblproperties (
  'type'='EXTERNAL'
)

-- COMMAND ----------

create table if not exists customers3 (
  id int,
  name string,
  loc string
)
using delta
tblproperties (
  'type'='MANAGED'
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - When tables are created without location
-- MAGIC - spark manages the data and the metadata of the table hence they are managed tables (Internal table)
-- MAGIC - when tables are created with location
-- MAGIC - spark only manages the data not the metadata of the table hence they are external tables
-- MAGIC
-- MAGIC ##### Difference
-- MAGIC - when we remove or drop the table
-- MAGIC - in Managed, the location is also removed along with table (Data, Metadata are removed)
-- MAGIC - in Extrenal, the metadata is removed from **spark_catalog**, but the data is still avaialble in the external location

-- COMMAND ----------

-- MAGIC %fs ls /user/hive/warehouse/

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### USING
-- MAGIC - we can use, csv, json, delta, parquet etc.
-- MAGIC
-- MAGIC ##### TBLPROPERTIES
-- MAGIC - we can use the "type"="MANAGED" or "EXTERNAL"
-- MAGIC - we can also use 'create external table TABLE1 location "LOC"'
-- MAGIC
-- MAGIC ##### LOCATION
-- MAGIC - to specify the location for the table

-- COMMAND ----------

create table if not exists sparksql_db.Tab1(
  id int, name string)
location "use/hive/warehouse/Tab1"

-- This should create a table in sparksql_db database
-- and also create in "use/hive/warehouse/Tab1" (Note the location)
-- as external table
-- and default format as delta

-- COMMAND ----------

show create table sparksql_db.Tab1

-- COMMAND ----------

create table if not exists sparksql_db.Tab2(
  id int, name string)

-- This should create a table in sparksql_db database
-- as managed table
-- and default format as delta

-- COMMAND ----------

show create table sparksql_db.Tab2

-- COMMAND ----------

-- droping external table
drop table sparksql_db.Tab1

-- COMMAND ----------

show create table sparksql_db.Tab1

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("/user/hive/warehouse/use/hive/warehouse/Tab1")

-- COMMAND ----------

-- droping managed table
drop table sparksql_db.Tab2

-- COMMAND ----------

show create table sparksql_db.Tab2

-- COMMAND ----------

-- In either case
-- we will not find the table from spark_catalog
-- and for managed tables location is also removed
-- but for external the location of the table can be found