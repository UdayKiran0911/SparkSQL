-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### SQL Commands
-- MAGIC
-- MAGIC - **DDL (Data Defination Language)**
-- MAGIC   - CREATE, ALTER, DROP, RENAME, TRUNCATE
-- MAGIC - **DML (Data Manupilation Language)**
-- MAGIC   - INSERT, UPDATE, DELETE, MERGE
-- MAGIC - **DCL (Data Control Language)**
-- MAGIC   - GRANT, REVOKE
-- MAGIC - **DR(Q)L (Data Retrival or Query Language)**
-- MAGIC   - SELECT
-- MAGIC - **TCL (Transaction Control Language)** (Not Used in Bigdata, Databricks File system manages with the help of Delta)
-- MAGIC   - COMMIT, ROLLBACK

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### On Premises Data Warehousing Project
-- MAGIC ##### OLTP - Online Transaction Processing Databases
-- MAGIC ##### OLAP - Online Analytic Processing Databases
-- MAGIC
-- MAGIC - Organizations store the data from front end applications in ERP, or files or in databases (OLTP)
-- MAGIC - A Data engineer is responsible for extracting the data from the database and perform ETL
-- MAGIC   - E: Extraction: Extracting data from various sources (OLTP) based on business requirement
-- MAGIC   - T: Transformation: Perform Data validation, Data cleaning, Data ingestion, Data Aggregation
-- MAGIC   - L: Load: Loading the data into Data warehouse (OLAP)
-- MAGIC - After ETL, the data is stored for analytical purposes in data wharehouse (OLAP)
-- MAGIC - The data from OLAP is then used for Anlytics, Presentations, ML or DS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Data Defination Language (DDL)
-- MAGIC - Creating Databases, Tables, Views
-- MAGIC - Altering Tables, Views
-- MAGIC - Deleting Databases, Tables, Views
-- MAGIC - Data Types
-- MAGIC - SQL Constraints

-- COMMAND ----------

create database if not exists sparksql_db;
--the command will create database in the spark_catalog, from the left menu go to data.*/

-- COMMAND ----------

describe database sparksql_db;

-- COMMAND ----------

create table if not exists sparksql_db.customers(id int, name string)
--the command will create table customers in the sparksql_db database
--the table is created as delta table */

-- COMMAND ----------

insert into sparksql_db.customers values(1, "Uday")

-- COMMAND ----------

-- MAGIC %fs 
-- MAGIC ls /user/hive/warehouse/sparksql_db.db/customers

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.get("spark.sql.warehouse.dir") # this is where the defualt location is selected for database and table location
-- MAGIC # also the defualt location is an AWS S3 location (if we are using only the databricks without azure)
-- MAGIC # spark uses hive metastore (catalog) to store the database(s)

-- COMMAND ----------

--if database is selected the table will be created in the default location
-- that is /user/hive/warehouse, you can locate the table in the left menu>>data>>hive_metastore>>default
create table if not exists customers(id int, name string)

-- COMMAND ----------

create database if not exists uday_db location "/user/hive/warehouse/uday_db"

-- COMMAND ----------

describe database uday_db