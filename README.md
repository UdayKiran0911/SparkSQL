# SparkSQL

### DDL - Data Defination Language

##### __Database__

* _Create Database_ : <code>CREATE DATABASE IF NOT EXISTS <DB_NAME>;</code>

* _Database Description_ : <code>DESCRIBE DATABASE <DB_NAME>;</code>
 
* _Create Database at a specific location_ : <code>SHOW DATABASES LOCATION "user/hive/warehouse/...";</code>

* _List of all available databases in spark - catalog_ : <code>SHOW DATABASES;</code>

##### __Table__

* _Create Table_ : <code>CREATE TABLE IF NOT EXISTS <TB_NAME> (Col1 type, col2 type, col3 type...);</code>

* _Create Table with primary key_ : <code>CREATE TABLE IF NOT EXISTS <TB_NAME> (Col1 type primary, col2 type, col3 type...);</code>

* _Create Table with not null constraint_ : <code>CREATE TABLE IF NOT EXISTS <TB_NAME> (Col1 type not null, col2 type, col3 type...);</code>

* _Table Description_ : <code>DESCRIBE TB_NAME;</code>
 
* _Create Database at a specific location_ : <code>SHOW DATABASES LOCATION "user/hive/warehouse/...";</code>

* _List of all available databases in spark - catalog_ : <code>SHOW DATABASES;</code>
---
### DML - Data Modification Language
---
### DRL - Data Retrival Language
