# SparkSQL

### DDL - Data Defination Language

##### __Database__
<table>
<thead><tr><th>statement</th><th>Query</th><th>Description</th><th>Example</th></tr></thead>
<tbody>
<tr>
<td>Create Database</td><td>CREATE DATABASE IF NOT EXISTS [DB_NAME];</td><td>Creates Database with the given name, only if it is not existing. 
<ul>Note:<ul>
<li><i>The database is created at the defualt location "user/hive/warehouse/"</i></li>
<li><i>If the database exist, it will raise an <code>NamespaceAlreadyExistsException</code> exception</i></li></td><td>CREATE DATABASE IF NOT EXISTS emp;</td>
</tr>


<tbody><tr><td>Database Description</td><td>DESCRIBE DATABASE <DB_NAME>;</td><td>Description</td><td>Example</td></tr>
<tbody><tr><td>Create Database at a specific location</td><td>SHOW DATABASES LOCATION "user/hive/warehouse/...";</td><td>Description</td><td>Example</td></tr>
<tbody><tr><td>List available databases</td><td>SHOW DATABASES;</td><td>Description</td><td>Example</td></tr>
</tbody>
</table>
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
