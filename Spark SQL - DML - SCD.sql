-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### SCD - Slowly Changing Dimensions
-- MAGIC <p>A slowly changing dimension (SCD) in data management and data warehousing is a dimension which contains relatively static data which can change slowly but unpredictably, rather than according to a regular schedule. Some examples of typical slowly changing dimensions are entities such as names of geographical locations, customers, or products.
-- MAGIC
-- MAGIC Some scenarios can cause referential integrity problems.
-- MAGIC
-- MAGIC For example, a database may contain a fact table that stores sales records. This fact table would be linked to dimensions by means of foreign keys. One of these dimensions may contain data about the company's salespeople: e.g., the regional offices in which they work. However, the salespeople are sometimes transferred from one regional office to another. For historical sales reporting purposes it may be necessary to keep a record of the fact that a particular sales person had been assigned to a particular regional office at an earlier date, whereas that sales person is now assigned to a different regional office.
-- MAGIC
-- MAGIC Dealing with these issues involves SCD management methodologies referred to as Type 0 through 6. Type 6 SCDs are also sometimes called Hybrid SCDs.</p>
-- MAGIC
-- MAGIC ###### Types
-- MAGIC - **Type 0**: 
-- MAGIC   - No changes allowed (static/append only)
-- MAGIC   - Useful for Lookup tables
-- MAGIC - **Type 1**: 
-- MAGIC   - Overwrite (no history is maintained)
-- MAGIC   - Useful when historical data is not required and need only the most recent data
-- MAGIC - **Type 2**: 
-- MAGIC   - Adding new row for each change and marking the old one as obselete
-- MAGIC   - Useful when we intend to keep the historical data, example price changes, location updates, designation changes etc.  
-- MAGIC - **Type 3**: 
-- MAGIC   - Adding new column to indicate the changes

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### SCD Type 1 - Example
-- MAGIC <p>Suppliers table</p>
-- MAGIC <table>
-- MAGIC   <thead>
-- MAGIC     <th>Supplier Key</th>
-- MAGIC     <th>Supplier Code</th>
-- MAGIC     <th>Supplier Name</th>
-- MAGIC     <th>Supplier Loc</th>
-- MAGIC   </thead>
-- MAGIC   <tbody>
-- MAGIC     <tr>
-- MAGIC       <td>123</td>
-- MAGIC       <td>ABC</td>
-- MAGIC       <td>Acme Supply Co</td>
-- MAGIC       <td>Delhi</td>
-- MAGIC     </tr>
-- MAGIC   </tbody>
-- MAGIC </table>
-- MAGIC
-- MAGIC <p>Supplier moved to a new state, in SCD - type 1, we overwrite the details</p>
-- MAGIC <table>
-- MAGIC   <thead>
-- MAGIC     <th>Supplier Key</th>
-- MAGIC     <th>Supplier Code</th>
-- MAGIC     <th>Supplier Name</th>
-- MAGIC     <th>Supplier Loc</th>
-- MAGIC   </thead>
-- MAGIC   <tbody>
-- MAGIC     <tr>
-- MAGIC       <td>123</td>
-- MAGIC       <td>ABC</td>
-- MAGIC       <td>Acme Supply Co</td>
-- MAGIC       <td>Kerala</td>
-- MAGIC     </tr>
-- MAGIC   </tbody>
-- MAGIC </table>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### SCD Type 2 - Example
-- MAGIC <p>Suppliers table</p>
-- MAGIC <table>
-- MAGIC   <thead>
-- MAGIC     <th>Supplier Key</th>
-- MAGIC     <th>Supplier Code</th>
-- MAGIC     <th>Supplier Name</th>
-- MAGIC     <th>Supplier Loc</th>
-- MAGIC   </thead>
-- MAGIC   <tbody>
-- MAGIC     <tr>
-- MAGIC       <td>123</td>
-- MAGIC       <td>ABC</td>
-- MAGIC       <td>Acme Supply Co</td>
-- MAGIC       <td>Delhi</td>
-- MAGIC     </tr>
-- MAGIC     <tr>
-- MAGIC       <td>123</td>
-- MAGIC       <td>ABC</td>
-- MAGIC       <td>Acme Supply Co</td>
-- MAGIC       <td>Kerala</td>
-- MAGIC     </tr>
-- MAGIC   </tbody>
-- MAGIC </table>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### SCD Type 3 - Example
-- MAGIC <p>Suppliers table</p>
-- MAGIC <table>
-- MAGIC   <thead>
-- MAGIC     <th>Supplier Key</th>
-- MAGIC     <th>Supplier Code</th>
-- MAGIC     <th>Supplier Name</th>
-- MAGIC     <th>Current Loc</th>
-- MAGIC     <th>Previous Loc</th>
-- MAGIC   </thead>
-- MAGIC   <tbody>
-- MAGIC     <tr>
-- MAGIC       <td>123</td>
-- MAGIC       <td>ABC</td>
-- MAGIC       <td>Acme Supply Co</td>
-- MAGIC       <td>Delhi</td>
-- MAGIC       <td>Delhi</td>
-- MAGIC     </tr>
-- MAGIC   </tbody>
-- MAGIC </table>
-- MAGIC
-- MAGIC <p>Supplier moved to a new state, in SCD - type 1, we overwrite the details</p>
-- MAGIC <table>
-- MAGIC   <thead>
-- MAGIC     <th>Supplier Key</th>
-- MAGIC     <th>Supplier Code</th>
-- MAGIC     <th>Supplier Name</th>
-- MAGIC     <th>Current Loc</th>
-- MAGIC     <th>Previous Loc</th>
-- MAGIC   </thead>
-- MAGIC   <tbody>
-- MAGIC     <tr>
-- MAGIC       <td>123</td>
-- MAGIC       <td>ABC</td>
-- MAGIC       <td>Acme Supply Co</td>
-- MAGIC       <td>Kerla</td>
-- MAGIC       <td>Delhi</td>
-- MAGIC     </tr>
-- MAGIC   </tbody>
-- MAGIC </table>

-- COMMAND ----------

-- DBTITLE 0,orical
-- MAGIC %md
-- MAGIC <table>
-- MAGIC   <thead>
-- MAGIC     <th>SCD</th>
-- MAGIC     <th>Operation</th>
-- MAGIC     <th>Comments</th>
-- MAGIC   </thead>
-- MAGIC   <tbody>
-- MAGIC     <tr>
-- MAGIC       <td>Type 0</td>
-- MAGIC       <td>append</td>
-- MAGIC       <td>-</td>
-- MAGIC     </tr>
-- MAGIC     <tr>
-- MAGIC       <td>Type 1</td>
-- MAGIC       <td>upsert</td>
-- MAGIC       <td>Only latest data (No history)</td>
-- MAGIC     </tr>
-- MAGIC     <tr>
-- MAGIC       <td>Type 2</td>
-- MAGIC       <td>upsert</td>
-- MAGIC       <td>Historical Data (Previous N times history in new row)</td>
-- MAGIC     </tr>
-- MAGIC     <tr>
-- MAGIC       <td>Type 3</td>
-- MAGIC       <td>upsert</td>
-- MAGIC       <td>Historical Data (only 1 time history in another column)</td>
-- MAGIC     </tr>
-- MAGIC   </tbody>
-- MAGIC </table>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC SCD - Slowly Changing Dimensions - Type 0

-- COMMAND ----------

drop table if exists dept;

create table if not exists dept (deptno decimal(2), dname varchar(14), loc varchar(13));

insert into dept
select 10 as deptno, "Accounting" as dname, "New York" as loc union all
select 20, "Research", "Dallas" union all
select 30, "Sales", "Chicago" union all
select 40, "Operations", "Boston";

-- COMMAND ----------

select * from dept

-- COMMAND ----------

drop table if exists dept_chng1;

create table if not exists dept_chng1 (deptno decimal(2), dname varchar(14), loc varchar(13));

insert into dept_chng1
select 10 as deptno, "Accounting" as dname, "New York" as loc union all
select 50, "IT", "Bangalore" union all
select 60, "HRMS", "Hyderabad" union all
select 40, "Operations", "Boston";

-- COMMAND ----------

select * from dept_chng1
-- in the new data, we received some rows which already exists
-- here we can merge two tables either using not in or anti join

-- COMMAND ----------

select * from dept_chng1 where deptno not in (select deptno from dept)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Using insert

-- COMMAND ----------

insert into dept 
select * from dept_chng1 where deptno not in (select deptno from dept)

-- COMMAND ----------

select * from dept

-- COMMAND ----------

describe history dept

-- COMMAND ----------

restore table dept version as of 1

-- COMMAND ----------

select * from dept

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### using Anti-Join

-- COMMAND ----------

select * from dept_chng1 src anti join dept tgt on src.deptno = tgt.deptno

-- COMMAND ----------

insert into dept
select * from dept_chng1 src anti join dept tgt on src.deptno = tgt.deptno

-- COMMAND ----------

select * from dept

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### UPSERT - Update and Insert

-- COMMAND ----------

drop table if exists dept_chng2;

create table if not exists dept_chng2 (deptno int, dname string, loc string);

insert into dept_chng2
select 10 as deptno, "Accounting" as dname, "New York" as loc union all
select 30, "Sales", "Delhi" union all
select 70, "After Market", "Hyderabad" union all
select 80, "Marketing", "Pune";

-- COMMAND ----------

-- DBTITLE 1,e
select * from dept_chng2
-- here the deptno 10 is repeated
-- dept no 30 have a location change

-- COMMAND ----------

merge into dept as tgt
using dept_chng2 as src
on tgt.deptno = src.deptno
when matched then
update set *
when not matched then
insert *

-- COMMAND ----------

select * from dept

-- COMMAND ----------

merge into dept as tgt
using dept_chng2 as src
on tgt.deptno = src.deptno
when matched and src.delete==true then
delete
when matched then
update set tgt.dname=src.dname and tgt.loc=src.loc
when not matched then
insert (deptno, dname, loc) values (src.deptno, src.dname, src.loc)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### UPSERT - Update, Insert and Delete

-- COMMAND ----------

drop table if exists events_tb;

create table if not exists events_tb (eid integer, edate date, edata string, deleteflag boolean);

insert into events_tb
select 1, '2021-01-01', '1st entry', 0 union all
select 2, '2021-02-02', '2nd entry', 0 union all
select 3, '2021-03-03', '3rd entry', 0 union all
select 4, '2021-04-04', '4th entry', 0;

-- COMMAND ----------

drop table if exists events_update_tb;

create table if not exists events_update_tb (eid integer, edate date, edata string, deleteflag boolean);

insert into events_update_tb
select 5, '2021-05-05', '5th entry', 0 union all
select 2, '2021-03-03', '2nd entry-updated', 1 union all
select 6, '2021-06-06', '3rd entry', 0 union all
select 1, '2021-04-04', '1st entry', 0;

-- COMMAND ----------

select * from events_tb

-- COMMAND ----------

select * from events_update_tb
-- eid 1 is an update
-- eid 5,6 are inserts
-- eid 2 is delete operation

-- COMMAND ----------

merge into events_tb tgt
using events_update_tb src
on tgt.eid = src.eid
when matched and src.deleteflag==true then
delete
when matched then
update set tgt.edate=src.edate, tgt.edata=src.edata
when not matched then
insert (eid, edate, edata, deleteflag) values (src.eid, src.edate, src.edata, src.deleteflag)

-- COMMAND ----------

select * from events_tb

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### SCD Type 2 - Mutlitple time based history

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### SCD Type 2 - Example
-- MAGIC <p>Employee location table</p>
-- MAGIC <table>
-- MAGIC   <thead>
-- MAGIC     <th>Name</th>
-- MAGIC     <th>Location</th>
-- MAGIC     <th>Start Date</th>
-- MAGIC     <th>End Date</th>
-- MAGIC     <th>Status</th>
-- MAGIC   </thead>
-- MAGIC   <tbody>
-- MAGIC     <tr>
-- MAGIC       <td>Raj</td>
-- MAGIC       <td>Hyderabad</td>
-- MAGIC       <td>2022-01-01</td>
-- MAGIC       <td>9999-12-31</td>
-- MAGIC       <td>True</td>
-- MAGIC     </tr>
-- MAGIC     <tr>
-- MAGIC       <td>Kishore</td>
-- MAGIC       <td>Bengalore</td>
-- MAGIC       <td>2022-05-07</td>
-- MAGIC       <td>9999-12-31</td>
-- MAGIC       <td>True</td>
-- MAGIC     </tr>
-- MAGIC     <tr>
-- MAGIC       <td>Sudha</td>
-- MAGIC       <td>Pune</td>
-- MAGIC       <td>2021-06-03</td>
-- MAGIC       <td>9999-12-31</td>
-- MAGIC       <td>True</td>
-- MAGIC     </tr>
-- MAGIC   </tbody>
-- MAGIC </table>
-- MAGIC <p>Employee location updated table</p>
-- MAGIC <table>
-- MAGIC   <thead>
-- MAGIC     <th>Name</th>
-- MAGIC     <th>Location</th>
-- MAGIC     <th>Start Date</th>
-- MAGIC     <th>End Date</th>
-- MAGIC     <th>Status</th>
-- MAGIC   </thead>
-- MAGIC   <tbody>
-- MAGIC     <tr>
-- MAGIC       <td>Raj</td>
-- MAGIC       <td>Hyderabad</td>
-- MAGIC       <td>2022-01-01</td>
-- MAGIC       <td>2022-07-15</td>
-- MAGIC       <td>False</td>
-- MAGIC     </tr>
-- MAGIC     <tr>
-- MAGIC       <td>Kishore</td>
-- MAGIC       <td>Bengalore</td>
-- MAGIC       <td>2022-05-07</td>
-- MAGIC       <td>9999-12-31</td>
-- MAGIC       <td>True</td>
-- MAGIC     </tr>
-- MAGIC     <tr>
-- MAGIC       <td>Sudha</td>
-- MAGIC       <td>Pune</td>
-- MAGIC       <td>2021-06-03</td>
-- MAGIC       <td>9999-12-31</td>
-- MAGIC       <td>True</td>
-- MAGIC     </tr>
-- MAGIC     <tr>
-- MAGIC       <td>Raj</td>
-- MAGIC       <td>Chennai</td>
-- MAGIC       <td>2022-07-15</td>
-- MAGIC       <td>9999-12-31</td>
-- MAGIC       <td>True</td>
-- MAGIC     </tr>
-- MAGIC   </tbody>
-- MAGIC </table>

-- COMMAND ----------

drop table if exists emploctab;

create table if not exists emploctab (empid int, name string, loc string, startdt date, enddt date, status boolean);
insert into emploctab
select 1, 'Raj','Hyderabad', '2022-01-01','9999-12-31', 1 union all
select 2, 'Kishore','Bengalore', '2022-05-07','9999-12-31',1 union all
select 3, 'Sudha','Pune', '2022-06-03','9999-12-31', 1;

-- COMMAND ----------

select * from emploctab;

-- COMMAND ----------

drop table if exists emploctab_up;

create table if not exists emploctab_up (empid int, name string, loc string, startdt date);
insert into emploctab_up
select 1, 'Raj','Chennai', '2022-07-15' union all
select 2, 'Kishore','Bengalore', '2022-05-07' union all
select 4, 'Amrith','Delhi', '2022-08-12';

-- COMMAND ----------

select * from emploctab_up

-- COMMAND ----------

select src.empid as mergekey, src.*
from emploctab_up src

-- COMMAND ----------

select null as mergekey, src.*
from emploctab_up src join emploctab tgt
on src.empid=tgt.empid
where tgt.status=1 and tgt.loc <> src.loc

-- COMMAND ----------

-- this part will get all the records from source table
select src.empid as mergekey, src.*
from emploctab_up src

union all

-- this will fethch all records from srouce where these is a change on location
-- between source and target form same ids
select null as mergekey, src.*
from emploctab_up src join emploctab tgt
on src.empid=tgt.empid
where tgt.status=1 and tgt.loc <> src.loc

-- because of union all, we might see some records reappearning

-- COMMAND ----------

merge into emploctab tgt
using (
  select src.empid as mergekey, src.*
  from emploctab_up src
  union all
  select null as mergekey, src.*
  from emploctab_up src join emploctab tgt
  on src.empid=tgt.empid
  where tgt.status=1 and tgt.loc <> src.loc
) staged_src
on tgt.empid = staged_src.mergekey -- this well make sure only the common records are effected, where mergekey="null" will not be considered
when matched and tgt.status=1 and tgt.loc <> staged_src.loc then
  update set tgt.status = 0, tgt.enddt = staged_src.startdt
when not matched then
  insert(tgt.empid, tgt.name, tgt.loc, tgt.startdt, tgt.enddt, tgt.status)
  values(staged_src.empid, staged_src.name, staged_src.loc, staged_src.startdt, '9999-12-31',1)

-- COMMAND ----------

select * from emploctab

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Upsert SCD-Type 3

-- COMMAND ----------

drop table if exists scdtype3_tgt;

create table if not exists scdtype3_tgt (empid int, name string, cloc string, ploc string);
insert into scdtype3_tgt
select 1, 'Raj','Hyderabad',null union all
select 2, 'Kishore','Bengalore',null union all
select 3, 'Sudha','Pune',null;

-- COMMAND ----------

select * from scdtype3_tgt;

-- COMMAND ----------

drop table if exists scdtype3_src;

create table if not exists scdtype3_src (empid int, name string, cloc string);
insert into scdtype3_src
select 1, 'Raj','Chennai' union all
select 2, 'Kishore','Patna' union all
select 3, 'Sudha','Pune' union all
select 4, 'Sridhar','Bengalore' union all
select 5, 'Krishna','Delhi';

-- Record 1 and 2 have an updated loc, 3 is a duplicate, 4,5 are new

-- COMMAND ----------

select * from scdtype3_src

-- COMMAND ----------

merge into scdtype3_tgt tgt
using scdtype3_src src
on tgt.empid = src.empid
when matched and src.cloc <> tgt.cloc then
update set tgt.ploc = tgt.cloc, tgt.cloc = src.cloc
when not matched then
insert (tgt.empid, tgt.name, tgt.cloc, tgt.ploc)
values(src.empid, src.name, src.cloc, null)

-- COMMAND ----------

select * from scdtype3_tgt;