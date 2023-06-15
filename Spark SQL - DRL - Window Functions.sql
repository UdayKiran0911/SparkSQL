-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Window Functions
-- MAGIC ###### Ranking Functions
-- MAGIC - RANK | DENSE RANK | PERCENT RANK | NTILE | ROW NUMBER
-- MAGIC ###### Analytic Functions
-- MAGIC - CUME_DIST | LAG | LEAD
-- MAGIC ###### Aggregate Functions
-- MAGIC - SUM | COUNT | MIN | MAX | STD | AVG

-- COMMAND ----------

DROP TABLE IF EXISTS EMP;

CREATE TABLE EMP
(EMPNO DECIMAL(4),
ENAME VARCHAR(10),
JOB VARCHAR(9),
MGR DECIMAL(4),
HIREDATE DATE,
SAL DECIMAL(7, 2),
COMM DECIMAL(7, 2),
DEPTNO DECIMAL(2));

INSERT INTO emp VALUES ('7369','SMITH','CLERK','7902','1980-12-17','800.00',NULL,'20');
INSERT INTO emp VALUES ('7499','ALLEN','SALESMAN','7698','1981-02-20','1600.00','300.00','30');
INSERT INTO emp VALUES ('7521','WARD','SALESMAN','7698','1981-02-22','1250.00','500.00','30');
INSERT INTO emp VALUES ('7566','JONES','MANAGER','7839','1981-04-02','2975.00',NULL,'20');
INSERT INTO emp VALUES ('7654','MARTIN','SALESMAN','7698','1981-09-28','1250.00','1400.00','30');
INSERT INTO emp VALUES ('7698','BLAKE','MANAGER','7839','1981-05-01','2850.00',NULL,'30');
INSERT INTO emp VALUES ('7782','CLARK','MANAGER','7839','1981-06-09','2450.00',NULL,'10');
INSERT INTO emp VALUES ('7788','SCOTT','ANALYST','7566','1982-12-09','3000.00',NULL,'20');
INSERT INTO emp VALUES ('7839','KING','PRESIDENT',NULL,'1981-11-17','5000.00',NULL,'10');
INSERT INTO emp VALUES ('7844','TURNER','SALESMAN','7698','1981-09-08','1500.00','0.00','30');
INSERT INTO emp VALUES ('7876','ADAMS','CLERK','7788','1983-01-12','1100.00',NULL,'20');
INSERT INTO emp VALUES ('7900','JAMES','CLERK','7698','1981-12-03','950.00',NULL,'30');
INSERT INTO emp VALUES ('7902','FORD','ANALYST','7566','1981-12-03','3000.00',NULL,'20');
INSERT INTO emp VALUES ('7934','MILLER','CLERK','7782','1982-01-23','1300.00',NULL,'10');
INSERT INTO emp VALUES ('1234','RAVI','IT','7782','1985-06-10','1300.00',NULL,'50');

-- COMMAND ----------

select * from emp;

-- COMMAND ----------

select empno, ename, sal, rank() over(order by sal desc) as sal_rank from emp
-- Rank skips the sequence of generating rank for the same value

-- COMMAND ----------

select empno, ename, sal, rank() over(partition by deptno order by sal desc) as sal_rank from emp
-- Rank skips the sequence of generating rank for the same value

-- COMMAND ----------

select deptno, ename, sal, 
rank() over(order by sal desc) as sal_rank,
dense_rank() over(order by sal desc) as sal_denserank,
--dense rank won't skip the sequence
row_number() over(order by sal desc) as sal_rownum
from emp

-- COMMAND ----------

-- find the record with 3rd highest salary
select * from (select ename, sal, dense_rank() over(order by sal desc) as sal_drank from emp)
where sal_drank = 3;

-- COMMAND ----------

-- find the record with 3rd highest salary
select * from (select deptno, ename, sal, dense_rank() over(order by sal desc) as sal_drank from emp)
where sal_drank = 3;

-- COMMAND ----------

-- find the record with highest salary for each of the dept
select * from (select deptno, ename, sal, dense_rank() over(partition by deptno order by sal desc) as sal_drank from emp)
where sal_drank = 1;

-- COMMAND ----------

select deptno, ename, sal, 
rank() over(order by sal desc) as sal_rank,
dense_rank() over(order by sal desc) as sal_denserank,
--dense rank won't skip the sequence
row_number() over(order by sal desc) as sal_rownum,
percent_rank() over(order by sal desc) as sal_prank,
ntile(3) over(order by sal desc) as sal_ntile
from emp

-- COMMAND ----------

-- window functions with aggregate
select deptno, ename, sal, sum(sal) over(order by sal) as cumm_sal from emp;

-- COMMAND ----------

-- window functions with aggregate
select deptno, ename, sal, 
sum(sal) over(order by sal) as cumm_sal, 
avg(sal) over(order by sal) as changing_avg 
from emp;

-- COMMAND ----------

-- window functions with lead and lag
drop table if exists sales;

create table if not exists sales (year int, tot_sales int);
insert into sales
select 2015, 23000 union all
select 2016, 25000 union all
select 2017, 34000 union all
select 2018, 32000 union all
select 2019, 33000;

-- COMMAND ----------

select * from sales

-- COMMAND ----------

select year, tot_sales, 
lag(tot_sales) over(order by year) as pre_total, 
(tot_sales - pre_total) as diff
from sales

-- COMMAND ----------

select year, tot_sales, 
lead(tot_sales) over(order by year) as next_total, 
(next_total - tot_sales) as diff
from sales

-- COMMAND ----------

select year, tot_sales, 
lead(tot_sales, 2) over(order by year) as next_total, -- lead with skip by 2
(next_total - tot_sales) as diff
from sales

-- COMMAND ----------

select deptno, ename, sal, cume_dist() over(order by sal) as Cumm_Dist, 
percent_rank() over(order by sal) as sal_prank
from emp;