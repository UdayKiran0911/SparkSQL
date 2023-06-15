-- Databricks notebook source
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

select *
from emp

-- COMMAND ----------

select empno, ename, sal
from emp

-- COMMAND ----------

DROP TABLE IF EXISTS DEPT;

CREATE TABLE DEPT
(DEPTNO DECIMAL(2),
DNAME VARCHAR(14),
LOC VARCHAR(13) );


INSERT INTO DEPT 
select 10 as deptno, 'ACCOUNTING' as dname, 'NEW YORK' as loc
union all  
select 20, 'RESEARCH', 'DALLAS'
union all
select 30, 'SALES', 'CHICAGO'
union all 
select  40, 'OPERATIONS', 'BOSTON';

-- COMMAND ----------

select * 
from dept

-- COMMAND ----------

-- for alias we can either use 'as' or just space
select *
from emp as e inner join dept as d on d.deptno=e.deptno;
--from emp e inner join dept d on d.deptno=e.deptno;

-- COMMAND ----------

select empno, ename, sal as salary, e.deptno as dept
from emp as e inner join dept as d on d.deptno=e.deptno;
-- spark sql alias, double/single quotes or space are not allowed like 'employee_salary'/"employee_salary"/'employee salary'
-- in dataframe it is alloweds

-- COMMAND ----------

select empno, ename, sal, comm, sal+comm as total_salary
from emp

-- COMMAND ----------

-- converting null values to actual values, we can use
-- nvl
-- coalesce
select empno, ename, sal, comm, sal+nvl(comm,0) as total_salary
from emp

-- COMMAND ----------

select empno, ename, sal, comm, sal+coalesce(null,null,null,null,comm,0) as total_salary
from emp
-- coalesce will find the first not null value, if all are null then will return the 0 or any value that is specified
-- coalesce(null, null, null, comm, <custome_val>)