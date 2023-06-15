-- Databricks notebook source
DROP TABLE IF EXISTS emp;
DROP TABLE IF EXISTS dept;
CREATE TABLE IF NOT EXISTS `emp`(`EMPNO` INT,`ENAME` STRING,`JOB` STRING,`MGR` STRING,`HIREDATE` DATE,`SAL` INT,`COMM` STRING,`DEPTNO` INT) USING delta;
CREATE TABLE IF NOT EXISTS `dept`( `Deptno` INT, `Dname` STRING, `Loc` STRING) USING delta;
INSERT INTO emp VALUES
(7698,'SGR','MANAGER',7839,'1981-01-05',2850,NULL,30),
(7782,'RAVI','MANAGER',7839,'1981-09-06',2450,NULL,40),
(7788,'SCOTT','ANALYST',7566,'1987-09-04',3000,NULL,20),
(7839,'KING','PRESIDENT',NULL,'1981-07-11',5000,NULL,10),
(7844,'TURNER','SALESMAN',7698,'1981-08-09',1500,0,30),
(7876,'ADAMS','CLERK',7788,'1987-03-05',1100,NULL,50),
(7900,'JAMES','CLERK',7698,'1981-03-12',950,NULL,40),
(7902,'FORD','ANALYST',7566,'1981-03-12',3000,NULL,20),
(7934,'MILLER','CLERK',7782,'1982-03-01',1300,NULL,10),
(1234,'SEKHAR','doctor',7777,NULL,667,78,50);
INSERT INTO DEPT VALUES (10,'ACCOUNTING','NEW YORK'),
(20,'RESEARCH','DALLAS'),
(30,'SALES','CHICAGO'),
(40,'OPERATIONS','BOSTON'),
(60,'MARKETING','LONDON');

-- COMMAND ----------

select * from emp;

-- COMMAND ----------

select * from dept;

-- COMMAND ----------

-- INNER JOIN
-- Matching deptno data
SELECT *
FROM emp as e
INNER JOIN dept as d
ON e.deptno = d.deptno;

-- COMMAND ----------

-- LEFT JOIN
-- Must have all data from left irrespective of matching or not
-- if not match it would return null
SELECT *
FROM emp as e
LEFT OUTER JOIN dept as d
ON e.deptno = d.deptno;

-- COMMAND ----------

-- RIGHT JOIN
-- Must have all data from right irrespective of matching or not
-- if not match it would return null
SELECT *
FROM emp as e
RIGHT OUTER JOIN dept as d
ON e.deptno = d.deptno;

-- COMMAND ----------

-- FULL OUTER JOIN
-- Must have all data from right and left irrespective of matching or not
-- if not match it would return null
SELECT *
FROM emp as e
FULL OUTER JOIN dept as d
ON e.deptno = d.deptno;

-- COMMAND ----------

-- LEFT ANTI JOIN
-- Unmatched data from left table
-- same as NOT EXISTS or NOT IN
SELECT *
FROM emp as e
LEFT ANTI JOIN dept as d
ON e.deptno = d.deptno;

-- COMMAND ----------

select *
from emp as e
where not exists (select 'x' from dept as d where d.deptno = e.deptno)

-- COMMAND ----------

-- LEFT SEMI JOIN
-- matched data from left table only
-- Same as EXISTS and IN
SELECT *
FROM emp as e
LEFT SEMI JOIN dept as d
ON e.deptno = d.deptno;

-- COMMAND ----------

select *
from emp as e
where exists (select 'x' from dept as d where d.deptno = e.deptno)

-- COMMAND ----------

select *
from emp
where deptno in (select deptno from dept)