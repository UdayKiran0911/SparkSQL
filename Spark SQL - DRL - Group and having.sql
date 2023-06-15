-- Databricks notebook source
DROP TABLE IF EXISTS emp;

CREATE TABLE IF NOT EXISTS `emp`(`EMPNO` INT,`ENAME` STRING,`JOB` STRING,`MGR` STRING,`HIREDATE` DATE,`SAL` INT,`COMM` STRING,`DEPTNO` INT) USING delta;
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

-- COMMAND ----------

select count(*), sum(sal), avg(sal), min(sal), max(sal), std(sal) from emp

-- COMMAND ----------

select job, sum(sal) from emp group by job

-- COMMAND ----------

-- adding a condition using where will not work, becasue where class do not take aggregate functions
-- we would require Having class
-- we can also used postion of the columns in group, based on the columns that are being called in select class
-- select job, sum(sal) from emp group by 1 having sum(sal) > 3000
select job, sum(sal) from emp group by job having sum(sal) > 3000