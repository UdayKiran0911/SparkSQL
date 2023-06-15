-- Databricks notebook source
DROP TABLE  IF EXISTS dealer;
CREATE TABLE dealer (id INT, city STRING, car_model STRING, quantity INT);
INSERT INTO dealer VALUES
    (100, 'Bangalore', 'Honda Civic', 10),
    (100, 'Bangalore', 'Honda Accord', 15),
    (100, 'Bangalore', 'Honda CRV', 7),
    (200, 'Chennai', 'Honda Civic', 20),
    (200, 'Chennai', 'Honda Accord', 10),
    (200, 'Chennai', 'Honda CRV', 3),
    (300, 'Hyderabad', 'Honda Civic', 5),
    (300, 'Hyderabad', 'Honda Accord', 8);

-- COMMAND ----------

select * from dealer;

-- COMMAND ----------

select city, sum(quantity) from dealer group by city

-- COMMAND ----------

select city, car_model, sum(quantity) from dealer
group by city, car_model
grouping sets ((city, car_model), (city), (car_model), ())
-- () will give all totals
-- (car_model) will give totals car mode wise
-- (city) will give totals city wise
-- (city, car_model) will gives totals by city and car_model

-- COMMAND ----------

select city, car_model, sum(quantity) from dealer
group by city, car_model
with rollup

-- COMMAND ----------

select city, car_model, sum(quantity) from dealer
group by city, car_model
with cube

-- COMMAND ----------

select city, car_model, round(avg(quantity),2) from dealer
group by city, car_model
with cube