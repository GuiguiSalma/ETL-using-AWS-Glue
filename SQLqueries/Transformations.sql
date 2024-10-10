
-- This query creates a table in Parquet format with weather data from 1950 to 2015

CREATE table weatherdata.late20th
WITH (
  format='PARQUET', external_location='s3://<glue-1950-bucket--ec80af40>/lab3'
) AS SELECT date, type, observation  FROM by_year
WHERE date/10000 between 1950 and 2015;


-- Creating the view TMAX to filter the maximum temperature data

CREATE VIEW TMAX AS
SELECT date, observation, type
FROM late20th
WHERE type = 'TMAX'


-- The purpose of this query is to calculate the average maximum temperature for each year in the dataset.


SELECT date/10000 as Year, avg(observation)/10 as Max
FROM tmax
GROUP BY date/10000 ORDER BY date/10000;