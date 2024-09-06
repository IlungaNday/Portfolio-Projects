-- WORLD LIFE EXPECTANCY PROJECT (DATA CLEANING)

SELECT * 
FROM world_life_expectancy
;

CREATE TABLE `world_life_expectancy_staging` (
  `Country` text,
  `Year` int DEFAULT NULL,
  `Status` text,
  `Life expectancy` text,
  `Adult Mortality` int DEFAULT NULL,
  `infant deaths` int DEFAULT NULL,
  `percentage expenditure` double DEFAULT NULL,
  `Measles` int DEFAULT NULL,
  `BMI` double DEFAULT NULL,
  `under-five deaths` int DEFAULT NULL,
  `Polio` int DEFAULT NULL,
  `Diphtheria` int DEFAULT NULL,
  `HIV/AIDS` double DEFAULT NULL,
  `GDP` int DEFAULT NULL,
  `thinness  1-19 years` double DEFAULT NULL,
  `thinness 5-9 years` double DEFAULT NULL,
  `Schooling` double DEFAULT NULL,
  `Row_ID` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; 

INSERT world_life_expectancy_staging
SELECT *
FROM world_life_expectancy 
;

SELECT country, YEAR, CONCAT(country, Year), COUNT(CONCAT(country, Year))
FROM world_life_expectancy
GROUP BY country, YEAR, CONCAT(country, Year) 
HAVING COUNT(CONCAT(country, Year)) > 1 
;
-- Findaing duplicates 

SELECT *
FROM(
	SELECT Row_id,
	CONCAT(country, Year), 
	ROW_NUMBER()OVER(PARTITION BY(CONCAT(country, Year))ORDER BY CONCAT(country, Year)) AS Row_Num
	FROM world_life_expectancy
    ) AS Row_table
    WHERE row_num > 1
;

    
DELETE FROM world_life_expectancy
WHERE 
	Row_ID IN (
    SELECT Row_id
FROM(
	SELECT Row_id,
	CONCAT(country, Year), 
	ROW_NUMBER()OVER(PARTITION BY(CONCAT(country, Year))ORDER BY CONCAT(country, Year)) AS Row_Num
	FROM world_life_expectancy
    ) AS Row_table
    WHERE row_num > 1 
    )
    ; 
    -- DELETED DUPLICATES 
    
SELECT * 
FROM world_life_expectancy
WHERE status = ''
; 

SELECT DISTINCT(status) 
FROM world_life_expectancy
WHERE status <> ''
;

SELECT DISTINCT(Country)
FROM world_life_expectancy
WHERE status = 'Developing'
; 

UPDATE world_life_expectancy
SET status = 'Developing'
WHERE COUNTRY IN (SELECT DISTINCT(Country)
				  FROM world_life_expectancy
				  WHERE status = 'Developing')
;
-- This method did not work. I will look for a workaround 

UPDATE world_life_expectancy AS t1
JOIN world_life_expectancy AS t2
	ON t1.Country = t2.Country
SET t1.status = 'Developing'
WHERE t1.status = '' 
AND t2.status <> '' 
AND t2.status = 'Developing' 
;
-- we are joining to itself so that we can filter from the other table.

SELECT * 
FROM world_life_expectancy
WHERE Country = 'United States of America'
; 

UPDATE world_life_expectancy AS t1
JOIN world_life_expectancy AS t2
	ON t1.Country = t2.Country
SET t1.status = 'Developed'
WHERE t1.status = '' 
AND t2.status <> '' 
AND t2.status = 'Developed' 
;

SELECT *
FROM world_life_expectancy
-- WHERE `Life expectancy` = ''
;

SELECT country, Year, `Life expectancy` 
FROM world_life_expectancy
-- WHERE `Life expectancy` = ''
;
-- I am trying to populate a blank field in the life expectancy column

SELECT t1.country, t1.Year, t1.`Life expectancy`, 
t2.country, t2.Year, t2.`Life expectancy`,
t3.country, t3.Year, t3.`Life expectancy`,
ROUND((t2.`Life expectancy` + t3.`Life expectancy`)/2, 1)  
FROM world_life_expectancy AS t1
JOIN world_life_expectancy AS t2 
	ON t1.country = t2.country 
    AND t1.Year = t2.year - 1
JOIN world_life_expectancy AS t3 
	ON t1.country = t3.country 
    AND t1.Year = t3.year + 1
WHERE t1.`Life expectancy` = ''
;


UPDATE world_life_expectancy AS t1
JOIN world_life_expectancy AS t2 
	ON t1.country = t2.country 
    AND t1.Year = t2.year - 1
JOIN world_life_expectancy AS t3 
	ON t1.country = t3.country 
    AND t1.Year = t3.year + 1 
SET t1.`Life expectancy` = ROUND((t2.`Life expectancy` + t3.`Life expectancy`)/2, 1) 
WHERE t1.`Life expectancy` = '';