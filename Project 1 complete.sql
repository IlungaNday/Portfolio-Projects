-- DATA CLEANING


SELECT * 
FROM layoffs; 

-- 1. REMOVE DUPLICATES 
-- 2. STANDARDIZE THE DATA 
-- 3. NULL VALLUES or BLANK VALUES
 -- 4. REMOVE ANY COLUMNS/ROWS OR IRRELEVANT DATA
 
 
-- 1. REMOVE DUPLICATES 
 CREATE TABLE layoffs_staging 
 LIKE layoffs; 
 -- created a new table because we do not want to edit the main data just in case we make a mistake.
 
 SELECT * 
 FROM layoffs_staging;
 
 INSERT layoffs_staging
 SELECT * 
 FROM layoffs; 
 -- Inserted data from layoffs to new table
 
  SELECT *,
  ROW_NUMBER() OVER(
  PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
 FROM layoffs_staging;
 -- we will add a row number so that we can identify duplicates
 
 WITH duplicate_cte AS 
 (
 SELECT *,
  ROW_NUMBER() OVER(
  PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`,
  stage, country, funds_raised_millions) AS row_num
 FROM layoffs_staging
 )
 -- cte will allow us to filter the data in the row_num column and it will also make everything look cleaner. I could have used a subquery but it would not look as clean 
 
 SELECT * 
 FROM duplicate_cte 
 WHERE row_num > 1;
 -- All values above are duplicates
 
 SELECT * 
 FROM layoffs_staging
 WHERE company = 'casper';
 -- Confirming the duplicates
 
 CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; 
-- creating new table with the data from the cte so that we can delete duplicates through the row_num
 
 SELECT * 
 FROM layoffs_staging2; 
 
 INSERT INTO layoffs_staging2
 SELECT *,
  ROW_NUMBER() OVER(
  PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`,
  stage, country, funds_raised_millions) AS row_num
 FROM layoffs_staging
 ;
-- NEW DATA has been added to the new table

DELETE
FROM layoffs_staging2 
WHERE row_num > 1 
;
-- Duplicates deleted

SELECT * 
FROM layoffs_staging2 
WHERE row_num > 1
;
SELECT * 
FROM layoffs_staging2 ;
-- ensuring that duplicates have been deleted 
 
-- Standardizing Data  
-- In this phase i will be finding issue in my data and fixing it 

SELECT company, TRIM(company)
FROM layoffs_staging2; 
-- There were spaces at the begining of the company names. so i got rid of them.

UPDATE layoffs_staging2 
SET company = TRIM(company); 
-- Updated the data in the table 

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1
; 
-- looking at the data to make sure there aren't any errors

SELECT industry
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%'
; 
-- i have identified an issue with crypto. There are different phrases that mean the same thing. 

UPDATE layoffs_staging2 
SET industry = 'Crypto'
;
-- This will change all the data to have the same spelling for crypto 

SELECT industry
FROM layoffs_staging2;
-- I just confirmed that everything was updated properly. 
--------------------------------------------------------------------------------------------
-- I just made a mistake i accidentally update all rows to be crypto
-- I will creat a new table called layoffs_staging22
-- And this is why we create new tables lol 

CREATE TABLE `layoffs_staging22` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; 

SELECT * 
FROM layoffs_staging22;

INSERT INTO layoffs_staging22
SELECT *,
  ROW_NUMBER() OVER(
  PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`,
  stage, country, funds_raised_millions) AS row_num
 FROM layoffs_staging
 ;
 -- I created a new table and i will edit it correctly this time 
 
SELECT company, TRIM(company)
FROM layoffs_staging22; 


UPDATE layoffs_staging22 
SET company = TRIM(company);  


UPDATE layoffs_staging22 
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%'; 

SELECT DISTINCT industry 
FROM layoffs_staging22; 
-- I have corrected my mistake. 
-- I forgot to add the where statement which would ensure that only the columns with crypto written differently would be update

SELECT DISTINCT location 
FROM layoffs_staging22
ORDER BY 1; 

SELECT DISTINCT location 
FROM layoffs_staging22
WHERE location LIKE 'D%'
ORDER BY 1; 
-- i have found an issue with the spelling of dusseldorf. 

UPDATE layoffs_staging22 
SET location = 'Dusseldorf'
WHERE location Like '%sseldorf'; 
-- I just update the table with the correct data 

SELECT DISTINCT location
From layoffs_staging22 
ORDER BY 1;
-- it updated correct. However, i found an issue with the spelling of Florianopolis

SELECT DISTINCT location
From layoffs_staging22 
WHERE location LIKE 'F%'
ORDER BY 1; 

SELECT * 
FROM layoffs_staging22 
WHERE location LIKE 'F%'; 

UPDATE layoffs_staging22 
SET location = 'Florianopolis' 
WHERE location LIKE 'Florian%'; 
-- Florianopolis has been corrected 

SELECT * 
FROM layoffs_staging22
WHERE location LIKE 'Malm%'
ORDER BY 1; 

UPDATE layoffs_staging22 
SET location = 'Malmo' 
WHERE location LIKE 'Malm%'; 
-- Malmo has been corrected 

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country) 
FROM layoffs_staging22 
WHERE country LIKE 'United States%'
ORDER BY 1;

UPDATE layoffs_staging22
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT `date`
FROM layoffs_staging22
;
-- COnverting our date column from string to date data type

UPDATE layoffs_staging22 
SET date = STR_TO_DATE(`date`, '%m/%d/%Y');
 
ALTER TABLE layoffs_staging22
MODIFY COLUMN `date` DATE;
-- Changes the column data type from text to date

-- 3. CHANGING NULL VALLUES or BLANK VALUES

SELECT *
FROM layoffs_staging22
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL; 
-- SHOWS me the NULLS in my data

UPDATE layoffs_staging22
SET industry = NULL 
WHERE industry = '';
-- Setting everything to null to make it easier to populate them empty fields 

SELECT *
FROM layoffs_staging22
WHERE industry IS NULL 
OR industry = '';

SELECT *
FROM layoffs_staging22
WHERE company LIKE 'Bally%';
-- Checking if i can populate the blank areas in the data. 
-- After looking at it, i can populate the Airbnb collumn with data but i can not populate Bally's%

SELECT st1.industry, st2.industry
FROM layoffs_staging22 AS st1 
JOIN layoffs_staging22 AS st2
	ON st1.company = st2.company 
    AND st1.location = st2.location 
WHERE (st1.industry IS NULL OR st1.industry = '')
AND st2.industry IS NOT NULL;

UPDATE layoffs_staging22 st1
JOIN layoffs_staging22 st2
	ON st1.company = st2.company 
SET st1.industry = st2.industry 
WHERE st1.industry IS NULL 
AND st2.industry IS NOT NULL;
-- Now Airbnb industry has been populated with data

-- 4. REMOVE ANY COLUMNS/ROWS OR IRRELEVANT DATA
SELECT * 
FROM layoffs_staging22;

SELECT *
FROM layoffs_staging22
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL; 


DELETE 
FROM layoffs_staging22
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL; 

SELECT * 
FROM layoffs_staging22;


ALTER TABLE layoffs_staging22 
DROP COLUMN row_num; 
--------------------------------------------------------------------------------------------
 -- REMOVING DUPLICATES FROM TABLE. FINAL TABLE IS layoffs_staging3
 
 SELECT * 
 FROM layoffs_staging22; 
 
   SELECT *,
  ROW_NUMBER() OVER(
  PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
 FROM layoffs_staging22;
 
 WITH duplicate_cte AS 
 (
 SELECT *,
  ROW_NUMBER() OVER(
  PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`,
  stage, country, funds_raised_millions) AS row_num
 FROM layoffs_staging22
 )
 
 SELECT * 
 FROM duplicate_cte 
 WHERE row_num > 1; 
 -- we will add a row number so that we can identify duplicates
 
 CREATE TABLE `layoffs_staging3` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` date DEFAULT NULL,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL, 
  `row_num` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

 SELECT*
 FROM layoffs_staging3
 ;
 
 INSERT INTO layoffs_staging3
SELECT *,
  ROW_NUMBER() OVER(
  PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`,
  stage, country, funds_raised_millions) AS row_num
 FROM layoffs_staging22; 
 
 SELECT * 
 FROM layoffs_staging3 
 WHERE row_num > 1
 ;
-- Confirmed Duplicates 

DELETE
FROM layoffs_staging3 
WHERE row_num > 1
; 
-- Deleted Duplicates

SELECT * 
FROM layoffs_staging3 
;

ALTER TABLE layoffs_staging3 
DROP COLUMN row_num; 



