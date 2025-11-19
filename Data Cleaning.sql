-- ============================================================
--                DATA CLEANING PIPELINE
-- ============================================================

-- Inspect raw layoffs table
SELECT * 
FROM layoffs;

-- Steps:
-- 1. Remove duplicates
-- 2. Standardize date formats
-- 3. Fix NULL or blank values
-- 4. Remove unnecessary rows/columns



-- Create a staging table to avoid modifying raw data

CREATE TABLE layoffs_staging LIKE layoffs;

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT * 
FROM layoffs_staging;



-- 1. Identify duplicate rows using ROW_NUMBER()

SELECT *,
ROW_NUMBER() OVER(
    PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`
) AS row_num
FROM layoffs_staging;


-- Check duplicates with full column comparison
WITH duplicate_cte AS (
    SELECT *,
        ROW_NUMBER() OVER(
            PARTITION BY company, location, industry, total_laid_off, 
                         percentage_laid_off, `date`, stage, country, 
                         funds_raised_millions
        ) AS row_num
    FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;


-- Example check: duplicates for 'Casper'
SELECT *
FROM layoffs_staging
WHERE company = 'Casper';



-- Attempt to delete duplicates directly from CTE (not valid in MySQL)

WITH duplicate_cte AS (
    SELECT *,
        ROW_NUMBER() OVER(
            PARTITION BY company,location, industry, total_laid_off, 
                         percentage_laid_off, `date`, stage, country, 
                         funds_raised_millions
        ) AS row_num
    FROM layoffs_staging
)
DELETE
FROM duplicate_cte
WHERE row_num > 1;



-- Create a second staging table with row_num included

CREATE TABLE `layoffs_staging2` (
  `company` TEXT,
  `location` TEXT,
  `industry` TEXT,
  `total_laid_off` INT DEFAULT NULL,
  `percentage_laid_off` TEXT,
  `date` TEXT,
  `stage` TEXT,
  `country` TEXT,
  `funds_raised_millions` INT DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Insert data and compute row_num again
INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
    PARTITION BY company,location, industry, total_laid_off, 
                 percentage_laid_off, `date`, stage, country, 
                 funds_raised_millions
) AS row_num
FROM layoffs_staging;

-- Delete actual duplicates (row_num > 1)
DELETE
FROM layoffs_staging2
WHERE row_num > 1;



-- 2. STANDARDIZE & CLEAN FIELDS


-- Trim whitespace from company names
SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);


-- Inspect industries
SELECT DISTINCT industry
FROM layoffs_staging2;

-- Standardize crypto-related variations
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';


-- Clean up country names (remove trailing '.')
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';



-- Standardize date format: convert text -> DATE

SELECT `date`
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


SELECT *
FROM layoffs_staging2;



-- 3. HANDLE NULL / BLANK VALUES


-- Check for rows where both layoff fields are NULL (useless rows)
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
  AND percentage_laid_off IS NULL;


-- Convert blank industry strings -> NULL
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';


-- Check rows with missing industry data
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL OR industry = '';


-- Look at a specific company for filling missing values
SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';


-- Find companies with missing industry by matching same company/location
SELECT t1.industry, t2.industry
FROM layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
    ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
  AND t2.industry IS NOT NULL;


-- Fill missing industries using matching rows
UPDATE layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
    ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
  AND t2.industry IS NOT NULL;



-- 4. REMOVE USELESS ROWS & COLUMNS


-- Recheck rows with no layoff information
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
  AND percentage_laid_off IS NULL;


-- Delete them
DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
  AND percentage_laid_off IS NULL;


-- Drop row_num column (no longer needed)
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;
