-- ============================================================
--                  EXPLORATORY DATA ANALYSIS (EDA)
-- ============================================================

-- View cleaned dataset
SELECT *
FROM layoffs_staging2;


-- Inspect max layoffs and % layoffs

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;


-- Companies with full layoffs (percentage_laid_off = 1)
-- Ordered by most funds raised
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;


-- Total layoffs per company
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;


-- Date range of the dataset
SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;


-- Total layoffs by industry
SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;


-- Total layoffs by country
SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;


-- Re-check dataset
SELECT *
FROM layoffs_staging2;


-- Yearly layoffs trends
SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;


-- Layoffs by funding stage (Seed, Series A, Series B, etc.)
SELECT stage , SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;


-- Sum of layoff percentages per company
-- (Not as meaningful as total_laid_off but still exploratory)
SELECT company , SUM(percentage_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;


-- Monthly layoffs (YYYY-MM)
SELECT SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;


-- Monthly rolling total of layoffs

WITH ROLLING_TOTAL AS (
    SELECT SUBSTRING(`date`,1,7) AS `MONTH`,
           SUM(total_laid_off) AS total_off
    FROM layoffs_staging2
    WHERE SUBSTRING(`date`,1,7) IS NOT NULL
    GROUP BY `MONTH`
    ORDER BY 1 ASC
)
SELECT `MONTH`,
       total_off,
       SUM(total_off) OVER(ORDER BY `MONTH`) AS rolling_total
FROM ROLLING_TOTAL;


-- Total layoffs by company (duplicate of earlier query but included for workflow)
SELECT company , SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;


-- Yearly layoffs per company
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;


-- Top 5 companies per year (ranked by layoffs)
WITH Company_Year(company, years, total_laid_off) AS (
    SELECT company,
           YEAR(`date`) AS years,
           SUM(total_laid_off)
    FROM layoffs_staging2
    GROUP BY company, YEAR(`date`)
),
Company_Year_Rank AS (
    SELECT *,
           DENSE_RANK() OVER(
               PARTITION BY years
               ORDER BY total_laid_off DESC
           ) AS Ranking
    FROM Company_Year
    WHERE years IS NOT NULL
)
SELECT *
FROM Company_Year_Rank
WHERE Ranking <= 5;
