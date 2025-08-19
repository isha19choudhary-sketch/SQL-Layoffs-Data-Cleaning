-- --------------------------------------------------
-- SQL Project: Data Cleaning on Layoffs Dataset
-- Author: Isha Choudhary
-- Goal: Clean and standardize a layoffs dataset to make it ready for analysis
-- --------------------------------------------------
-----data cleaning---

select *
from layoffs;

--1. remove duplicates 
--2, standardise data
--3. null values or blank values 
--4. remove any columns 

- STEP 0: Setup staging table to preserve raw data
use world_layoffs;

create table layoffs_staging
like layoffs;

select *
 from  layoffs_staging;
 
 insert layoffs_staging
 select * 
 from layoffs;
 
 -- --------------------------------------------------
-- STEP 1: Remove Duplicates
-- Use ROW_NUMBER() to identify and keep only the first record of duplicates
-- --------------------------------------------------
 
 select * ,
 row_number ()  over(
 partition by company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) as row_num
 from layoffs_staging;
 
 with duplicate_cte as 
 (
 select * ,
 row_number ()  over(
 partition by company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) as row_num
 from layoffs_staging
 )
 select * 
 from duplicate_cte 
 where row_num> 1;
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
  row_num int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_c

select * 
from layoffs_staging2
 
insert into layoffs_staging2
select * ,
 row_number ()  over(
 partition by company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) as row_num
 from layoffs_staging;

select * 
from layoffs_staging2

select * 
from layoffs_staging2
where row_num > 1;
 
 
delete 
from layoffs_staging2
where row_num > 1;

-- --------------------------------------------------
-- STEP 2: Standardize Text Fields (Company, Industry, Country)
-- --------------------------------------------------

-- Remove extra spaces in company names
 
 select company , (trim(company))
 from layoffs_staging2;
  
 update layoffs_staging2
 set company = trim(company);
 
 -- Standardize industry names (e.g., all variations of crypto → 'Crypto')
 
 select distinct  industry 
 from layoffs_staging2
 order by 1;
 
 select * 
 from layoffs_staging2 
 where industry like 'crypto%';
 
 update layoffs_staging2
 set industry = 'crypto'
  where industry like 'crypto%';
 
 -- Fix inconsistent country names (e.g., 'United States.')
 
 select distinct country, trim(trailing '.' from country)
 from layoffs_staging2
 order by 1;
 
 update layoffs_staging2
 set country = trim(trailing '.' from country)
 where country like 'united states%' ;
 
 -- --------------------------------------------------
-- STEP 3: Convert Date Column into SQL DATE Format
-- --------------------------------------------------

 select date,
STR_TO_DATE(date,'%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set date =STR_TO_DATE(date,'%m/%d/%Y')

select date
from layoffs_staging2;

-- --------------------------------------------------
-- STEP 4: Handle Null and Blank Values
-- ------------------------------------------------

select * 
 from layoffs_staging2
 where total_laid_off is null
 and percentage_laid_off is null;
 
 -- Convert blank industries to NULL
 update layoffs_staging2
 set industry = null 
 where industry = '';
 
 select distinct  industry 
 from layoffs_staging2 
where industry is null
or industry ='';

select *
 from layoffs_staging2 
where company ='airbnb';


select * 
from layoffs_staging2 t1
JOIN layoffs_staging2 t2
on t1.company = t2.company 
and t1.location = t2.location
where t1.industry is null 
and t2.industry is not null

-- Fill missing industries using non-null values from the same company/location
UPDATE layoffs_staging2 t1
 JOIN layoffs_staging2 t2
on t1.company = t2.company 
set t1.industry = t2.industry 
where (t1.industry is null or t1.industry = '')
and t2.industry is not null

select * 
from layoffs_staging2;

delete 
from layoffs_staging2 a
where total_laid_off is null 
and percentage_laid_off is null;

alter table layoffs_staging2 
drop column row_num;

-- Verify final cleaned data
SELECT * FROM layoffs_staging2 LIMIT 20;
-- --------------------------------------------------
-- END RESULT:
-- ✅ Duplicate-free
-- ✅ Standardized text fields
-- ✅ Proper DATE format
-- ✅ Nulls handled
-- ✅ Analysis-ready dataset
-- --------------------------------------------------

---exploring data analysis---

select * 
from layoffs_staging2;

-- 1. Maximum layoffs & layoff percentage
select max(total_laid_off), max(percentage_laid_off)
from layoffs_staging2;

-- 2. Companies with 100% layoffs
select * 
from layoffs_staging2
 where percentage_laid_off = 1
 order by funds_raised_millions desc;
 
 -- 3. Total layoffs by company
 select company, sum(total_laid_off)
from layoffs_staging2
group by company 
order by 2 desc;

-- 4. Total layoffs by industry
select industry , sum(total_laid_off)
from layoffs_staging2
group by industry 
order by 2 desc;

-- 5. Total layoffs by country
select country  , sum(total_laid_off)
from layoffs_staging2
group by country
order by 2 desc;

-- 6. Total layoffs by date
select date  , sum(total_laid_off)
from layoffs_staging2
group by date
order by 1 desc;

-- 7. Total layoffs by stage
select stage  , sum(total_laid_off)
from layoffs_staging2
group by stage
order by 2 desc;

-- 8. Monthly layoffs trend
select substring(date , 1,7) as month , sum(total_laid_off)
from layoffs_staging2
where substring(date , 1,7) IS NOT NULL
group by month
ORDER by 1 asc
;

-- 9. Rolling total of layoffs over time
with rolling_total as 
(
select substring(date , 1,7) as month , sum(total_laid_off) as total_off
from layoffs_staging2
where substring(date , 1,7) IS NOT NULL
group by month
ORDER by 1 asc
)
select month , total_off
, sum(total_off) over(order by month) as rolling_total
from rolling_total;



-- Final dataset for analysis
select * 
from layoffs_staging2;