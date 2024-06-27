-- DATA CLEANING PORTFOLIO PROJECT WITH MYSQL

-- CREATE SCHEMA/TABLE
/*
 CREATE TABLE world_layoffs.layoffs (
    company TEXT,
    location TEXT,
    industry TEXT,
    total_laid_off INT,
    percentage_laid_off TEXT,
    `date` TEXT,
    stage TEXT,
    country TEXT,
    funds_raised_millions INT
);

##OR##   

-- CREATE SCHEMA/TABLE ( STATING DATASET COULD HAVE NULL VALUES )

CREATE TABLE world_layoffs.layoffs (
    company TEXT NULL,
    location TEXT NULL,
    industry TEXT NULL,
    total_laid_off INT NULL,
    percentage_laid_off TEXT NULL,
    `date` TEXT NULL,
    stage TEXT NULL,
    country TEXT NULL,
    funds_raised_millions INT NULL
);
*/
-- 2 LOAD THE TABLE WITH THE CSV FILE
-- https://www.kaggle.com/datasets/swaptr/layoffs-2022 

-- 3
### View the Loaded Dataset 
select *
from layoffs;

###############################################################################################
-- STEPS --
-- 0
/* Generate a table that mirrors the loaded dataset, serving as a duplicate or backup. */
create table layoffs_staging
like layoffs;

/* Examine the table to ensure the integrity of its row and column structure. */
select *
from layoffs_staging;

/* Populate the table with the data */
insert layoffs_staging
select *
from layoffs;
/* NB: the point of making a duplicate table is because we are going to be making changes and
alterations to the table and if we make a mistake we need to be able to refer back to original
data*/  
  
  
-- 1
/* Remove Duplicate

-- 2
/* Standardize the Data: Spellings/white-spaces/punctuation */

-- 3
/* Null Values / Blank values: View them and Try to populate them

-- 4 
/* Remove rows and columns that aren't necessary*/



-- 1
/* Remove Duplicates

/* i. Label each row with the number 1 and apply it to all rows in the table, partitioning by all columns. */ 
select *
from layoffs_staging;

select *, 
row_number() over(
partition by company, industry, total_laid_off, percentage_laid_off, `date`) as row_num
from layoffs_staging;
	/* date is enclosed in backticks(``) because it is a reserved keyword in MySql */

/* Create a CTE(Common Table Expression) 
A Common Table Expression (CTE) is a temporary result set in SQL that you can reference within a 
`SELECT`, `INSERT`, `UPDATE`, or `DELETE` statement. 
It is defined using the `WITH` keyword, followed by a name and a query. 
CTEs are particularly useful for simplifying complex queries, improving readability, and breaking down queries into modular, understandable parts.

### Syntax
WITH cte_name AS (
    -- Your SQL query here
)
SELECT * FROM cte_name;

### Example
Here’s an example that uses a CTE to select from tables: employees(e) and their departments(d):
WITH EmployeeDepartment AS 
(
    SELECT e.EmployeeID, e.Name, d.DepartmentName
    FROM Employees e
    JOIN Departments d ON e.DepartmentID = d.DepartmentID
)
SELECT * FROM EmployeeDepartment;

In this example, `EmployeeDepartment` is the CTE that selects employees and their department names. The final `SELECT` statement then uses this CTE. */

with duplicate_cte as
(
select *, 
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num > 1;
# we dont have duplicates in this dataset

# another way to spot the duplicate rows

SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;

/* To handle duplicates, we will need to create a similar table and add row_num column to assist in systematically removing them */
/* ii. Create a new table with a new column row_num */
CREATE TABLE `world_layoffs`.`layoffs_staging2` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int,
`row_num` INT
);

select *
from layoffs_staging2;

/* insert #i. into our new table */
insert into layoffs_staging2
select *, 
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;

delete
from layoffs_staging2
where row_num >= 2;

-- 2 
/* standardizing the data */
/* its best practice to do this column by column */
/* column 1: Company */
select *
from layoffs_staging2; 

select company, trim(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);
# trim eliminates the leading or trailing whitespaces around your characters

/* column 3: Industry */
select industry
from layoffs_staging2;
# observe that there are numerous industries, with some appearing multiple times

select distinct industry
from layoffs_staging2
order by 1;
# The output reveals that there are just 29 distinct industries. Additionally, we need to standardize this column due to the presence of nulls and blanks."

/* In cases where you have industry names that are similar but spelled differently, you can standardize them. 
In this instance we will correct Crypo */

select *
from layoffs_staging2
where industry like 'Crypto%';

/* In our current dataset, we do not encounter issues with inconsistent industry names. 
However, for anyone facing such challenges, the following steps outline how to standardize industry names 
that are the same or similar but spelled differently e.g., "crypto" and "cryptocurrency" */
update layoffs_staging2
set industry = 'crypto'
where industry like 'Crypto%';

/*column 2: locatio*/
 select distinct location
 from layoffs_staging2
 order by 1;
 # this column seems fine
 
 /* column 4: country*/
 select distinct country
 from layoffs_staging2
 order by 1;
 /* This column requires standardization and correction due to entries such as 
'united states' and 'united states.' that need consistent formatting. */
 
 select *
 from layoffs_staging2
 where country like 'united states%'
 order by 1;

select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'united states%';

select *
from layoffs_staging2;
# looks good now

/* column 6: Date*/
/* Observe that the date column is currently of type 'text,' which is unsuitable for operations like time-series. 
The next step is to convert the column to a 'date' type. */
select `date`
from layoffs_staging2;
# observe that the format of the date isn't in mm-dd-yy.

select `date`, str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;
# this syntax changes our date format to the standard YYYY-MM-DD date.

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

select `date`
from layoffs_staging2;

# The date column is still set to 'text,' so we need to modify it to the 'date' type."
alter table layoffs_staging2
modify column `date` date;

select *
from layoffs_staging2;

-- 3
/* Null Values & Blank Values */
/* null values and blank values were spotted in several columns like industry */

select distinct industry
from layoffs_staging2;
# there seems to be blank spaces in the industry column

select *
from layoffs_staging2
where industry is null
or industry = '';
/* lets try to populate them */

select *
from layoffs_staging2
where company = 'Airbnb';

/* Since Airbnb in the travel industry, we can fill the blank spaces with the word 'Travel' */
update layoffs_staging2
set industry = 'Travel'
where company = 'Airbnb'
and industry = '';
# the company Airbnb has its industry updated with 'Travel'

select *
from layoffs_staging2
where industry is null
or industry = '';

/* The industry field for Bally's Interactive is currently null. Given that it is a betting company, we will update the industry to 'Betting. */

update layoffs_staging2
set industry = 'Betting'
where company like 'Bally%'
and industry is null;

select *
from layoffs_staging2;
/*it seems all cells in industry column are now populated */

# the columns total laid off and percentage laid off have null values
select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;
/* Some rows contain null values in both the 'total_laid_off' and 'percentage_laid_off' columns, 
rendering those rows insignificant to our analysis. Therefore, for the 'total_laid_off' and 'percentage_laid_off' columns, 
we will either populate known string values or delete/ignore numeric entries. 
Since we lack sufficient data to compute accurate values for these columns, we will proceed by removing them entirely, affecting 84 rows. */

delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

/* We will now dleete the column row_num, its redundant we do not need it anymore*/
alter table layoffs_staging2
drop column row_num;

select *
from layoffs_staging2;

/* Our data is cleaned and ready for exploration.
HURRAY!!!!! */