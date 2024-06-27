-- EXPLORATORY DATA ANALYSIS PORTFOLIO PROJECT WITH MYSQL

select *
from layoffs_staging2;

select *
from layoffs_staging2;

-- Basic explorations on our dataset
-- 1
select max(total_laid_off), max(percentage_laid_off)
from layoffs_staging2;
/* This highlights the peak number of individuals laid off in a single day, with 12,000 employees affected in a single event. */

select company, sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc;
/* This provides the total number of layoffs by company, ranked from highest to lowest. */


select min(`date`), max(`date`)
from layoffs_staging2;
# this tells the period which all this layoffs took place (from 2022-dec to 2023-march (spanning 3 months)

select industry, sum(total_laid_off)
from layoffs_staging2
group by industry
order by 2 desc;
/* This indicates the timeframe during which these layoffs occurred, spanning from December 2022 to March 2023, 
covering a period of three months. */

select country, sum(total_laid_off)
from layoffs_staging2
group by country
order by 2 desc;
/* According to the dataset, the United States experienced the highest number of layoffs, 
totaling 94,985k within a span of three months */

select year(`date`), sum(total_laid_off)
from layoffs_staging2
group by year(`date`)
order by 1 desc;
/* In 2023, layoffs were notably higher, based on our dataset spanning from December 2022 to March 2023. */

select stage, sum(total_laid_off)
from layoffs_staging2
group by stage
order by 2 desc;
/* The majority of companies affected by the layoffs were in the 'Post-IPO stage,' 
resulting in a total of 101,614 employees laid off across all companies. */

-- rolling sums: the progression or regression of layoffs
select *
from layoffs_staging2;

select substring(`date`, 1, 7) as `month`
from layoffs_staging2;
# 

select substring(`date`, 1, 7) as `month`, sum(total_laid_off)
from layoffs_staging2
where substring(`date`, 1, 7) is not null
group by `month`
order by 1 asc;
# majority of the layoffs happened in february 2023

with rolling_total as
(
select substring(`date`, 1, 7) as `month`, sum(total_laid_off) as total_off
from layoffs_staging2
where substring(`date`, 1, 7) is not null
group by `month`
order by 1 asc
)
select `month`, total_off, sum(total_off) over (order by `month`) as rolling_total
from rolling_total;
/* This provides a cumulative total from December 2022 to March 2023. */


/* To determine which company had the highest number of layoffs per year. */
select company, sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc;

select company, `date`, sum(total_laid_off)
from layoffs_staging2
group by company, `date`
order by `date` asc;

select company, `date`, sum(total_laid_off)
from layoffs_staging2
group by company, `date`
order by 3 desc;
/* Major companies such as Microsoft, Google, Ericsson, and Amazon laid off a significant number of employees. */


with company_year (company, years, total_laid_off) as
(
select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, year(`date`)
)
select *
from company_year;

with company_year (company, years, total_laid_off) as
(
select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, year(`date`)
)
select *, dense_rank() over(partition by years order by total_laid_off desc)	
from company_year;
/* We removed null values to ensure our rankings are accurate and unaffected. */

with company_year (company, years, total_laid_off) as
(
select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, year(`date`)
)
select *, dense_rank() over(partition by years order by total_laid_off desc) as ranking	
from company_year
where years is not null
order by ranking asc;

with company_year (company, years, total_laid_off) as
(
select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, year(`date`) -- 1st CTE (company_year)
), company_year_rank as        -- ranked it
(select *, dense_rank() over(partition by years order by total_laid_off desc) as ranking	
from company_year
where years is not null)       -- 2nd CTE
select * 
from company_year_rank;
/* In 2022, Playtika, Doma, and Pluralsight had the highest number of employee layoffs. 
In 2023, Google, Microsoft, and Ericsson led in layoffs. */

/*END*/
