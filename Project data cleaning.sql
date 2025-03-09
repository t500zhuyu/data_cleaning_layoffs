
-- Data cleaning

select * from layoffs;

# create a copy of the original table
create table layoffs_staging
like layoffs;

# populate the table with the information
# of the original table
insert layoffs_staging
select * from layoffs;

select * from layoffs_staging;


-- 1° Remove duplicates values

# identify the duplicate values, use all the usuful columns to check well
with duplicate_cte as
(
select *,
row_number() over (
partition by company, location, industry, total_laid_off,
percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
select * from duplicate_cte
where row_num >1; #if there are values greater than 1, 
	# then those are duplicated values

# Double check with the duplicates
select * from layoffs_staging
where company = 'Casper';

# 

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

select * from layoffs_staging2;

INSERT INTO layoffs_staging2
select *,
row_number() over (
partition by company, location, industry, total_laid_off,
percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;

# just select those that are bigger than 1, (duplicates values)
select * from layoffs_staging2
where row_num >1;

SET SQL_SAFE_UPDATES = 0;

# removing the duplicates
DELETE
from layoffs_staging2
where row_num >1;

select *
from layoffs_staging2;

-- 2° Standardize the data

# remove the spaces in the rows
select company, trim(company)
from layoffs_staging2;

# make this change in the table update
UPDATE layoffs_staging2
set company = trim(company);

# check the distinct rows values
select distinct industry
from layoffs_staging2
order by 1;

# update crypto info the are 3 different but is the same
select * from layoffs_staging2
where industry like 'Crypto%';

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

select distinct industry
from layoffs_staging2;

# now checking the location
select distinct location
from layoffs_staging2
order by 1;

select distinct country
from layoffs_staging2
order by 1;

# check usa
select *
from layoffs_staging2
where country like 'United States%'
order by 1;

# put out the . in United States
select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';


# change column date from text to date format

select `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;


update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

select `date`
from layoffs_staging2;

# changing the data format
alter table layoffs_staging2
modify column `date` DATE;

select * from layoffs_staging2;

-- 3° Null values or blank values

# check the null values in the columns
select * from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;


select *
from layoffs_staging2
where industry is null
or industry = '';

select *
from layoffs_staging2
where company ='Airbnb';

# populate blank Airbnb
select * 
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
    and t1.location = t2.location
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;


select t1.industry, t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
    and t1.location = t2.location
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;


# change blanks rows for null ones
update layoffs_staging2
set industry = null
where industry = '';

select *
from layoffs_staging2
where company like 'Bally%';

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

# removing null values for these columns
# total_laid_off
# percentage_laid_off
delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select * from layoffs_staging2;

-- 4° Remove any columns

# drop columns
alter table layoffs_staging2
drop column row_num;

select * from layoffs_staging2;

-- Exploratory analysis


select max(total_laid_off), max(percentage_laid_off)
from layoffs_staging2;

select * from layoffs_staging2
where percentage_laid_off =1
order by funds_raised_millions desc;

select company, sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc;

# first date and last date
select min(`date`), max(`date`)
from layoffs_staging2;

# sum by industry total laid off
select industry, sum(total_laid_off)
from layoffs_staging2
group by industry
order by 2 desc;

select * from layoffs_staging2;

select country, sum(total_laid_off)
from layoffs_staging2
group by country
order by 2 desc;


select YEAR(`date`), sum(total_laid_off)
from layoffs_staging2
group by YEAR(`date`)
order by 1 desc;

select stage, sum(total_laid_off)
from layoffs_staging2
group by stage
order by 2 desc;

select company, avg(percentage_laid_off)
from layoffs_staging2
group by company
order by 2 desc;

# # getting the month
select substring(`date`, 1,7) as `Month`, sum(total_laid_off)
from layoffs_staging2
where substring(`date`, 1,7) is not null
group by `Month`
order by 1 asc;

with Rolling_total as
(
select substring(`date`, 1,7) as `Month`, sum(total_laid_off) as total_off
from layoffs_staging2
where substring(`date`, 1,7) is not null
group by `Month`
order by 1 asc
)
select `Month`, total_off
 ,sum(total_off) over(order by `Month`) as Rolling_total
from Rolling_total;


select company, sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc;

select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company,  year(`date`)
order by 3 desc;

with company_year (company, years,total_laid_off) as
(
select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company,  year(`date`)
), company_year_rank as
(select *, 
dense_rank() over (partition by years order by total_laid_off desc) as ranking
from company_year
where years is not null
)
select *
from company_year_rank
where ranking <= 5
;



























































































































