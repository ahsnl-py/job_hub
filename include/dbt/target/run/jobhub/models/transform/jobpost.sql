

  create or replace view `job-hub-bucket`.`jobhub`.`jobpost`
  OPTIONS()
  as with cte_jobpost as (
  SELECT load_date, job_id, title, company, city, link, work_from_home,
          salary as salary_range, rating, response_period,
          trim(SPLIT(salary, '–‍')[offset(0)]) as salary_lower_r,
          trim(IF(array_length(SPLIT(salary, '–‍')) > 1, SPLIT(salary, '–‍')[offset(1)], '0')) as salary_higher_r,
          case when contains_substr(salary, 'CZK') then trim(substring(salary, strpos(salary, 'CZK')))
               when contains_substr(salary, 'EUR') then trim(substring(salary, strpos(salary, 'EUR')))
               else '' end as currency,
          if (district = 'N/A', '', district) as district
  from `job-hub-bucket`.`jobhub`.`stg_jobpost`
  where salary != 'N/A'
)
select m.load_date, m.job_id, m.title, m.company, m.city, m.district,
      trim(replace(m.salary_lower_r, m.currency, '')) as salary_lower_r,
      trim(replace(m.salary_higher_r, m.currency, '')) as salary_higher_r,
      m.currency,
      m.link as url,
      if (m.rating = 'N/A', '', m.rating) as company_rating,
      cast( if (m.response_period = 'Response within 2 weeks', 0, 1) as bool ) as is_fast_response,
      cast( if (m.work_from_home = 'N/A', 0, 1) as bool ) as is_remote
from cte_jobpost m
order by m.salary_higher_r desc, m.currency;

