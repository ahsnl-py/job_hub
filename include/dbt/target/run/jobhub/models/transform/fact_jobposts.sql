

  create or replace view `job-hub-bucket`.`jobhub`.`fact_jobposts`
  OPTIONS()
  as select t.city_id, t.currency_id, 
  max(salary) as max_salary_city,
  sum(salary) as total_salary_city,
  min(salary) as min_salary_city
from (
  SELECT jp.load_date, dc.city_id, dcy.currency_id, 
            CASE WHEN jp.salary_higher_r != '0' then  
                CAST(REPLACE(REGEXP_EXTRACT(jp.salary_higher_r, r'\d+\,\d*'), ',', '') AS INT64)
               else 0 end as salary
  FROM `job-hub-bucket`.`jobhub`.`jobpost` jp
  INNER JOIN `job-hub-bucket`.`jobhub`.`dim_city` dc 
    on jp.city = dc.city
  INNER JOIN `job-hub-bucket`.`jobhub`.`dim_currency` dcy
    on jp.currency = dcy.currency
  where jp.salary_higher_r != '' or jp.salary_higher_r is not null
) t
group by t.load_date, t.city_id, t.currency_id;

