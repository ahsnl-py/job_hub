

  create or replace view `job-hub-bucket`.`jobhub`.`dim_city`
  OPTIONS()
  as with cte_cities as (
  select distinct
    city, substring(city, 0, strpos(city, ' +')) as city_c,
    contains_substr(city, '+') as is_city_symbol
  from `job-hub-bucket`.`jobhub`.`stg_jobpost`
),
cte_cities_t as (
  select trim(if( is_city_symbol, city_c, city)) as city
  from cte_cities
)
select t.city, to_hex(md5(cast(coalesce(cast(city as STRING), '_dbt_utils_surrogate_key_null_') as STRING))) as city_id
from (select distinct city from cte_cities_t) t;

