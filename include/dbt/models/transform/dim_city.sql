with cte_cities as (
  select distinct
    city, substring(city, 0, strpos(city, ' +')) as city_c,
    contains_substr(city, '+') as is_city_symbol
  from {{ source('jobhub', 'stg_jobpost') }}
),
cte_cities_t as (
  select trim(if( is_city_symbol, city_c, city)) as city
  from cte_cities
)
select t.city, {{ dbt_utils.generate_surrogate_key(['city']) }} as city_id
from (select distinct city from cte_cities_t) t 