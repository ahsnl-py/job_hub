

  create or replace view `job-hub-bucket`.`jobhub`.`dim_currency`
  OPTIONS()
  as select  currency,
        case when currency = 'EUR' then 'Euro'
            when currency = 'CZK' then 'Czech Koruna'
        end as currency_desc,
        to_hex(md5(cast(coalesce(cast(currency as STRING), '_dbt_utils_surrogate_key_null_') as STRING))) as currency_id
from (
    select
        distinct currency
    from `job-hub-bucket`.`jobhub`.`jobpost`
);

