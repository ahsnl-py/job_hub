select  currency,
        case when currency = 'EUR' then 'Euro'
            when currency = 'CZK' then 'Czech Koruna'
        end as currency_desc,
        {{ dbt_utils.generate_surrogate_key(['currency']) }} as currency_id
from (
    select
        distinct currency
    from {{ ref('jobpost') }}
)