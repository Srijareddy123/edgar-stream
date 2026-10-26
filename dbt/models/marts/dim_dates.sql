-- Calendar covering every filed and period date seen in the data.
with bounds as (
    select
        least(min(filed_date), min(period)) as lo,
        greatest(max(filed_date), max(period)) as hi
    from {{ source('staging', 'stg_filings') }}
)
select
    d::date as date_key,
    year(d) as year,
    quarter(d) as quarter,
    month(d) as month,
    dayname(d) as day_name,
    isodow(d) in (6, 7) as is_weekend
from bounds, range(lo, hi + interval 1 day, interval 1 day) as t(d)
