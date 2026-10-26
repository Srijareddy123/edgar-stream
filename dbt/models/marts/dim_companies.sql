-- One row per company. Name and attributes come from its most recent filing.
with ranked as (
    select
        cik, company, sic, state_inc, filed_date,
        row_number() over (partition by cik order by filed_date desc, order_key desc) as rn
    from {{ source('staging', 'stg_filings') }}
),
agg as (
    select cik, min(filed_date) as first_filed_date, count(*) as n_filings_loaded
    from {{ source('staging', 'stg_filings') }}
    group by cik
)
select
    r.cik as company_key,
    r.company as company_name,
    r.sic,
    r.state_inc,
    a.first_filed_date,
    a.n_filings_loaded
from ranked r
join agg a using (cik)
where r.rn = 1
