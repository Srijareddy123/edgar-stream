select distinct
    form,
    base_form,
    is_amendment,
    case base_form
        when '10-K' then 'annual report'
        when '10-Q' then 'quarterly report'
        when '8-K' then 'current report'
        else 'other'
    end as form_family
from {{ source('staging', 'stg_filings') }}
