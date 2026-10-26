select
    md5(s.accession || ':' || s.version) as filing_sk,
    s.item,
    s.ord,
    s.title,
    s.n_chars,
    s.text
from {{ source('staging', 'stg_sections') }} s
