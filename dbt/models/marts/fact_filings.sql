-- One row per stored filing version. is_latest marks the version that currently stands.
select
    md5(f.accession || ':' || f.version) as filing_sk,
    f.accession,
    f.version,
    f.cik as company_key,
    f.form,
    f.filed_date as filed_date_key,
    f.period as period_date_key,
    f.acceptance_ts,
    c.accession = f.accession and c.version = f.version as is_latest,
    f.n_sections,
    f.n_chars,
    f.truncated,
    f.raw_bytes,
    f.parser_version
from {{ source('staging', 'stg_filings') }} f
join {{ source('staging', 'filing_current') }} c using (filing_key)
