select
    borrower_id,
    occupation_type,
    years_employed
from {{ ref('stg_employment') }}