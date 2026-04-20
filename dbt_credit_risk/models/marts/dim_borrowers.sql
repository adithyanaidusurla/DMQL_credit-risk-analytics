select
    borrower_id,
    gender,
    education,
    family_status,
    income
from {{ ref('stg_borrowers') }}