select
    borrower_id,
    count(*) as previous_loan_count,
    avg(loan_amount) as avg_previous_loan_amount,
    sum(loan_amount) as total_previous_loan_amount
from {{ ref('stg_previous_loans') }}
group by borrower_id