select
    borrower_id,
    count(*) as total_bureau_records,
    sum(credit_amount) as total_credit_amount,
    sum(overdue_amount) as total_overdue_amount,
    sum(case when credit_active = 'Active' then 1 else 0 end) as active_credit_count
from {{ ref('stg_bureau_records') }}
group by borrower_id