select
    la.application_id,
    la.borrower_id,
    la.loan_amount,
    la.target,
    b.income,
    b.education,
    b.family_status,
    e.occupation_type,
    e.years_employed,
    coalesce(cp.total_bureau_records, 0) as total_bureau_records,
    coalesce(cp.total_credit_amount, 0) as total_credit_amount,
    coalesce(cp.total_overdue_amount, 0) as total_overdue_amount,
    coalesce(cp.active_credit_count, 0) as active_credit_count,
    coalesce(pls.previous_loan_count, 0) as previous_loan_count,
    coalesce(pls.avg_previous_loan_amount, 0) as avg_previous_loan_amount,
    coalesce(pls.total_previous_loan_amount, 0) as total_previous_loan_amount
from {{ ref('stg_loan_applications') }} la
left join {{ ref('dim_borrowers') }} b
    on la.borrower_id = b.borrower_id
left join {{ ref('dim_employment') }} e
    on la.borrower_id = e.borrower_id
left join {{ ref('dim_credit_profile') }} cp
    on la.borrower_id = cp.borrower_id
left join {{ ref('dim_previous_loan_summary') }} pls
    on la.borrower_id = pls.borrower_id