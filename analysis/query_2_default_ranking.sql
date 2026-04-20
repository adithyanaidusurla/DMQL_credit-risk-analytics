with ranked_borrowers as (
    select
        borrower_id,
        application_id,
        loan_amount,
        income,
        target,
        total_credit_amount,
        total_overdue_amount,
        rank() over (
            partition by target
            order by total_overdue_amount desc, total_credit_amount desc
        ) as risk_rank
    from analytics.fact_loan_applications
)
select
    borrower_id,
    application_id,
    loan_amount,
    income,
    target,
    total_credit_amount,
    total_overdue_amount,
    risk_rank
from ranked_borrowers
where risk_rank <= 10
order by target desc, risk_rank asc;