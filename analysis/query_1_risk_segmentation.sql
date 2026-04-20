with borrower_risk as (
    select
        borrower_id,
        income,
        total_credit_amount,
        total_overdue_amount,
        previous_loan_count,
        target,
        ntile(4) over (order by income) as income_quartile
    from analytics.fact_loan_applications
)
select
    income_quartile,
    count(*) as total_applications,
    avg(target::numeric) as default_rate,
    avg(total_credit_amount) as avg_credit_exposure,
    avg(total_overdue_amount) as avg_overdue_amount,
    avg(previous_loan_count) as avg_previous_loans
from borrower_risk
group by income_quartile
order by income_quartile;