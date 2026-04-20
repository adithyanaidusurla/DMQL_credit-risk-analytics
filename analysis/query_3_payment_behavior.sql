with installment_summary as (
    select
        pl.borrower_id,
        i.previous_loan_id,
        i.installment_amount,
        i.payment_amount,
        i.days_late,
        avg(i.days_late) over (
            partition by pl.borrower_id
        ) as avg_days_late_per_borrower,
        row_number() over (
            partition by pl.borrower_id
            order by i.days_late desc
        ) as lateness_rank
    from analytics.stg_installments i
    join analytics.stg_previous_loans pl
        on i.previous_loan_id = pl.previous_loan_id
)
select
    borrower_id,
    previous_loan_id,
    installment_amount,
    payment_amount,
    days_late,
    avg_days_late_per_borrower,
    lateness_rank
from installment_summary
where lateness_rank <= 5
order by borrower_id, lateness_rank;