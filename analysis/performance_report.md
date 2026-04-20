# Performance Tuning Report

## Query Selected

The most complex analytical query selected for performance profiling was:

`query_3_payment_behavior.sql`

This query was chosen because it includes:
- a Common Table Expression (CTE)
- a join between installments and previous loans
- multiple window functions: `AVG()` and `ROW_NUMBER()`
- ranking, sorting, and filtering logic

---

## Baseline Performance (Before Indexing)

The query was first profiled using `EXPLAIN ANALYZE`.

### Observations
- Execution time before indexing: **3s 15.5ms**
- The execution plan showed:
  - `Parallel Seq Scan` on `installments`
  - `Parallel Seq Scan` on `previous_loans`
  - `Parallel Hash Join`
  - sort and window aggregation steps

This indicates that PostgreSQL relied mainly on sequential scans for reading large tables before performing the join and ranking operations.

---

## Indexes Added

To improve join performance and borrower-level lookup efficiency, the following indexes were added:

```sql
CREATE INDEX IF NOT EXISTS idx_previous_loans_previous_loan_id
ON public.previous_loans(previous_loan_id);

CREATE INDEX IF NOT EXISTS idx_installments_previous_loan_id
ON public.installments(previous_loan_id);

CREATE INDEX IF NOT EXISTS idx_previous_loans_borrower_id
ON public.previous_loans(borrower_id);