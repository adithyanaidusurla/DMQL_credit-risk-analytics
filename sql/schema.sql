-- =========================
-- CREDIT RISK DATABASE SCHEMA (Optimized)
-- =========================

-- Drop tables if re-running (development only)
DROP TABLE IF EXISTS installments CASCADE;
DROP TABLE IF EXISTS previous_loans CASCADE;
DROP TABLE IF EXISTS bureau_records CASCADE;
DROP TABLE IF EXISTS loan_applications CASCADE;
DROP TABLE IF EXISTS employment CASCADE;
DROP TABLE IF EXISTS borrowers CASCADE;

-- =========================
-- BORROWERS TABLE
-- =========================
CREATE TABLE borrowers (
    borrower_id BIGINT PRIMARY KEY,
    gender TEXT CHECK (gender IN ('M', 'F', 'XNA')),
    income DECIMAL(15,2) CHECK (income >= 0),
    education TEXT,
    family_status TEXT
);

-- =========================
-- EMPLOYMENT TABLE
-- =========================
CREATE TABLE employment (
    employment_id SERIAL PRIMARY KEY,
    borrower_id BIGINT NOT NULL,
    years_employed INT CHECK (years_employed >= 0),
    occupation_type TEXT,
    FOREIGN KEY (borrower_id) REFERENCES borrowers(borrower_id)
);

-- =========================
-- LOAN APPLICATIONS TABLE
-- =========================
CREATE TABLE loan_applications (
    application_id SERIAL PRIMARY KEY,
    borrower_id BIGINT NOT NULL,
    loan_amount DECIMAL(15,2) CHECK (loan_amount >= 0),
    annuity DECIMAL(15,2),
    credit_term INT,
    target INT CHECK (target IN (0,1)),
    FOREIGN KEY (borrower_id) REFERENCES borrowers(borrower_id)
);

-- =========================
-- BUREAU RECORDS TABLE
-- =========================
CREATE TABLE bureau_records (
    bureau_id SERIAL PRIMARY KEY,
    borrower_id BIGINT NOT NULL,
    credit_active TEXT,
    credit_type TEXT,
    credit_amount DECIMAL(15,2),
    overdue_amount DECIMAL(15,2),
    FOREIGN KEY (borrower_id) REFERENCES borrowers(borrower_id)
);

-- =========================
-- PREVIOUS LOANS TABLE
-- =========================
CREATE TABLE previous_loans (
    previous_loan_id BIGINT PRIMARY KEY,
    borrower_id BIGINT NOT NULL,
    loan_amount DECIMAL(15,2),
    contract_status TEXT,
    loan_type TEXT,
    FOREIGN KEY (borrower_id) REFERENCES borrowers(borrower_id)
);

-- =========================
-- INSTALLMENTS TABLE
-- =========================
CREATE TABLE installments (
    installment_id SERIAL PRIMARY KEY,
    previous_loan_id BIGINT NOT NULL,
    installment_amount DECIMAL(15,2),
    payment_amount DECIMAL(15,2),
    days_late INT,
    FOREIGN KEY (previous_loan_id) REFERENCES previous_loans(previous_loan_id)
);

-- =========================
-- INDEXES (for performance)
-- =========================
CREATE INDEX idx_borrower_id ON loan_applications(borrower_id);
CREATE INDEX idx_prev_loan_borrower ON previous_loans(borrower_id);
CREATE INDEX idx_installments_prev ON installments(previous_loan_id);