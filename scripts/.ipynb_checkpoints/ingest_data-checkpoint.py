# ingest_data.py
import os
import pandas as pd
from sqlalchemy import create_engine, text

# ----------------------------
# 1. Database Connection
# ----------------------------
DATABASE_URL = os.getenv("DATABASE_URL")  # Set in environment variable

engine = create_engine(
    DATABASE_URL,
    pool_size=2,
    max_overflow=1,
    pool_pre_ping=True,
    pool_recycle=300
)

# ----------------------------
# 2. File Paths
# ----------------------------
DATA_DIR = "data"
APPLICATION_FILE = f"{DATA_DIR}/application_train.csv"
BUREAU_FILE = f"{DATA_DIR}/bureau.csv"
PREVIOUS_FILE = f"{DATA_DIR}/previous_application.csv"
INSTALLMENTS_FILE = f"{DATA_DIR}/installments_payments.csv"

# ----------------------------
# 3. Functions
# ----------------------------
def clean_application(df):
    df = df.drop_duplicates()
    df = df.fillna({"CODE_GENDER": "XNA", "AMT_INCOME_TOTAL": 0})
    df['AMT_INCOME_TOTAL'] = pd.to_numeric(df['AMT_INCOME_TOTAL'], errors='coerce').fillna(0)
    df['TARGET'] = df['TARGET'].fillna(0).astype(int)
    return df

def clean_bureau(df):
    df = df.drop_duplicates()
    df = df.fillna({"CREDIT_ACTIVE": "UNKNOWN", "CREDIT_TYPE": "UNKNOWN",
                    "AMT_CREDIT_SUM": 0, "AMT_CREDIT_SUM_OVERDUE": 0})
    return df

def clean_previous(df):
    df = df.drop_duplicates()
    df = df.fillna({"NAME_CONTRACT_STATUS": "UNKNOWN", "NAME_CONTRACT_TYPE": "UNKNOWN",
                    "AMT_CREDIT": 0})
    return df

def clean_installments(df):
    df = df.drop_duplicates()
    df = df.fillna({"AMT_INSTALMENT": 0, "AMT_PAYMENT": 0, "DAYS_ENTRY_PAYMENT": 0})
    return df

# ----------------------------
# 4. Ingestion
# ----------------------------
def ingest():
    # --- Application Table (borrowers + loan_applications) ---
    app_df = pd.read_csv(APPLICATION_FILE)
    app_df = clean_application(app_df)

    borrowers = app_df[['SK_ID_CURR', 'CODE_GENDER', 'AMT_INCOME_TOTAL', 'NAME_EDUCATION_TYPE', 'NAME_FAMILY_STATUS']]
    borrowers.columns = ['borrower_id', 'gender', 'income', 'education', 'family_status']

    loan_applications = app_df[['SK_ID_CURR', 'AMT_CREDIT', 'AMT_ANNUITY', 'CNT_CHILDREN', 'TARGET']]
    loan_applications.columns = ['borrower_id', 'loan_amount', 'annuity', 'credit_term', 'target']

    with engine.begin() as conn:
        # Borrowers (idempotent)
        for _, row in borrowers.iterrows():
            conn.execute(text("""
                INSERT INTO borrowers (borrower_id, gender, income, education, family_status)
                VALUES (:borrower_id, :gender, :income, :education, :family_status)
                ON CONFLICT (borrower_id) DO NOTHING
            """), **row.to_dict())

        # Loan Applications
        for _, row in loan_applications.iterrows():
            conn.execute(text("""
                INSERT INTO loan_applications (borrower_id, loan_amount, annuity, credit_term, target)
                VALUES (:borrower_id, :loan_amount, :annuity, :credit_term, :target)
            """), **row.to_dict())

    # --- Bureau Records ---
    bureau_df = pd.read_csv(BUREAU_FILE)
    bureau_df = clean_bureau(bureau_df)
    bureau_records = bureau_df[['SK_ID_CURR', 'CREDIT_ACTIVE', 'CREDIT_TYPE', 'AMT_CREDIT_SUM', 'AMT_CREDIT_SUM_OVERDUE']]
    bureau_records.columns = ['borrower_id', 'credit_active', 'credit_type', 'credit_amount', 'overdue_amount']

    with engine.begin() as conn:
        for _, row in bureau_records.iterrows():
            conn.execute(text("""
                INSERT INTO bureau_records (borrower_id, credit_active, credit_type, credit_amount, overdue_amount)
                VALUES (:borrower_id, :credit_active, :credit_type, :credit_amount, :overdue_amount)
            """), **row.to_dict())

    # --- Previous Loans ---
    prev_df = pd.read_csv(PREVIOUS_FILE)
    prev_df = clean_previous(prev_df)
    previous_loans = prev_df[['SK_ID_PREV', 'SK_ID_CURR', 'AMT_CREDIT', 'NAME_CONTRACT_STATUS', 'NAME_CONTRACT_TYPE']]
    previous_loans.columns = ['previous_loan_id', 'borrower_id', 'loan_amount', 'contract_status', 'loan_type']

    with engine.begin() as conn:
        for _, row in previous_loans.iterrows():
            conn.execute(text("""
                INSERT INTO previous_loans (previous_loan_id, borrower_id, loan_amount, contract_status, loan_type)
                VALUES (:previous_loan_id, :borrower_id, :loan_amount, :contract_status, :loan_type)
                ON CONFLICT (previous_loan_id) DO NOTHING
            """), **row.to_dict())

    # --- Installments ---
    inst_df = pd.read_csv(INSTALLMENTS_FILE)
    inst_df = clean_installments(inst_df)
    installments = inst_df[['SK_ID_PREV', 'AMT_INSTALMENT', 'AMT_PAYMENT', 'DAYS_ENTRY_PAYMENT']]
    installments.columns = ['previous_loan_id', 'installment_amount', 'payment_amount', 'days_late']

    with engine.begin() as conn:
        for _, row in installments.iterrows():
            conn.execute(text("""
                INSERT INTO installments (previous_loan_id, installment_amount, payment_amount, days_late)
                VALUES (:previous_loan_id, :installment_amount, :payment_amount, :days_late)
            """), **row.to_dict())

    print("✅ Data ingestion completed successfully!")

# ----------------------------
# 5. Main
# ----------------------------
if __name__ == "__main__":
    ingest()
