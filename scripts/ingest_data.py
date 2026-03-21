import os
import pandas as pd
from sqlalchemy import create_engine

# ----------------------------
# 0. Database connection
# ----------------------------
DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(
    DATABASE_URL,
    pool_size=2,
    max_overflow=1,
    pool_pre_ping=True,
    pool_recycle=300
)

# ----------------------------
# 1. File paths
# ----------------------------
DATA_DIR = "data"
APPLICATION_FILE = f"{DATA_DIR}/application_train.csv"
BUREAU_FILE = f"{DATA_DIR}/bureau.csv"
PREVIOUS_FILE = f"{DATA_DIR}/previous_application.csv"
INSTALLMENTS_FILE = f"{DATA_DIR}/installments_payments.csv"

# ----------------------------
# 2. Cleaning functions
# ----------------------------
def clean_application(df):
    df = df.drop_duplicates()
    df = df.fillna({"CODE_GENDER": "XNA", "AMT_INCOME_TOTAL": 0, "TARGET": 0})
    df['AMT_INCOME_TOTAL'] = pd.to_numeric(df['AMT_INCOME_TOTAL'], errors='coerce').fillna(0).astype('float32')
    df['TARGET'] = df['TARGET'].astype('int8')
    return df

def clean_bureau(df):
    df = df.drop_duplicates()
    df = df.fillna({
        "CREDIT_ACTIVE": "UNKNOWN",
        "CREDIT_TYPE": "UNKNOWN",
        "AMT_CREDIT_SUM": 0,
        "AMT_CREDIT_SUM_OVERDUE": 0
    })
    df['AMT_CREDIT_SUM'] = pd.to_numeric(df['AMT_CREDIT_SUM'], errors='coerce').fillna(0).astype('float32')
    df['AMT_CREDIT_SUM_OVERDUE'] = pd.to_numeric(df['AMT_CREDIT_SUM_OVERDUE'], errors='coerce').fillna(0).astype('float32')
    return df

def clean_previous(df):
    df = df.drop_duplicates()
    df = df.fillna({"NAME_CONTRACT_STATUS": "UNKNOWN", "NAME_CONTRACT_TYPE": "UNKNOWN",
                    "AMT_CREDIT": 0})
    df['AMT_CREDIT'] = pd.to_numeric(df['AMT_CREDIT'], errors='coerce').fillna(0).astype('float32')
    return df

def clean_installments(df):
    df = df.drop_duplicates()
    df = df.fillna({"AMT_INSTALMENT": 0, "AMT_PAYMENT": 0, "DAYS_ENTRY_PAYMENT": 0})
    df['AMT_INSTALMENT'] = pd.to_numeric(df['AMT_INSTALMENT'], errors='coerce').fillna(0).astype('float32')
    df['AMT_PAYMENT'] = pd.to_numeric(df['AMT_PAYMENT'], errors='coerce').fillna(0).astype('float32')
    df['DAYS_ENTRY_PAYMENT'] = pd.to_numeric(df['DAYS_ENTRY_PAYMENT'], errors='coerce').fillna(0).astype('int32')
    return df

# ----------------------------
# 3. Ingestion function
# ----------------------------
def ingest(sample_frac=0.1):
    print("🚀 Starting data ingestion...")

    # --- Load application CSV ---
    app_cols = ['SK_ID_CURR', 'CODE_GENDER', 'AMT_INCOME_TOTAL',
                'TARGET', 'DAYS_EMPLOYED', 'OCCUPATION_TYPE',
                'NAME_EDUCATION_TYPE', 'NAME_FAMILY_STATUS']
    app_df = pd.read_csv(APPLICATION_FILE, usecols=app_cols)
    app_df = app_df.sample(frac=sample_frac, random_state=42)
    app_df = clean_application(app_df)

    # --- Borrowers ---
    borrowers = app_df[['SK_ID_CURR', 'CODE_GENDER', 'AMT_INCOME_TOTAL', 
                        'NAME_EDUCATION_TYPE', 'NAME_FAMILY_STATUS']].copy()
    borrowers.columns = ['borrower_id', 'gender', 'income', 'education', 'family_status']
    borrowers.drop_duplicates(subset='borrower_id', inplace=True)
    existing_borrowers = pd.read_sql("SELECT borrower_id FROM borrowers", engine)
    borrowers = borrowers[~borrowers['borrower_id'].isin(existing_borrowers['borrower_id'])]
    borrowers.to_sql('borrowers', engine, if_exists='append', index=False, chunksize=1000)
    print(f"✅ Borrowers uploaded: {len(borrowers)}")

    # --- Employment ---
    employment = app_df[['SK_ID_CURR', 'OCCUPATION_TYPE', 'DAYS_EMPLOYED']].copy()
    employment.columns = ['borrower_id', 'occupation_type', 'years_employed']
    employment['years_employed'] = employment['years_employed'].apply(lambda x: max(x, 0))
    employment['occupation_type'] = employment['occupation_type'].fillna('UNKNOWN')
    employment.drop_duplicates(subset='borrower_id', inplace=True)
    existing_ids = pd.read_sql("SELECT borrower_id FROM borrowers", engine)['borrower_id']
    employment = employment[employment['borrower_id'].isin(existing_ids)]
    employment.to_sql('employment', engine, if_exists='append', index=False, chunksize=1000)
    print(f"✅ Employment uploaded: {len(employment)}")

    # --- Loan Applications ---
    loan_app = app_df[['SK_ID_CURR', 'AMT_INCOME_TOTAL', 'TARGET']].copy()
    loan_app.columns = ['borrower_id', 'loan_amount', 'target']
    loan_app['loan_amount'] = loan_app['loan_amount'].astype('float32')
    loan_app.to_sql('loan_applications', engine, if_exists='append', index=False, chunksize=1000)
    print(f"✅ Loan applications uploaded: {len(loan_app)}")

    # --- Bureau Records ---
    bureau_cols = ['SK_ID_CURR', 'CREDIT_ACTIVE', 'CREDIT_TYPE', 'AMT_CREDIT_SUM', 'AMT_CREDIT_SUM_OVERDUE']
    bureau_df = pd.read_csv(BUREAU_FILE, usecols=bureau_cols).sample(frac=sample_frac, random_state=42)
    bureau_df = clean_bureau(bureau_df)

    # Correct column mapping
    bureau_df = bureau_df.rename(columns={
        'SK_ID_CURR': 'borrower_id',
        'CREDIT_ACTIVE': 'credit_active',
        'CREDIT_TYPE': 'credit_type',
        'AMT_CREDIT_SUM': 'credit_amount',
        'AMT_CREDIT_SUM_OVERDUE': 'overdue_amount'
    })

    bureau_df['credit_amount'] = pd.to_numeric(bureau_df['credit_amount'], errors='coerce').fillna(0).astype('float32')
    bureau_df['overdue_amount'] = pd.to_numeric(bureau_df['overdue_amount'], errors='coerce').fillna(0).astype('float32')
    bureau_df['credit_active'] = bureau_df['credit_active'].fillna('UNKNOWN')
    bureau_df['credit_type'] = bureau_df['credit_type'].fillna('UNKNOWN')

    # Filter only existing borrowers
    bureau_df = bureau_df[bureau_df['borrower_id'].isin(existing_ids)]
    bureau_df.to_sql('bureau_records', engine, if_exists='append', index=False, chunksize=1000)
    print(f"✅ Bureau records uploaded: {len(bureau_df)}")

    # --- Previous Loans ---
    prev_cols = ['SK_ID_PREV', 'SK_ID_CURR', 'AMT_CREDIT', 'NAME_CONTRACT_STATUS', 'NAME_CONTRACT_TYPE']
    prev_df = pd.read_csv(PREVIOUS_FILE, usecols=prev_cols).sample(frac=sample_frac, random_state=42)
    prev_df = clean_previous(prev_df)
    prev_df = prev_df.rename(columns={
        'SK_ID_PREV': 'previous_loan_id',
        'SK_ID_CURR': 'borrower_id',
        'AMT_CREDIT': 'loan_amount',
        'NAME_CONTRACT_STATUS': 'contract_status',
        'NAME_CONTRACT_TYPE': 'loan_type'
    })
    prev_df['loan_amount'] = pd.to_numeric(prev_df['loan_amount'], errors='coerce').fillna(0).astype('float32')
    prev_df['contract_status'] = prev_df['contract_status'].fillna('UNKNOWN')
    prev_df['loan_type'] = prev_df['loan_type'].fillna('UNKNOWN')
    existing_prev = pd.read_sql("SELECT previous_loan_id FROM previous_loans", engine)
    prev_df = prev_df[~prev_df['previous_loan_id'].isin(existing_prev['previous_loan_id'])]
    prev_df = prev_df[prev_df['borrower_id'].isin(existing_ids)]
    prev_df.to_sql('previous_loans', engine, if_exists='append', index=False, chunksize=1000)
    print(f"✅ Previous loans uploaded: {len(prev_df)}")

    # --- Installments ---
    inst_cols = ['SK_ID_PREV', 'AMT_INSTALMENT', 'AMT_PAYMENT', 'DAYS_ENTRY_PAYMENT']
    inst_df = pd.read_csv(INSTALLMENTS_FILE, usecols=inst_cols).sample(frac=sample_frac, random_state=42)
    inst_df = clean_installments(inst_df)
    inst_df = inst_df.rename(columns={
        'SK_ID_PREV': 'previous_loan_id',
        'AMT_INSTALMENT': 'installment_amount',
        'AMT_PAYMENT': 'payment_amount',
        'DAYS_ENTRY_PAYMENT': 'days_late'
    })
    existing_prev_ids = pd.read_sql("SELECT previous_loan_id FROM previous_loans", engine)['previous_loan_id']
    inst_df = inst_df[inst_df['previous_loan_id'].isin(existing_prev_ids)]
    inst_df.to_sql('installments', engine, if_exists='append', index=False, chunksize=1000)
    print(f"✅ Installments uploaded: {len(inst_df)}")

    print("🎉 Data ingestion completed successfully!")

# ----------------------------
# 4. Run ingestion
# ----------------------------
if __name__ == "__main__":
    ingest(sample_frac=0.5)