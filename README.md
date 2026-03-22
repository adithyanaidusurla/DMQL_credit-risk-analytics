# Credit Risk Analytics Database

## Project Overview
This project ingests and models raw credit risk datasets into a **normalized PostgreSQL database** hosted on Neon. The database is designed to support analytics, reporting, and data-driven decision-making for credit risk assessment.

---

## Dataset
The project uses the following datasets (all CSV files stored in `data/`):

- `application_train.csv` – Borrower demographic and financial data  
- `bureau.csv` – Credit bureau records  
- `previous_application.csv` – Historical loan data  
- `installments_payments.csv` – Installment payments for previous loans

- [Find and download the dataset in this link](https://buffalo.box.com/s/z3wlswzqif58vksdiaixkogzhdkzb7w9) 

---

## Database Design
The database is normalized to **Third Normal Form (3NF)** to:

- Eliminate redundancy  
- Avoid update, insert, and delete anomalies  
- Support secure and efficient data ingestion  

### Entities

| Table | Description |
|-------|-------------|
| `borrowers` | Stores borrower demographic and financial information |
| `employment` | Employment details for borrowers |
| `loan_applications` | New loan requests |
| `bureau_records` | Active and historical credit bureau information |
| `previous_loans` | Historical loans of borrowers |
| `installments` | Installment payments for previous loans |

**Relationships:**

- One-to-many between Borrowers → Employment, Loan Applications, Bureau Records, Previous Loans  
- One-to-many between Previous Loans → Installments  

---

## Project Structure

DMQL_credit-risk-analytics/
│
├── data/ # Raw CSV datasets
├── docs/ # Documentation and ERD
├── scripts/ # Python ETL scripts
│ └── ingest_data.py
├── sql/ # Database schema and security
│ ├── schema.sql
│ └── security.sql
├── README.md # Project overview
├── venv/ # Python virtual environment (gitignored)
└── .gitignore # Ignore venv, data dumps, etc.



---

## Setup Instructions

### 1. Environment

1. Create a Python virtual environment:

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt


2. Set your Neon database URL:

export DATABASE_URL="postgresql://<user>:<password>@<host>/<dbname>?sslmode=require&channel_binding=require"



### 2. Provision Database
1. Create tables:
psql "$DATABASE_URL" -f sql/schema.sql

2. Set up RBAC roles:
psql "$DATABASE_URL" -f sql/security.sql

RBAC in this project:
.Analyst: Read-only (SELECT)
.App User: Read + write (SELECT, INSERT, UPDATE)

3. Data Ingestion
Run the Python ETL script:
python scripts/ingest_data.py

Features:
Cleans and transforms CSV data
Loads tables in correct order to satisfy foreign keys
Handles missing values and incorrect types
Idempotent: running multiple times does not duplicate data

4. Resource Management
SQLAlchemy connection pooling configured to conserve Neon free-tier Compute Units (CU)

engine = create_engine(
    DATABASE_URL,
    pool_size=2,
    max_overflow=1,
    pool_pre_ping=True,
    pool_recycle=300
)

Neon automatically pauses after 5 minutes of inactivity
Script runs in batches to minimize idle connections


### 3NF Justification
Database normalized to Third Normal Form (3NF)
Tables contain attributes fully dependent on their primary keys
Eliminates redundancy and prevents anomalies
Separate tables for employment, previous loans, and installments maintain clean relationships


### Demo Video
An unlisted YouTube video demonstrates:
ERD walkthrough
Database tables in Neon
Running the ingestion script successfully

[Watch the demo](https://youtu.be/IVqARN8tUk8)
