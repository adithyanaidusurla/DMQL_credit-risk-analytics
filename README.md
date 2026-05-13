# Credit Risk Analytics Platform

## Project Overview

This project builds an end-to-end **Credit Risk Analytics Platform** using:

- PostgreSQL (Neon Cloud Database)
- Python + SQLAlchemy ETL Pipelines
- dbt (Data Build Tool)
- Streamlit Interactive Dashboard
- Render Cloud Deployment
- GitHub Actions CI/CD

The system ingests raw financial datasets, transforms them into a normalized OLTP database, builds an analytics-ready star schema using dbt, and delivers interactive business intelligence dashboards for credit risk analysis.

---

# Project Architecture

```text
Raw CSV Data
      ↓
Python ETL + SQLAlchemy
      ↓
PostgreSQL (Neon)
      ↓
Normalized OLTP Schema (3NF)
      ↓
dbt Transformations
      ↓
Star Schema Analytics Layer
      ↓
Streamlit Dashboard
      ↓
Render Cloud Deployment
```

---

# Technologies Used

| Technology | Purpose |
|---|---|
| Python | ETL pipelines & application logic |
| PostgreSQL | Cloud relational database |
| Neon | Serverless PostgreSQL hosting |
| SQLAlchemy | Database connection & ORM |
| dbt | Data transformation & analytics modeling |
| Streamlit | Interactive dashboard |
| Plotly | Data visualization |
| Render | Cloud deployment |
| GitHub Actions | CI/CD automation |
| SQLFluff | SQL linting & formatting |

---

# Dataset

The project uses the following datasets:

| Dataset | Description |
|---|---|
| `application_train.csv` | Borrower demographic & financial data |
| `bureau.csv` | Credit bureau records |
| `previous_application.csv` | Historical loan applications |
| `installments_payments.csv` | Installment payment history |

Dataset Download:

https://buffalo.box.com/s/z3wlswzqif58vksdiaixkogzhdkzb7w9

---

# Repository Structure

```text
credit-risk-analytics/
│
├── app.py
├── requirements.txt
├── .env
├── README.md
│
├── data/
│   ├── application_train.csv
│   ├── bureau.csv
│   ├── previous_application.csv
│   └── installments_payments.csv
│
├── etl/
│   ├── create_tables.py
│   ├── load_data.py
│   └── transform_data.py
│
├── dbt_credit_risk/
│   ├── models/
│   │   ├── staging/
│   │   └── marts/
│   ├── tests/
│   ├── snapshots/
│   └── dbt_project.yml
│
├── docs/
│   ├── ERD.png
│   └── star_schema.png
│
└── .github/
    └── workflows/
        └── ci.yml
```

---

# Phase 1 — OLTP Database Design

## Objective

Design a normalized PostgreSQL database in **Third Normal Form (3NF)** for secure and efficient transactional storage.

---

# OLTP Database Design (3NF)

The schema was normalized to:

- Eliminate redundancy
- Improve data consistency
- Avoid insert/update/delete anomalies
- Improve scalability and maintainability

---

# Entity Relationship Diagram (ERD)

![Database Design ERD](docs/ERD.png)

---

# Core Tables

| Table | Description |
|---|---|
| `borrowers` | Borrower demographic & financial profile |
| `employment` | Employment details |
| `loan_applications` | Current loan applications |
| `bureau_records` | Credit bureau records |
| `previous_loans` | Historical loans |
| `installments` | Installment payment history |

---

# Relationships

- One-to-many: Borrowers → Employment
- One-to-many: Borrowers → Loan Applications
- One-to-many: Borrowers → Bureau Records
- One-to-many: Borrowers → Previous Loans
- One-to-many: Previous Loans → Installments

---

# ETL Pipeline

## Features

- CSV ingestion using Pandas
- PostgreSQL loading using SQLAlchemy
- Batch inserts
- Data cleaning & preprocessing
- Automated table creation

---

# Database Connection

Environment variable:

```env
DATABASE_URL=your_neon_postgresql_connection_string
```

---

# Running the ETL Pipeline

## Create Tables

```bash
python etl/create_tables.py
```

## Load Data

```bash
python etl/load_data.py
```

## Run Transformations

```bash
python etl/transform_data.py
```

---

# Phase 2 — Analytics Layer with dbt

## Objective

Build an analytics-ready Star Schema optimized for reporting and advanced SQL analytics.

---

# Star Schema Design

The analytics layer was implemented using dbt.

## Central Fact Table

### `fact_loan_applications`

Stores:

- loan application outcomes
- borrower financial metrics
- credit exposure
- employment indicators
- previous loan history
- risk-related attributes

---

# Dimension Tables

| Table | Description |
|---|---|
| `dim_borrowers` | Borrower demographic attributes |
| `dim_employment` | Employment information |
| `dim_credit_profile` | Credit bureau summaries |
| `dim_previous_loan_summary` | Historical loan aggregates |

---

# Star Schema Diagram

![Star Schema](docs/star_schema.png)

---

# dbt Features Implemented

## Staging Models

Created staging models for:

- `stg_borrowers`
- `stg_employment`
- `stg_loan_applications`
- `stg_bureau_records`
- `stg_previous_loans`
- `stg_installments`

---

# Analytics Marts

Created final marts:

- `fact_loan_applications`
- `dim_borrowers`
- `dim_employment`
- `dim_credit_profile`
- `dim_previous_loan_summary`

---

# dbt Tests

Implemented:

- `unique`
- `not_null`
- `relationships`

All tests passed successfully.

---

# Generate dbt Documentation

```bash
dbt docs generate
dbt docs serve
```

Generated documentation includes:

- model lineage graph
- dependency visualization
- schema documentation
- data catalog

---

# Advanced SQL Analytics

Implemented advanced SQL queries using:

- Window Functions
- CTEs
- Ranking Functions
- Aggregations
- Risk Segmentation
- Payment Behavior Analysis

---

# Performance Optimization

Implemented:

- Connection pooling
- Query caching
- PostgreSQL indexing
- Optimized joins
- Materialized analytics tables

Example indexes:

```sql
CREATE INDEX IF NOT EXISTS idx_fact_income
ON analytics.fact_loan_applications(income);

CREATE INDEX IF NOT EXISTS idx_fact_target
ON analytics.fact_loan_applications(target);

CREATE INDEX IF NOT EXISTS idx_fact_borrower
ON analytics.fact_loan_applications(borrower_id);
```

---

# Phase 3 — Streamlit Analytics Dashboard

## Objective

Develop an interactive business intelligence dashboard for credit risk analytics.

---

# Dashboard Features

## Portfolio KPIs

- Total Applications
- Default Rate
- Average Income
- Average Loan Amount
- Total Defaults

---

# Interactive Visualizations

Implemented using Plotly:

- Loan Distribution Analysis
- Default Rate by Education
- Default Rate by Occupation
- Income Quartile Risk Segmentation
- Credit Exposure Analysis
- Payment Behavior Analysis
- Late Payment Trends

---

# Dashboard Filters

Interactive filtering by:

- Gender
- Education
- Income Range

---

# Dashboard Technologies

| Technology | Purpose |
|---|---|
| Streamlit | Web dashboard |
| Plotly | Interactive charts |
| SQLAlchemy | Database connectivity |
| Pandas | Data processing |

---

# Running the Dashboard Locally

Install dependencies:

```bash
pip install -r requirements.txt
```

Run Streamlit:

```bash
streamlit run app.py
```

---

# Streamlit Dashboard Deployment (Render)

## Deployment Platform

The Streamlit dashboard is deployed on:

- Render Cloud Platform

---

# Deployment Steps

## 1. Push Project to GitHub

```bash
git init
git add .
git commit -m "Initial commit"
git push origin main
```

---

## 2. Create Render Web Service

- Connect GitHub repository
- Select branch: `main`
- Runtime: Python

---

# Render Build Command

```bash
pip install -r requirements.txt
```

---

# Render Start Command

```bash
streamlit run app.py --server.port=$PORT --server.address=0.0.0.0
```

---

# Environment Variables

Set in Render dashboard:

```env
DATABASE_URL=your_neon_connection_string
```

---

# Streamlit Deployment Features

Implemented:

- Connection pooling
- Streamlit caching
- Optimized SQL queries
- Error handling
- Responsive layout
- Dark theme UI

---

# CI/CD Automation

## GitHub Actions

Implemented CI/CD pipeline using GitHub Actions.

Workflow file:

```text
.github/workflows/ci.yml
```

---

# Automated Pipeline Includes

- Python dependency installation
- SQLFluff linting
- dbt validation
- Automated testing
- Continuous integration checks

---

# Example GitHub Actions Workflow

```yaml
name: CI Pipeline

on:
  push:
    branches:
      - main

jobs:
  build:
    runs-on: ubuntu-latest

    steps:
    - uses: actions/checkout@v3

    - name: Setup Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.11'

    - name: Install Dependencies
      run: pip install -r requirements.txt

    - name: Run SQLFluff
      run: sqlfluff lint

    - name: Run dbt Tests
      run: dbt test
```

---

# Data Quality Validation

Implemented validation checks for:

- Missing values
- Duplicate records
- Foreign key integrity
- Null constraints
- Relationship consistency

---

# Key Learning Outcomes

This project demonstrates:

- Relational database design
- PostgreSQL cloud deployment
- ETL pipeline engineering
- Data warehousing concepts
- dbt analytics engineering
- Dashboard development
- Cloud deployment
- CI/CD automation
- SQL performance tuning

---



# Live Dashboard Deployment

Streamlit dashboard deployed on Render:

https://dmql-credit-risk-analytics.onrender.com

---

