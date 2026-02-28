# 📈 Automated Multi-Currency ETL Pipeline

A production-grade Data Engineering project that extracts, transforms, and loads (ETL) global exchange rates into a cloud-based Data Warehouse, featuring automated orchestration and a real-time monitoring dashboard.

## 🏗️ Architecture & Data Flow

This project implements a **Batch Processing** architecture using a **Schema-on-Write** approach to ensure data integrity.

1.  **Extraction:** Python script fetches daily rates from the Open ER-API (JSON).
2.  **Transformation:** Data cleansing using `Python 3.14` and `Pandas`, including Unix timestamp normalization and currency filtering.
3.  **Loading:** Secure UPSERT (Insert or Update) logic into **Supabase (PostgreSQL)** via `psycopg3` to ensure idempotency.
4.  **Orchestration:** **GitHub Actions** triggers the pipeline daily at 11:00 UTC using a serverless runner.
5.  **Visualization:** **Streamlit Cloud** provides a real-time dashboard connecting directly to the PostgreSQL instance.

## 🛠️ Tech Stack

- **Language:** Python 3.14 (Bleeding Edge)
- **Database:** PostgreSQL (Supabase)
- **Infrastructure:** GitHub Actions (CI/CD & Cron)
- **Visualization:** Streamlit
- **Library Focus:** `psycopg3` (Native Driver), `requests`, `Pandas`

## 🚀 Key Features

- **Idempotency:** The pipeline uses an `ON CONFLICT` SQL clause, allowing safe retries without data duplication.
- **Connection Pooling:** Optimized for cloud environments using `PgBouncer` compatibility (`prepare_threshold=None`).
- **Security:** Zero-leak policy. All credentials managed via GitHub Secrets and Streamlit Secrets.
- **Precision:** Uses `NUMERIC(18, 6)` for financial data to prevent floating-point rounding errors.

## 📈 Dashboard

The live dashboard can be accessed here: `[YOUR_STREAMLIT_LINK_HERE]`

## 🛠️ Local Setup

1. Clone the repo: `git clone https://github.com/youruser/your-repo.git`
2. Create a virtual environment: `python -m venv venv`
3. Install dependencies: `pip install -r requirements.txt`
4. Set up your `.env` file with `SUPABASE_DB_URL`.
5. Run the ingestor: `python ingestor_trm.py`
6. Run the dashboard: `streamlit run app.py`