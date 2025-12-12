# Automated Due Diligence Market Intelligence Agent - Data Pipeline

## 1. Overview and Data Flow

This Data Pipeline is the backbone of the Market Intelligence Agent, responsible for continuously ingesting, processing, and storing financial data from multiple sources to power downstream RAG (Retrieval-Augmented Generation) applications.

### Key Capabilities
*   **Multi-Source Ingestion**: Fetches data from SEC Filings (10-K/10-Q), Wikipedia, and Global News (NewsAPI + GDELT).
*   **Advanced Processing**:
    *   **Full Document Processing**: Handles complete SEC filings, not just summaries.
    *   **Table Intelligence**: Identifies tables in SEC filings, summarizes them using LLMs (Groq), and creates separation between text and tabular data for better retrieval.
    *   **Smart Chunking**: Uses recursive character splitting with overlap to create context-aware chunks.
    *   **Embedding**: Generates high-quality vector embeddings using `BAAI/bge-large-en-v1.5`.
*   **Bias Mitigation**: Tracks data coverage per company and applies boosting factors during retrieval to ensure fair representation of smaller companies.
*   **Robust Storage**:
    *   **Google Cloud Storage (GCS)**: Stores raw processed JSON data for audit and backup.
    *   **Qdrant**: Stores vector embeddings with rich metadata for semantic search.
    *   **PostgreSQL**: Tracks the processing state of every filing and article to prevent duplicates and monitor pipeline health.

### Data Flow
1.  **Ingestion**: Airflow DAGs trigger fetchers (`SECFetcher`, `NewsFetcher`, `WikipediaFetcher`) based on `companies.yaml`.
2.  **Processing**:
    *   Text is cleaned and normalized.
    *   Tables are extracted, summarized, and replaced with placeholders in the text.
    *   Content is chunked into manageable sizes (e.g., 1024 tokens).
3.  **Embedding**: Chunks and table summaries are passed through the Embedding Model to generate vectors.
4.  **Storage**:
    *   Raw data (chunks + metadata) -> Uploaded to GCS.
    *   Vectors -> Uploaded to Qdrant.
    *   Job Status -> Updated in PostgreSQL.
5.  **Validation**: Data availability and quality are verified using `DataValidator`.

## 2. Repository Structure

```text
Data_Pipeline/
├── airflow/                 # Apache Airflow configuration and DAGs
│   ├── dags/                # Workflow definitions (initial_load, sec_monitoring, news_fetch)
│   ├── airflow.cfg          # Local Airflow config
│   └── README.md            # Airflow-specific setup guide
├── bias_config/             # Configuration for Bias Mitigation (boost factors)
├── configs/                 # YAML Configuration files
│   ├── companies.yaml       # List of companies to track (Ticker, CIK)
│   └── sec_config.yaml      # Settings for SEC fetching and processing
├── data/                    # Local temporary storage for intermediate files
├── logs/                    # Execution logs
├── scripts/                 # Standalone utility scripts
│   ├── fetch_and_process_sec.py  # Manual script to process SEC filings
│   ├── test_end_to_end.py        # Full pipeline integration test
│   ├── test_infrastructure.py    # Verifies clout connectivity
│   └── validate_data.py          # Data quality validation tool
├── src/                     # Core Source Code
│   ├── bias/                # Coverage tracking and bias boosting logic
│   ├── cloud/               # Connectors for GCS, Qdrant, and Postgres
│   ├── data_ingestion/      # Fetching logic for SEC, News, Wiki
│   ├── data_processing/     # Cleaning, chunking, and table summarization
│   ├── embedding/           # Vector generation (BGE-Large)
│   ├── orchestration/       # Airflow helpers and utilities
│   ├── state/               # State management (Postgres)
│   └── validation/          # Data validation checks
└── requirements.txt         # Python dependencies
```

## 3. Setup Instructions

### Prerequisites
*   Python 3.10+
*   Google Cloud Platform Account (for GCS)
*   Qdrant Cloud Cluster (or local instance)
*   PostgreSQL Database (local or cloud)
*   Groq API Key (for table summarization)

### Step 1: Installation
1.  **Clone the repository** and navigate to `Data_Pipeline`.
2.  **Install dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

### Step 2: Environment Configuration
Create a `.env` file in the `Data_Pipeline` root with the following variables:

```ini
# Google Cloud
GCP_PROJECT_ID=your-project-id
GCP_BUCKET_NAME=your-bucket-name
GCP_CREDENTIALS_PATH=path/to/your/service-account.json

# Qdrant Vector DB
QDRANT_URL=your-qdrant-url
QDRANT_API_KEY=your-qdrant-api-key
QDRANT_COLLECTION=company_data

# PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=market_intel
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your-password

# API Keys
GROQ_API_KEY=your-groq-api-key
NEWS_API_KEY=your-news-api-key
SEC_API_KEY=your-email@example.com  # Used as User-Agent for SEC EDGAR
```

### Step 3: Verify Infrastructure
Run the infrastructure test to ensure all connections are working:
```bash
python scripts/test_infrastructure.py
```

## 4. Running Locally & Testing

You can run individual parts of the pipeline manually without Airflow for testing or ad-hoc data loading.

### Run SEC Processor
To fetch and process SEC filings for companies in `configs/companies.yaml`:
```bash
python scripts/fetch_and_process_sec.py
```
*   **Output**: Processed JSON files in `data/` and uploads to GCS/Qdrant.

### Run End-to-End Test
To simulate a full pipeline run for a single filing (AAPL 10-K):
```bash
python scripts/test_end_to_end.py
```

### Validate Data
To check the quality of processed data (completeness, token sizes, table references):
```bash
python scripts/validate_data.py --file data/some_processed_file.json
```

## 5. Cloud Setup Guide

### Google Cloud Storage (GCS)
1.  Create a project in Google Cloud Console.
2.  Create a **Storage Bucket** (e.g., `market-intel-data`).
3.  Go to **IAM & Admin > Service Accounts** and create a service account.
4.  Grant the **Storage Object Admin** role to this account.
5.  Create a JSON key for the service account and save it locally (point `GCP_CREDENTIALS_PATH` to this file).

### Qdrant (Vector Database)
1.  Sign up for [Qdrant Cloud](https://cloud.qdrant.io/).
2.  Create a **free tier cluster**.
3.  Get the **Cluster URL** and **API Key** from the dashboard.
4.  The pipeline will automatically create the collection `company_data` if it doesn't exist.

### PostgreSQL (State DB)
1.  Install PostgreSQL locally or use a cloud provider (e.g., Supabase, AWS RDS).
2.  Create a database named `market_intel`.
3.  The pipeline will automatically create necessary tables (`sec_filings`, `pipeline_runs`) on the first run.

## 6. Orchestration (Airflow)

For production, use Airflow to schedule and manage tasks.

1.  **Initialize Airflow**:
    ```bash
    export AIRFLOW_HOME=$(pwd)/airflow
    airflow db init
    airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com
    ```
2.  **Start Airflow**:
    ```bash
    # Terminal 1: Scheduler
    export AIRFLOW_HOME=$(pwd)/airflow
    airflow scheduler

    # Terminal 2: Webserver
    export AIRFLOW_HOME=$(pwd)/airflow
    airflow webserver --port 8080
    ```
3.  **Access UI**: Go to `http://localhost:8080`, login with the admin account, and enable the DAGs (`initial_load_dag`, `sec_monitoring_dag`, `news_fetch_dag`).
