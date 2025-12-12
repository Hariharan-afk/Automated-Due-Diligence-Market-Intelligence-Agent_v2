# ☁️ Deployment Guide: Google Cloud Composer

This guide details how to deploy the **Automated Market Intelligence Data Pipeline** to Google Cloud Composer (Managed Airflow).

---

## 🏗️ 1. Create Composer Environment

1.  Go to **Google Cloud Console** > **Composer**.
2.  Click **CREATE ENVIRONMENT** (Composer 2 recommended).
3.  **Configuration:**
    - **Name:** `market-intel-pipeline`
    - **Location:** `us-central1` (or your preferred region)
    - **Image Version:** Latest `composer-2.x.x-airflow-2.x.x`
    - **Service Account:** Compute Engine default (or custom with Storage Object Admin + BigQuery roles)
    - **Resources:**
        - **Workloads:** Medium (needed for `sentence-transformers` embedding generation)
        - **Scheduler:** 0.5 CPU, 2 GB RAM
        - **Web Server:** 0.5 CPU, 2 GB RAM
        - **Worker:** **2 GB RAM minimum** (important for PyTorch/Embeddings)

4.  Click **CREATE** (Takes ~15-20 mins).

---

## 📦 2. Install Python Dependencies

1.  In the Composer list, click your environment name.
2.  Go to the **PYPI PACKAGES** tab.
3.  **Option A (Manual):** Add packages one by one.
4.  **Option B (Bulk - Recommended):**
    - Upload the provided `composer_requirements.txt` to the environment's config bucket or edit manually.
    - **Crucial Packages:**
      ```text
      edgartools>=0.0.46
      groq
      sentence-transformers>=2.2.2
      qdrant-client
      google-cloud-storage
      psycopg2-binary
      newspaper3k
      wikipedia-api
      ```
    - **Note:** `sentence-transformers` handles PyTorch, but if you run into size limits, consider using a custom Docker image (advanced).

---

## 🔑 3. Configure Environment Variables

1.  Go to the **AIRFLOW CONFIGURATION OVERRIDES** tab (or "Environment Variables" section in Airflow UI).
2.  **Recommended:** Use the Airflow UI for variables.
    - Click **OPEN AIRFLOW UI**.
    - Go to **Admin** > **Variables**.
    - Click **Import Variables** (Browse button).
    - Select `env_template.json` (Make sure to fill in your actual keys first!).
    - Click **Import**.

3.  **Verify Variables:**
    - `SEC_API_KEY`: Your email
    - `GROQ_API_KEY`: Groq Cloud key
    - `QDRANT_URL` / `QDRANT_API_KEY`
    - `GCP_BUCKET_NAME`: The bucket you created for RAW DATA (not the composer bucket)

---

## 📂 4. Upload Code to DAGs Folder

Composer automatically creates a GCS bucket for your environment (e.g., `us-central1-market-intel-xyz-bucket`).

1.  **Locate the DAGs folder:**
    - In Composer details, click **DAGs folder** link (opens GCS Browser).
    - Path matches: `gs://[COMPOSER_BUCKET]/dags/`

2.  **Upload Structure:**
    You must maintain the import structure. Upload the entire `Data_Pipeline` contents or structure it as follows inside the `dags/` folder:

    ```text
    dags/
    ├── .airflowignore          <-- Important!
    ├── companies.yaml          <-- Upload to dags/ or separate configs/ folder
    ├── initial_load_dag.py
    ├── sec_monitoring_dag.py
    ├── ... (other DAGs)
    │
    ├── src/                    <-- Upload entire specific src folder
    │   ├── data_ingestion/
    │   ├── data_processing/
    │   └── ...
    │
    └── configs/
        ├── companies.yaml
        └── ...
    ```

    **Command Line Upload (fastest):**
    ```bash
    # From the Data_Pipeline directory
    gsutil -m cp -r .airflowignore airflow/dags/* src configs scripts gs://[COMPOSER_BUCKET]/dags/
    ```

3.  **Secrets File:**
    - Upload `gcp-credentials.json` to `gs://[COMPOSER_BUCKET]/data/`
    - Update `GCP_CREDENTIALS_PATH` variable to `/home/airflow/gcs/data/gcp-credentials.json` (Composer mounts the data bucket there).

---

## 🚀 5. Test & Verify

1.  **Check DAGs:** Refresh Airflow UI. You should see 4 DAGs.
    - If `import errors` appear, click them to debug (usually missing PyPi packages or path issues).
    
2.  **Run Initial Load:**
    - Trigger `initial_load_dag` manually.
    - **Monitor Logs:** Click the specific task instance > Log.

3.  **Troubleshooting:**
    - **Memory Error:** Increase Worker resources in Composer settings.
    - **Import Error:** Ensure `src` folder is in `dags/` and `__init__.py` files exist.

---

## ✅ Checklist

- [ ] Environment Healthy (Green check)
- [ ] PyPi Packages Installed
- [ ] Variables Imported
- [ ] Code Uploaded (dags + src)
- [ ] `gcp-credentials.json` Uploaded
- [ ] DAGs visible without errors
