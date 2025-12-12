# 🆓 GCP Free Tier Deployment Guide - Market Intelligence Pipeline

**Updated:** December 2024  
**For:** Free trial accounts with $300 credits + Always Free tier

---

## 🎯 Executive Summary

**Cloud Composer is NOT viable for free tier** due to:
- ❌ Minimum cost: ~$400/month (exhausts credits in <1 month)
- ❌ High resource quotas needed (2000GB SSD, multiple CPUs)
- ❌ **Cannot request quota increases on free trial accounts**

**✅ Recommended Solution:** Self-hosted Airflow on Compute Engine VM

---

## 📊 Deployment Options Comparison

| Factor | **Cloud Composer** | **Airflow on VM (Recommended)** |
|--------|-------------------|----------------------------------|
| **Monthly Cost** | ~$400 (exhausts credits fast) | $0 (e2-small free tier eligible) |
| **Setup Complexity** | Low (managed) | Medium (manual install) |
| **Quota Requirements** | High (often blocked on free tier) | Low (within free limits) |
| **Resource Control** | Limited | Full control |
| **Free Trial Viability** | ❌ No | ✅ Yes |
| **Production Scale** | High | Low-Medium (single VM) |

---

## ✅ Option 1: Self-Hosted Airflow on Compute Engine (FREE TIER)

This option deploys Airflow using Docker Compose on a free GCP VM.

### **Step 1: Check Your Free Tier Eligibility**

**Always Free tier** includes (after 90-day trial):
- 1 x `e2-micro` VM (0.25-1 vCPU, 1GB RAM) in `us-west1`, `us-central1`, or `us-east1`
- 30GB standard persistent disk
- 1GB network egress/month

**90-day trial** ($300 credits):
- Can use larger VMs (e.g., `e2-small`: 2 vCPUs, 2GB RAM)
- Recommended for Airflow

---

### **Step 2: Create a Compute Engine VM**

1. **Go to** [Compute Engine](https://console.cloud.google.com/compute) > **CREATE INSTANCE**

2. **Configuration:**
   ```
   Name: airflow-pipeline
   Region: us-central1 (Iowa - free tier eligible)
   Zone: us-central1-a
   Machine Type:
     - Free trial: e2-small (2 vCPU, 2GB RAM) - RECOMMENDED
     - Always free: e2-micro (0.25 vCPU, 1GB RAM) - minimal, may struggle
   
   Boot Disk:
     - OS: Ubuntu 22.04 LTS
     - Disk type: Standard persistent disk
     - Size: 30 GB (free tier limit)
   
   Firewall:
     ☑ Allow HTTP traffic
     ☑ Allow HTTPS traffic
   ```

3. **Advanced Options** > **Networking** > **Network Tags:** Add `airflow`

4. Click **CREATE**

---

### **Step 3: Configure Firewall for Airflow UI**

1. Go to **VPC Network** > **Firewall** > **CREATE FIREWALL RULE**

2. **Configuration:**
   ```
   Name: allow-airflow-ui
   Targets: Specified target tags
   Target tags: airflow
   Source IP ranges: 0.0.0.0/0 (or restrict to your IP)
   Protocols and ports:
     ☑ tcp:8080
   ```

3. Click **CREATE**

---

### **Step 4: SSH into VM and Install Docker**

1. **SSH** into your VM from the Compute Engine console (click **SSH** button)

2. **Install Docker:**
   ```bash
   # Update packages
   sudo apt-get update
   sudo apt-get upgrade -y
   
   # Install Docker
   curl -fsSL https://get.docker.com -o get-docker.sh
   sudo sh get-docker.sh
   
   # Add user to docker group
   sudo usermod -aG docker $USER
   
   # Install Docker Compose
   sudo apt-get install -y docker-compose-plugin
   
   # Verify
   docker --version
   docker compose version
   
   # Re-login for group changes (or logout/login via SSH)
   newgrp docker
   ```

---

### **Step 5: Set Up Airflow with Docker Compose**

1. **Create Airflow directory:**
   ```bash
   mkdir ~/airflow
   cd ~/airflow
   mkdir -p ./dags ./logs ./plugins ./config
   echo -e "AIRFLOW_UID=$(id -u)" > .env
   ```

2. **Download Airflow Docker Compose file:**
   ```bash
   curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.8.0/docker-compose.yaml'
   ```

3. **IMPORTANT: Create lightweight configuration**

   For `e2-small` (2GB RAM), you MUST simplify the setup:

   ```bash
   # Edit docker-compose.yaml
   nano docker-compose.yaml
   ```

   **Make these changes:**
   ```yaml
   # Change executor to SequentialExecutor (lightweight)
   # Find: AIRFLOW__CORE__EXECUTOR: CeleryExecutor
   # Replace with:
   AIRFLOW__CORE__EXECUTOR: SequentialExecutor
   
   # Remove or comment out these services (not needed for Sequential):
   # - redis
   # - airflow-worker
   # - airflow-triggerer
   # - flower
   
   # Keep only these services:
   # - postgres (metadata DB)
   # - airflow-webserver
   # - airflow-scheduler
   # - airflow-init (one-time setup)
   ```

   **Minimal `docker-compose.yaml` example:**
   See complete minimal config in `minimal-docker-compose.yaml` (provided separately)

4. **Initialize Airflow database:**
   ```bash
   docker compose up airflow-init
   ```

5. **Start Airflow:**
   ```bash
   docker compose up -d
   ```

6. **Check status:**
   ```bash
   docker compose ps
   ```

---

### **Step 6: Upload Your DAGs**

1. **From your local machine**, use `gcloud` to copy files:

   ```bash
   # Install gcloud SDK if not already installed
   # https://cloud.google.com/sdk/docs/install
   
   # Authenticate
   gcloud auth login
   gcloud config set project YOUR_PROJECT_ID
   
   # Copy DAGs folder
   gcloud compute scp --recurse \
     Data_Pipeline/airflow/dags/* \
     airflow-pipeline:~/airflow/dags/ \
     --zone=us-central1-a
   
   # Copy src folder (needed by DAGs)
   gcloud compute scp --recurse \
     Data_Pipeline/src \
     airflow-pipeline:~/airflow/dags/src \
     --zone=us-central1-a
   
   # Copy configs
   gcloud compute scp --recurse \
     Data_Pipeline/configs \
     airflow-pipeline:~/airflow/dags/configs \
     --zone=us-central1-a
   ```

2. **Restart Airflow** to pick up new DAGs:
   ```bash
   # SSH into VM
   cd ~/airflow
   docker compose restart
   ```

---

### **Step 7: Install Python Dependencies**

Your DAGs need additional packages. Create a custom Airflow image:

1. **SSH into VM**, create `requirements.txt`:
   ```bash
   cd ~/airflow
   nano requirements.txt
   ```

   Paste (lightweight version):
   ```txt
   edgartools
   groq
   qdrant-client
   google-cloud-storage
   psycopg2-binary
   wikipedia-api
   newspaper3k
   pyyaml
   tiktoken
   # Skip sentence-transformers on small VM (too heavy)
   ```

2. **Create custom Dockerfile:**
   ```bash
   nano Dockerfile
   ```

   ```dockerfile
   FROM apache/airflow:2.8.0-python3.11
   
   COPY requirements.txt /requirements.txt
   RUN pip install --no-cache-dir -r /requirements.txt
   
   USER airflow
   ```

3. **Update `docker-compose.yaml`:**
   ```yaml
   # Replace: image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.8.0}
   # With:
   build: .
   image: custom-airflow:latest
   ```

4. **Rebuild and restart:**
   ```bash
   docker compose build
   docker compose down
   docker compose up -d
   ```

---

### **Step 8: Configure Environment Variables**

1. **Create `.env` file** (SSH into VM):
   ```bash
   cd ~/airflow
   nano .env
   ```

2. **Add your secrets:**
   ```bash
   AIRFLOW_UID=50000
   SEC_API_KEY=your.email@example.com
   GROQ_API_KEY=gsk_...
   QDRANT_URL=https://...
   QDRANT_API_KEY=...
   GCP_BUCKET_NAME=your-bucket
   GCP_PROJECT_ID=your-project-id
   POSTGRES_HOST=your-supabase-host
   POSTGRES_DB=postgres
   POSTGRES_USER=postgres.xyz
   POSTGRES_PASSWORD=...
   POSTGRES_PORT=5432
   ```

3. **Update `docker-compose.yaml`** to pass these as environment variables:
   ```yaml
   services:
     airflow-common:
       environment:
         # ... existing vars ...
         SEC_API_KEY: ${SEC_API_KEY}
         GROQ_API_KEY: ${GROQ_API_KEY}
         QDRANT_URL: ${QDRANT_URL}
         # ... add all others ...
   ```

4. **Restart:**
   ```bash
   docker compose restart
   ```

---

### **Step 9: Access Airflow UI**

1. **Get VM external IP:**
   ```bash
   # In GCP Console: Compute Engine > VM instances
   # Or via command:
   gcloud compute instances describe airflow-pipeline \
     --zone=us-central1-a \
     --format='get(networkInterfaces[0].accessConfigs[0].natIP)'
   ```

2. **Open browser:**
   ```
   http://[EXTERNAL_IP]:8080
   ```

3. **Login:**
   - Username: `airflow`
   - Password: `airflow`

4. **You should see your 4 DAGs!**

---

### **Step 10: Test Your Pipeline**

1. **Enable DAG:** Click the toggle switch on `initial_load_dag`
2. **Trigger manually:** Click ▶ icon
3. **Monitor logs:** Click task > Log

---

## ⚠️ Free Tier Limitations & Workarounds

### **1. Embedding Model (sentence-transformers) Too Large**

**Problem:** `sentence-transformers` requires ~2GB RAM, but `e2-small` only has 2GB total.

**Solution:**
- **Option A:** Skip embedding generation on Airflow. Generate embeddings separately on your local machine or use a serverless function.
- **Option B:** Upgrade to `e2-medium` (4GB RAM, ~$25/month, uses credits).
- **Option C:** Use a lightweight embedding API (e.g., OpenAI, Cohere) instead of local model.

### **2. Long-Running Tasks**

**Problem:** Single VM SequentialExecutor runs one task at a time.

**Solution:**
- Accept slower execution for free tier.
- Use `dag_run_timeout` to prevent indefinite hangs.

### **3. Persistent Storage**

**Problem:** Docker volumes on VM are lost if VM is deleted.

**Solution:**
- Regularly backup `/airflow` directory to GCS:
  ```bash
  tar -czf airflow-backup.tar.gz ~/airflow
  gsutil cp airflow-backup.tar.gz gs://your-bucket/backups/
  ```

---

## 💰 Cost Monitoring

**Free trial ($300 credits):**
- `e2-small` VM: ~$13/month → 23 months of credits
- Persistent disk (30GB): ~$1.20/month
- External IP (ephemeral): ~$5/month
- **Total: ~$19/month** → Can run for ~15 months on credits

**After credits expire:**
- Switch to `e2-micro` (always free in eligible regions)
- Will be very slow but technically works for minimal DAGs

---

## 🚫 Option 2: Cloud Composer (NOT RECOMMENDED FOR FREE TIER)

**Only proceed if:**
- You're okay exhausting all $300 credits in <1 month
- You need managed service for production workload
- You can afford $400+/month after trial

### Why It Won't Work:

1. **Quota Issues:**
   - Composer needs 2000GB Persistent Disk SSD (free tier default: 500GB)
   - Needs 4+ CPUs (free tier limit: often 8 total)
   - **Free trial accounts CANNOT request quota increases**

2. **Cost:**
   - Small Composer 3 environment: ~$12/day = $360/month
   - Will exhaust $300 credits in 25 days

3. **Minimum Resources:**
   - 1-3 workers @ 0.5vCPU, 2GB RAM each
   - Managed Airflow DB, scheduler, web server
   - Cannot scale down further

### If You Still Want to Try:

Follow the original `deployment_guide.md` but be aware:
- Request quota increases (will likely be denied)
- Monitor credit burn rate closely
- Migrate to VM-based solution before credits run out

---

## ✅ Recommended Path

1. **Start with self-hosted Airflow on `e2-small` VM** (uses credits, but affordable)
2. **Test your DAGs** thoroughly
3. **Switch to `e2-micro`** (always free) once development is stable
4. **Upgrade to Cloud Composer** only when:
   - Project generates revenue
   - You have budget for $400+/month
   - You need production-grade scaling

---

## 📚 Additional Resources

- [Airflow Docker Compose Docs](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
- [GCP Always Free Tier](https://cloud.google.com/free/docs/free-cloud-features#free-tier)
- [Minimal Airflow Docker Setup](https://github.com/apache/airflow/blob/main/docs/apache-airflow/howto/docker-compose/index.rst)

---

## 🆘 Troubleshooting

**DAG not appearing:**
- Check `~/airflow/dags/` has your files
- Check `docker compose logs airflow-scheduler`
- Restart: `docker compose restart`

**Out of memory:**
- Check: `docker stats`
- Reduce concurrent tasks in Airflow config
- Upgrade to `e2-medium`

**Import errors:**
- Rebuild custom image: `docker compose build`
- Check `requirements.txt` installed correctly

---

## ✅ Final Checklist

- [ ] VM created (`e2-small` in `us-central1`)
- [ ] Firewall rule for port 8080
- [ ] Docker & Docker Compose installed
- [ ] Custom Airflow image with dependencies
- [ ] DAGs uploaded to `~/airflow/dags/`
- [ ] Environment variables configured
- [ ] Airflow UI accessible at `http://[EXTERNAL_IP]:8080`
- [ ] DAGs visible and runnable
