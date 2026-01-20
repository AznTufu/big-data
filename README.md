# Big Data Pipeline - ELT Architecture

## 🚀 Quick Start

### 1. Setup Infrastructure
```bash
docker-compose up -d
python -m venv .venv

# Windows PowerShell
.venv\Scripts\Activate.ps1

# Linux/Mac
source .venv/bin/activate

pip install -r requirements.txt
```

### 2. Run Complete Pipeline (Data + Dashboard)
```bash
python pipeline.py --clients 500 --purchases 7
```
✓ Generates data → Bronze → Silver → Gold → Launches dashboard on http://localhost:8502

**Scale up data volume:**
```bash
# Medium: ~500K records
python pipeline.py --clients 10000 --purchases 50

# Large: ~5M records (test Spark performance)
python pipeline.py --clients 100000 --purchases 50

# Generate data only (no pipeline)
python script/generate_data.py --clients 50000 --purchases 100
```

### 3. Run Benchmark (Pandas vs Spark + Dashboard)
```bash
# Windows PowerShell
.\run_benchmark.ps1

# Linux/Mac
chmod +x run_benchmark.sh
./run_benchmark.sh
```
✓ Runs benchmark → Saves results → Launches dashboard on http://localhost:8501

**Note:** Benchmark tests the latest CSV files from `data/sources/`. Generate data first:
- Full pipeline: `python pipeline.py --clients 10000 --purchases 50`
- Data only: `python script/generate_data.py --clients 10000 --purchases 50`

---

## 📈 Dashboards

**Business Dashboard** (port 8502): Auto-launched by `python pipeline.py`  
**Benchmark Dashboard** (port 8501): Auto-launched by `run_benchmark` scripts

---

## 🔗 Services

- **MinIO UI**: http://localhost:9001 (admin/minioadmin)
- **Prefect UI**: http://localhost:4200
- **PostgreSQL**: localhost:5432

## 📁 Project Structure

```
big_data/
├── flows/                    # Prefect flows
│   ├── bronze_ingestion.py
│   ├── silver_transformation.py
│   ├── silver_transformation_spark.py
│   ├── gold_aggregation.py
│   └── config.py
├── script/
│   └── generate_data.py
├── dashboard.py              # Business dashboard
├── dashboard_benchmark.py    # Benchmark dashboard
├── pipeline.py               # Main orchestrator
├── benchmark_simple.py       # Pandas vs Spark
├── run_benchmark.ps1         # Benchmark launcher (Windows)
├── run_benchmark.sh          # Benchmark launcher (Linux/Mac)
└── docker-compose.yml
```

## 🛑 Stop Services

```bash
docker-compose down
docker-compose down -v
```
