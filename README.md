# End-to-End ETL Pipeline

A production-style batch ETL pipeline built with PySpark, Apache Airflow, AWS S3, and MySQL.
Raw CSV and JSON data is uploaded to AWS S3, read by PySpark, transformed, stored as Parquet
in S3 processed folder, and finally loaded into a MySQL database — fully orchestrated and
scheduled via Apache Airflow.

---


## Production flow 

```
Upstream Systems
(APIs, Databases, SFTP, Kafka)
        ↓
    Auto drop files
        ↓
  AWS S3 (raw/ folder)     ← data lands here automatically
        ↓
  Airflow schedules & orchestrates:
        ↓
  Task 1: upload_raw_files
          Upload local CSV/JSON → S3 raw/
          (In production this step is skipped —
           upstream systems handle it automatically)
        ↓
  Task 2: transform_data
          PySpark reads from S3 raw/
          → Schema enforcement (StructType)
          → Null handling, Deduplication
          → Join employees + departments
          → Save as Parquet locally (intermediate)
        ↓
  Task 3: load_to_s3_processed
          Read local Parquet
          → Save to S3 processed/ as Parquet
          → Clean Data Lake layer ready for consumption
        ↓
  Task 4: load_to_mysql
          Read local Parquet
          → Load into MySQL etl_db.employees
          → Full refresh (truncate + insert)
```
## Architecture (How it works — Big Picture)

```
Your Local Machine
      │
      │  Step 1: You update employees.csv or departments.json locally
      │
      ▼
  upload_to_s3.py (manual) OR Airflow DAG (scheduled)
      │
      │  Step 2: Raw files uploaded to AWS S3 raw/ folder
      │
      ▼
  AWS S3 Bucket: etl-end-to-end-project
      │   └── raw/employees.csv
      │   └── raw/departments.json
      │
      │  Step 3: PySpark reads directly from S3 raw/
      │
      ▼
  transform.py (PySpark)
      │
      │  Step 4: Transformations applied
      │          - Schema enforcement using StructType + StructField
      │          - Null handling (unknown name, 0 salary)
      │          - Deduplication (remove duplicate emp_id)
      │          - Uppercase department names
      │          - Join employees + departments data
      │          - Add ingestion_timestamp for audit
      │
      ▼
  data/output/employees_parquet  (Local Parquet — Intermediate Storage)
      │
      │  Step 5: Save processed Parquet back to S3 processed/ folder
      │
      ▼
  AWS S3 Bucket: etl-end-to-end-project
      │   └── processed/employees_parquet/   ← Clean Data Lake layer
      │
      │  Step 6: Load clean data from local Parquet into MySQL
      │
      ▼
  MySQL Database: etl_db.employees (Final destination)

  ──────────────────────────────────────────────────────────
  All steps above are orchestrated by Airflow DAG:

  upload_raw_files → transform_data → load_to_s3_processed → load_to_mysql
```

---

## Why this flow? (Explain in interviews like this)

> "In production, raw files land in S3 automatically from upstream systems like APIs or databases.
> Airflow schedules the pipeline daily. PySpark reads raw files from S3, enforces schema using
> StructType, applies transformations like null handling, deduplication, and joins, then saves
> cleaned data back to S3 as Parquet in a processed/ folder — this is the Data Lake pattern
> where raw and processed layers are separated. Finally clean data is loaded into MySQL for
> structured querying. AWS credentials are never hardcoded — managed via .env file excluded
> from version control."

---

## S3 Folder Structure (Data Lake Pattern)

```
etl-end-to-end-project/          ← S3 Bucket
├── raw/                          ← Raw layer (source files as-is)
│   ├── employees.csv
│   └── departments.json
└── processed/                    ← Processed layer (clean, transformed)
    └── employees_parquet/
        ├── part-00000-xxx.parquet
        └── _SUCCESS
```

**Why separate raw and processed folders?**
> "This is standard Data Lake architecture. Raw layer preserves original data for reprocessing.
> Processed layer has clean, transformed data ready for consumption. If transformation logic
> changes, we can always reprocess from raw without losing original data."

---

## Tech Stack

| Tool | Purpose | Why used |
|------|---------|----------|
| Python | Core scripting | General purpose, widely used in DE |
| PySpark | Data transformation | Handles large-scale data efficiently |
| Apache Airflow | Pipeline orchestration | Scheduling, retries, task dependencies |
| AWS S3 | Raw + Processed file storage | Scalable, durable Data Lake storage |
| MySQL | Final data warehouse | Structured storage for clean data |
| Docker | Airflow containerization | Consistent environment setup |
| Parquet | Intermediate + processed storage | Columnar format, efficient for reads |
| boto3 | AWS S3 Python SDK | Upload/download/verify files in S3 |
| Java 17 | PySpark runtime | PySpark requires JVM to run |

---

## Project Structure

```
etl-pipeline-pyspark/
├── dags/
│   └── etl_dag.py                  # Airflow DAG using TaskFlow API
├── data/
│   ├── employees.csv               # Raw employee data (source)
│   ├── departments.json            # Raw department data (source)
│   └── output/
│       └── employees_parquet/      # Local Parquet (intermediate) - not pushed to GitHub
├── scripts/
│   ├── upload_to_s3.py             # Manual: Upload local files to S3 raw/
│   ├── transform.py                # PySpark: Read S3 raw/ → transform → save local Parquet
│   ├── load_to_s3_processed.py     # Save local Parquet → S3 processed/ folder
│   └── load_to_mysql.py            # Load local Parquet → MySQL database
├── .env                            # AWS credentials (NOT pushed to GitHub)
├── .env.example                    # Template showing required env variables (safe to push)
├── .gitignore                      # Excludes .env, output/, logs/, __pycache__
├── docker-compose.yml              # Airflow Docker setup
├── Dockerfile                   # Custom Airflow image with boto3 + mysql-connector
└── README.md
```

---

## Step-by-Step Pipeline Explanation

### Step 1 — Upload raw files to S3

**Manual run:**
```bash
python scripts/upload_to_s3.py
```

**Via Airflow:** `upload_raw_files` task runs automatically as first DAG task.

**What it does:**
- Reads `employees.csv` and `departments.json` from local `data/` folder
- Uploads to S3 `raw/` folder
- S3 paths:
  - `s3://etl-end-to-end-project/raw/employees.csv`
  - `s3://etl-end-to-end-project/raw/departments.json`

**Why S3 raw/?**
S3 raw/ acts as staging layer — single source of truth for original data.
In production, upstream systems drop files here automatically.

---

### Step 2 — PySpark reads from S3 and transforms

```bash
python scripts/transform.py
```

**What it does:**

| Operation | Detail |
|-----------|--------|
| Schema definition | StructType + StructField — enforced at read time |
| Read CSV from S3 | `s3a://etl-end-to-end-project/raw/employees.csv` |
| Read JSON from S3 | `s3a://etl-end-to-end-project/raw/departments.json` |
| Null handling | Empty name → `"Unknown"`, empty salary → `0` |
| Deduplication | Remove duplicates based on `emp_id` |
| Uppercase | Department names uppercased for consistent join |
| Join | Employee + Department (left join) |
| Timestamp | `ingestion_timestamp` added for audit trail |
| Save Parquet | `data/output/employees_parquet/` (local intermediate) |

**Why StructType instead of inferSchema?**
> "StructType enforces schema at read time — faster, catches bad data early,
> no extra Spark pass needed unlike inferSchema which scans full data first."

---

### Step 3 — Save processed data to S3 processed/ folder

```bash
python scripts/load_to_s3_processed.py
```

**What it does:**
- Reads local Parquet from `data/output/employees_parquet/`
- Writes directly to S3 `processed/` folder as Parquet
- Verifies upload by listing files in S3 `processed/` folder

**Why save back to S3?**
> "Processed/ folder in S3 acts as the clean data layer in Data Lake architecture.
> Other downstream systems or teams can directly consume from here without
> hitting the database. Raw and processed are always separate."

---

### Step 4 — Load into MySQL

```bash
python scripts/load_to_mysql.py
```

**What it does:**
- Reads local Parquet from `data/output/employees_parquet/`
- Creates `etl_db.employees` table if not exists
- Truncates existing data (full refresh)
- Inserts all clean records
- Verifies by printing loaded records

**Why truncate before load?**
> "Full refresh pattern — simpler and safer than upsert for this use case.
> Every run loads fresh clean data."

---

### Step 5 — Airflow orchestrates all steps

```bash
docker-compose up
```

Open `http://localhost:8080` → username: `admin` password: `admin`

**DAG: `etl_pipeline_dag`**

```
upload_raw_files → transform_data → load_to_s3_processed → load_to_mysql
```

**DAG features:**
- Runs **daily** automatically (`@daily`)
- **1 retry** with 2 minute delay on failure
- Task outputs passed between tasks via XCom automatically
- If any task fails → downstream tasks won't run
- Built using modern **TaskFlow API** (`@dag` + `@task` decorators)

- Airflow tasks use Python + boto3 directly (not PySpark inside container)
- PySpark runs locally — Airflow orchestrates, not processes
- Standard pattern: in production Airflow submits jobs to EMR/Databricks

**Why TaskFlow API over BashOperator?**
> "TaskFlow API is the modern Pythonic way to write DAGs. Tasks are plain Python
> functions decorated with @task. Data passes between tasks automatically via XCom
> without manual push/pull. Much cleaner than BashOperator."

---

## How to Run from Scratch

### 1. Clone the repository
```bash
git clone https://github.com/surajk252000/etl-pipeline-pyspark.git
cd etl-pipeline-pyspark
```

### 2. Install dependencies
```bash
pip install pyspark==3.5.0 mysql-connector-python boto3 awscli
```

### 3. Setup AWS credentials
Copy `.env.example` to `.env` and fill real values:
```bash
cp .env.example .env
```

`.env` file:
```
AWS_ACCESS_KEY_ID=your_actual_access_key
AWS_SECRET_ACCESS_KEY=your_actual_secret_key
AWS_DEFAULT_REGION=ap-south-1
```

Also create `~/.aws/credentials` for local script runs:
```
[default]
aws_access_key_id = your_actual_access_key
aws_secret_access_key = your_actual_secret_key
```

### 4. Upload raw files to S3
```bash
python scripts/upload_to_s3.py
```

### 5. Run PySpark transformation
```bash
python scripts/transform.py
```

### 6. Save processed data to S3
```bash
python scripts/load_to_s3_processed.py
```

### 7. Load into MySQL
```bash
python scripts/load_to_mysql.py
```

### 8. Start Airflow
```bash
docker-compose up
```
- First run takes 4-5 minutes (pip installs inside container)
- Open `http://localhost:8080`
- Login: `admin` / `admin`
- Trigger `etl_pipeline_dag`

---

## Security — Credentials handling

| Location | Method | Safe to push? |
|----------|--------|---------------|
| Local scripts | `~/.aws/credentials` | Never pushed (system file) |
| Airflow/Docker | `.env` via `env_file` | No — in `.gitignore` |
| GitHub | `.env.example` placeholder | Yes — safe template |
| PySpark | `DefaultAWSCredentialsProviderChain` | Yes — no hardcoded keys |

**Rule: Never hardcode AWS keys in any script pushed to GitHub.**

---

## Data Flow Example

### Raw Input — employees.csv (S3 raw/)
```
emp_id  name            department    salary   joining_date
1       Amit Sharma     Engineering   75000    2022-01-15
7       null            Finance       50000    2021-04-30   <- null name
8       Rohit Mehta     Engineering   null     2020-07-18   <- null salary
```

### Raw Input — departments.json (S3 raw/)
```json
{"dept_id": 1, "dept_name": "Engineering", "location": "Bengaluru", "budget": 5000000}
{"dept_id": 4, "dept_name": "Finance", "location": "Pune", "budget": 2000000}
```

### After PySpark Transformation
```
emp_id  name          department    salary  dept_name    location   budget
1       Amit Sharma   ENGINEERING   75000   ENGINEERING  Bengaluru  5000000
7       Unknown       FINANCE       50000   FINANCE      Pune       2000000
8       Rohit Mehta   ENGINEERING   0       ENGINEERING  Bengaluru  5000000
```

### Destinations
- S3 processed/ → `etl-end-to-end-project/processed/employees_parquet/` ✅
- MySQL → `etl_db.employees` table ✅

---

## Key Concepts for Interview

| Question | Answer |
|----------|--------|
| Why S3 raw/ as staging? | Decouples source from processing, preserves original data |
| Why S3 processed/ folder? | Data Lake pattern — clean layer separate from raw |
| Why Parquet? | Columnar format, faster reads, better compression than CSV |
| Why StructType over inferSchema? | Schema enforced at read time, faster, catches bad data early |
| Why Airflow? | Scheduling, retry logic, task dependency management |
| Why TaskFlow API? | Modern Pythonic DAGs, auto XCom, cleaner than BashOperator |
| Why left join? | Keep all employees even if department data is missing |
| Why truncate before load? | Full refresh pattern, simpler than upsert for this use case |
| Why DefaultAWSCredentialsProviderChain? | Reads env variables, no hardcoded keys in code |
| Why Java inside Docker? | PySpark needs JVM, not available in Airflow container by default |
| Why .env file? | Keeps credentials out of code, standard security practice |
| What is Data Lake? | S3 with raw/ and processed/ layers — raw preserved, processed ready to use |

---

## Versions Used

| Tool | Version |
|------|---------|
| Python | 3.14.3 |
| PySpark | 3.5.0 |
| Apache Airflow | 2.7.0 |
| Docker | 29.3.1 |
| Java | 17 (inside Docker), 21 (local) |
| MySQL | 8.x |
| boto3 | Latest |
| hadoop-aws JAR | 3.3.4 |
| aws-java-sdk-bundle | 1.12.262 |
| OS | Windows 11 |

---

## Author

**Suraj Kushwaha** — Data Engineer
[LinkedIn](https://www.linkedin.com/in/surajkushwaha25/) | [GitHub](https://github.com/surajk252000)
