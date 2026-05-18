# ETL Log Analytics Pipeline

## Multi-Pipeline ETL & Reporting Framework for NASA Web Server Logs

This project implements a unified ETL and analytics framework using multiple NoSQL and Hadoop ecosystem technologies to process NASA HTTP web server logs.

The same analytical queries are implemented across:

* MongoDB
* Apache Pig
* Hadoop Streaming MapReduce
* Apache Hive

All query outputs are integrated into PostgreSQL for centralized reporting and runtime analysis.

---

# Project Objectives

* Parse raw NASA web server logs in Common Log Format (CLF)
* Perform ETL using multiple big-data technologies
* Execute identical analytical queries across all pipelines
* Support scalable batch processing
* Store results in PostgreSQL
* Generate unified runtime and analytics reports
* Compare different NoSQL and Hadoop processing models

---

# Dataset

NASA Kennedy Space Center HTTP Access Logs

| Dataset  | Records   | Size   |
| -------- | --------- | ------ |
| Jul95    | 1,891,714 | 196 MB |
| Aug95    | 1,569,898 | 161 MB |
| Combined | 3,461,612 | 357 MB |

Example log entry:

```text id="ozpwby"
199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245
```

---

# Project Architecture

```text id="0gtcnz"
Raw NASA Logs
    ↓
Batch Splitting
    ↓
Pipeline Processing
(MongoDB / Pig / MapReduce / Hive)
    ↓
Analytical Queries
    ↓
PostgreSQL Integration
    ↓
Reporting Framework
```

---

# Directory Structure

```text id="d5ezgt"
etl-log-analytics-pipeline/
│
├── batching/                  # Batch splitting utilities
├── data/                      # Raw NASA log datasets
├── parser/                    # Shared regex parser
├── pipelines/
│   ├── mongodb/               # MongoDB pipeline
│   ├── pig/                   # Apache Pig pipeline
│   ├── mapreduce/             # Hadoop Streaming MapReduce
│   └── hive/                  # Apache Hive pipeline
│
├── reporting/                 # PostgreSQL loaders + reporting
├── results/                   # Generated outputs
├── pig_output/                # Pig HDFS/local outputs
│
├── main.py                    # Unified pipeline runner
├── load_pig_results.py        # Pig PostgreSQL loader
├── load_mapreduce_results.py  # MapReduce PostgreSQL loader
├── requirements.txt
└── README.md
```

---

# Pipelines

## 1. MongoDB Pipeline

### Features

* Document-oriented ETL
* Aggregation pipelines
* Batch insertion
* Shared Python parser
* PostgreSQL integration

### Workflow

```text id="quaj1j"
Raw Logs
→ Python Parser
→ MongoDB
→ Aggregation Queries
→ PostgreSQL
```

---

## 2. Apache Pig Pipeline

### Features

* Pig Latin ETL
* Hadoop local MapReduce mode
* HDFS integration
* Query-wise analytical processing

### Workflow

```text id="it9qvn"
Raw Logs
→ HDFS
→ Pig Scripts
→ HDFS Outputs
→ PostgreSQL
```

---

## 3. Hadoop Streaming MapReduce Pipeline

### Features

* Custom Python mappers/reducers
* Hadoop Streaming
* TSV outputs
* Batch analytics

### Workflow

```text id="8b59gg"
Raw Logs
→ Mapper
→ Hadoop Streaming
→ Reducer
→ TSV Outputs
→ PostgreSQL
```

---

## 4. Apache Hive Pipeline

### Features

* HiveQL analytics
* ORC + Snappy storage
* SQL-on-Hadoop ETL
* Partitioned tables

### Workflow

```text id="3r8kg8"
Raw Logs
→ Hive External Tables
→ HiveQL Processing
→ ORC Tables
→ PostgreSQL
```

---

# Analytical Queries

## Query 1 — Daily Traffic Summary

Metrics:

* request count
* total bytes
* status code distribution

---

## Query 2 — Top Requested Resources

Metrics:

* most accessed resources
* total bandwidth
* distinct hosts

---

## Query 3 — Hourly Error Analysis

Metrics:

* hourly error rates
* total requests
* distinct error hosts

---

# Technology Stack

| Component             | Technology        |
| --------------------- | ----------------- |
| Programming Language  | Python 3          |
| Database              | PostgreSQL 14     |
| NoSQL Store           | MongoDB 6.x       |
| Distributed Framework | Hadoop 3.3.6      |
| SQL Engine            | Apache Hive 3.1.3 |
| ETL Engine            | Apache Pig        |
| Reporting             | psycopg2          |
| Runtime               | Java 8            |

---

# PostgreSQL Reporting Layer

All pipelines store outputs into centralized PostgreSQL tables:

* run_metadata
* batch_metadata
* q1_daily_traffic
* q2_top_resources
* q3_hourly_errors

This enables:

* cross-pipeline comparison
* unified reporting
* runtime benchmarking

---

# Running Pipelines

## MongoDB

```powershell id="mw4r98"
python main.py --pipeline mongodb --query all --batch-size 500000
```

---

## MapReduce

```bash id="i6i7w2"
python3 main.py --pipeline mapreduce --query all --batch-size 500000 --data-dir data/raw
```

---

## Pig

```bash id="5o6b9n"
pig -param INPUT=/user/trivedh/data/raw/NASA_access_log_Aug95/access_log_Aug95 -param OUTPUT=/user/trivedh/pig_output/query1 pipelines/pig/query1.pig
```

---

# Generating Reports

```powershell id="7t8g8n"
python reporting/report.py --run-id <RUN_ID>
```

---

# Key Features

* Multi-pipeline ETL framework
* Shared analytical query logic
* Batch-wise processing
* PostgreSQL reporting
* Runtime metadata collection
* HDFS integration
* NoSQL + Hadoop ecosystem support

---

# Challenges Faced

* PostgreSQL cross-environment connectivity
* HDFS NameNode failures
* Pig HDFS path management
* MongoDB schema mismatches
* Batch metadata synchronization
* Hadoop Streaming subprocess issues
* Hive environment setup complexity

---

# Current Project Status

| Component            | Status      |
| -------------------- | ----------- |
| MongoDB Pipeline     | Completed   |
| Pig Pipeline         | Completed   |
| MapReduce Pipeline   | Completed   |
| Hive Scripts         | Implemented |
| PostgreSQL Reporting | Completed   |
| Runtime Metadata     | Completed   |
| Batch Processing     | Completed   |

---

# Conclusion

This project demonstrates a scalable multi-pipeline ETL and reporting framework for large-scale web log analytics using multiple NoSQL and Hadoop ecosystem technologies.

The implementation highlights the strengths, trade-offs, and execution models of MongoDB, Pig, MapReduce, and Hive for semi-structured analytical workloads.
