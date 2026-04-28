# ETL Log Analytics Pipeline

An Extract, Transform, and Load (ETL) pipeline designed to process, analyze, and query large-scale web server access logs (specifically NASA HTTP logs). This project was built for **DAS 839 – NoSQL Systems** to demonstrate and compare log analytics workflows across two distinct NoSQL paradigms: **Apache Hive (Hadoop Ecosystem)** and **MongoDB (Document Store)**.

## Architecture Overview

The pipeline processes raw HTTP logs in the Common Log Format (CLF), parses them using a custom Python regex-based parser, and executes analytical queries to extract operational insights. The project implements two separate pipelines:

1. **MongoDB Pipeline**: 
   - A Python-driven pipeline that parses raw logs and ingests them directly into MongoDB as JSON documents.
   - Utilizes MongoDB's powerful Aggregation Framework to run analytical queries.
   - Results are stored in dedicated output collections (`query1_results`, `query2_results`, `query3_results`).

2. **Hive Pipeline (Batch Processing)**:
   - A distributed batch processing pipeline where raw logs reside on HDFS.
   - Uses HiveQL to extract data into `parsed_logs` (stored as ORC format with Snappy compression).
   - Executes complex ETL staging queries partitioned by batch IDs.
   - Employs a Python loader (`load_to_pg.py`) to transfer the aggregated Hive TSV outputs into a highly structured **PostgreSQL** database for downstream reporting.

## Repository Structure

```text
etl-log-analytics-pipeline/
├── data/                       # Directory for raw log files and sample data
│   └── sample/                 # Small sample logs for testing
├── parser/
│   └── parser.py               # Core Python regex parser for NASA logs
├── pipelines/
│   ├── hive/                   # Hive/Hadoop ETL Pipeline
│   │   ├── hive_logic/         # HQL scripts for table creation and batch processing
│   │   ├── scripts/            # Python/Shell orchestration & PostgreSQL loaders
│   │   └── sql/                # PostgreSQL schema definitions
│   └── mongodb/                # MongoDB NoSQL Pipeline
│       ├── load_data.py        # Log ingestion script into MongoDB
│       ├── query1.py           # Daily Traffic Summary aggregation
│       ├── query2.py           # Top 20 Requested Resources aggregation
│       └── query3.py           # Hourly Error Analysis aggregation
├── results/                    # Directory for generated CSV/TSV reports
├── requirements.txt            # Python dependencies
└── readme.md                   # Project documentation
```

## Key Analytics Queries

Both pipelines are built to compute three primary analytical reports:

1. **Query 1: Daily Traffic Summary**
   - *Goal*: Compute the total request count and total bytes transferred grouped by day and HTTP status code.
2. **Query 2: Top 20 Requested Resources**
   - *Goal*: Rank the most frequently accessed endpoints (paths) by request count. Computes request counts, total bytes, and the number of distinct hosts accessing each path.
3. **Query 3: Hourly Error Analysis**
   - *Goal*: Analyze server errors (status 400-599) grouped by day and hour. Calculates error requests, total requests, error rates, and the distinct number of hosts experiencing errors.

## Setup & Execution

### Prerequisites
- Python 3.x
- MongoDB (Running locally on port 27017)
- Hadoop/Hive (For the Hive pipeline)
- PostgreSQL (Running locally for Hive reporting)

### Install Dependencies

```bash
pip install -r requirements.txt
# Ensure pymongo and psycopg2-binary are installed
```

### Running the MongoDB Pipeline

1. **Load Data**:
   Ensure your raw log data path is correctly set in `pipelines/mongodb/load_data.py`, then run:
   ```bash
   python pipelines/mongodb/load_data.py
   ```
2. **Execute Queries**:
   Run the aggregation pipelines individually. Results will be saved to their respective MongoDB collections.
   ```bash
   python pipelines/mongodb/query1.py
   python pipelines/mongodb/query2.py
   python pipelines/mongodb/query3.py
   ```

### Running the Hive Pipeline

1. **Initialize PostgreSQL Schema**:
   Set up the target reporting tables using the provided schema.
   ```bash
   psql -d nasa_etl -f pipelines/hive/sql/schema_pg.sql
   ```
2. **Execute Hive Scripts**:
   The Hive pipeline requires passing variables (like `batch_id` and paths) to `process_batch.hql`. You can use the provided bash orchestration scripts in `pipelines/hive/scripts/` to automate the workflow.
3. **Load Results & Generate Reports**:
   Once Hive exports the data, load it into PostgreSQL and generate a CLI report:
   ```bash
   python pipelines/hive/scripts/load_to_pg.py --run-id <run_identifier> --batch-id <id> ...
   python pipelines/hive/scripts/report.py --run-id <run_identifier>
   ```

## Technologies Used
- **Python**: Core scripting, Regex, PyMongo, Psycopg2
- **MongoDB**: Document Database, Aggregation Pipeline
- **Apache Hive / HDFS**: Distributed Batch Processing, ORC Storage
- **PostgreSQL**: Relational Reporting Database
