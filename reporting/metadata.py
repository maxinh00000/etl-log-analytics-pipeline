"""
metadata.py

Handles:
- run-level metadata
- batch-level metadata

Stores everything into PostgreSQL.
"""

import psycopg2
from datetime import datetime


# =========================================================
# DATABASE CONNECTION
# =========================================================

def get_connection():

    return psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="etl_logs",
        user="postgres",
        password="postgres"
    )


# =========================================================
# WRITE RUN METADATA
# =========================================================

def write_run_metadata(
    run_id,
    pipeline_name,
    query_name,
    batch_size,
    avg_batch_size,
    total_records,
    malformed_records,
    runtime_seconds
):
    """
    Inserts one row into run_metadata table.
    """

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO run_metadata (
            run_id,
            pipeline_name,
            query_name,
            batch_size,
            avg_batch_size,
            total_records,
            malformed_records,
            runtime_seconds,
            executed_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        run_id,
        pipeline_name,
        query_name,
        batch_size,
        avg_batch_size,
        total_records,
        malformed_records,
        runtime_seconds,
        datetime.now()
    ))

    conn.commit()

    cursor.close()
    conn.close()


# =========================================================
# WRITE BATCH METADATA
# =========================================================

def write_batch_metadata(
    run_id,
    batch_id,
    batch_size,
    records_processed,
    malformed_records
):
    """
    Inserts one row into batch_metadata table.
    """

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO batch_metadata (
            run_id,
            batch_id,
            batch_size,
            records_processed,
            malformed_records
        )
        VALUES (%s, %s, %s, %s, %s)
    """, (
        run_id,
        batch_id,
        batch_size,
        records_processed,
        malformed_records
    ))

    conn.commit()

    cursor.close()
    conn.close()
def update_run_metadata(
    run_id,
    total_records,
    malformed_records,
    runtime_seconds
):

    conn = get_connection()

    cur = conn.cursor()

    cur.execute(
        """
        UPDATE run_metadata
        SET
            total_records = %s,
            malformed_records = %s,
            runtime_seconds = %s
        WHERE run_id = %s
        """,
        (
            total_records,
            malformed_records,
            runtime_seconds,
            run_id
        )
    )

    conn.commit()

    cur.close()

    conn.close()