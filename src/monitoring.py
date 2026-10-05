import os
import uuid
from datetime import datetime

import psycopg2


def get_connection():
    return psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        dbname="real_estate",
        user="postgres",
        password=os.environ["REAL_ESTATE_POSTGRES_PASSWORD"],
    )


def start_run(stage, partition_name=None):
    run_id =str(uuid.uuid4())
    started_at = datetime.now()

    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO monitoring.pipeline_runs (
                    run_id,
                    stage,
                    partition_name,
                    started_at,
                    status
                )
                VALUES (%s, %s, %s, %s, %s);
                """,
                (
                    run_id,
                    stage,
                    partition_name,
                    started_at,
                    "STARTED",
                ),
            )

    return run_id


def finish_run(
    run_id,
    status,
    rows_processed=None,
    error_message=None,
):
    finished_at = datetime.now()

    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE monitoring.pipeline_runs
                SET
                    finished_at = %s,
                    status = %s,
                    rows_processed = %s,
                    error_message = %s
                WHERE run_id = %s;
                """,
                (
                    finished_at,
                    status,
                    rows_processed,
                    error_message,
                    run_id,
                ),
            )