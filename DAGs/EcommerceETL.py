from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime


with DAG(
    dag_id="Ecommerce_Pipeline",
    start_date=datetime(2024, 1, 1), 
    schedule_interval=None,
    catchup=False
) as dag:

    # ── Task 1: Stream data to landing zone ───────────────────────────────────
    # e.py — pure Python, no Spark
    # Reads CSVs from /home/jovyan/data/commerce_data/
    # Writes JSON batches → /home/jovyan/data/raw_ecommerce/stream/
    # Writes static JSON  → /home/jovyan/data/raw_ecommerce/static/
    task1 = BashOperator(
        task_id="Extracting_data",
        bash_command=(
            "docker exec -e HADOOP_USER_NAME=root spark-jupyter spark-submit /home/jovyan/work/e.py"
        )
    )

    # ── Task 2: Bronze ingestion + Gold transformation ────────────────────────
    # t.py — Spark job
    # Reads JSON from landing zone → writes Bronze parquet to HDFS
    # Reads Bronze parquet → builds dims + facts → writes Gold parquet to HDFS
    task2 = BashOperator(
        task_id="Transformation",
        bash_command=(
            "docker exec -e HADOOP_USER_NAME=root spark-jupyter "
            "spark-submit /home/jovyan/work/t.py"
        )
    )

    # ── Task 3: Archive processed JSON files to HDFS ──────────────────────────
    # Moves the stream + static JSON from the local landing zone into HDFS
    # archive so they are not reprocessed on the next pipeline run.
    task3 = BashOperator(
        task_id="Archive_raw_files_to_HDFS",
        bash_command=(
            # Create archive directories
            "docker exec -e HADOOP_USER_NAME=root hadoop-namenode "
            "hdfs dfs -mkdir -p /user/root/datalake/bronze/archived/stream/ && "

            "docker exec -e HADOOP_USER_NAME=root hadoop-namenode "
            "hdfs dfs -mkdir -p /user/root/datalake/bronze/archived/static/ && "

            # Copy stream JSON batches to HDFS archive
            "docker exec spark-jupyter "
            "hdfs dfs -put -f /home/jovyan/data/raw_ecommerce/stream/*.json "
            "/user/root/datalake/bronze/archived/stream/ || true && "

            # Copy static JSON to HDFS archive
            "docker exec spark-jupyter "
            "hdfs dfs -put -f /home/jovyan/data/raw_ecommerce/static/*.json "
            "/user/root/datalake/bronze/archived/static/ || true && "

            # Clean up local landing zone so next run starts fresh
            "docker exec spark-jupyter "
            "sh -c 'rm -f /home/jovyan/data/raw_ecommerce/stream/*.json || true' && "

            "docker exec spark-jupyter "
            "sh -c 'rm -f /home/jovyan/data/raw_ecommerce/static/*.json || true'"
        )
    )

    # ── Task 4: Load Gold → Snowflake ─────────────────────────────────────────
    # l.py — Spark job
    # Reads Gold parquet from HDFS
    # Loads dims (atomic swap) + facts (incremental append) into Snowflake
    task4 = BashOperator(
        task_id="Loading",
        bash_command=(
            "docker exec -e HADOOP_USER_NAME=root spark-jupyter "
            "spark-submit "
            "--packages net.snowflake:snowflake-jdbc:3.13.33,"
            "net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.3 "
            "/home/jovyan/work/l.py"
        )
    )

    task1 >> task2 >> task3 >> task4
