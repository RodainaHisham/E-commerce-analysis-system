from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="E_Pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 22 * * *", 
    catchup=False
) as dag:

    task0 = BashOperator(
        task_id="Simulate_Streaming",
        bash_command=(
            "docker exec spark-jupyter "
            "jupyter nbconvert --to notebook --execute --inplace "
            "/home/jovyan/work/Simulation.ipynb"
        )
    )  

    task1 = BashOperator(
        task_id="Copy_to_HDFS",
        bash_command=(
            "rm -rf /tmp/stream/ /tmp/static/ && "
            "mkdir -p /tmp/stream/ /tmp/static/ && "

            "docker exec -e HADOOP_USER_NAME=root hadoop-namenode "
            "sh -c '/opt/hadoop-3.2.1/bin/hdfs dfs -mkdir -p hdfs://hadoop-namenode:9000/user/jovyan/raw_ecommerce/stream/' && "

            "docker exec -e HADOOP_USER_NAME=root hadoop-namenode "
            "sh -c '/opt/hadoop-3.2.1/bin/hdfs dfs -mkdir -p hdfs://hadoop-namenode:9000/user/jovyan/raw_ecommerce/static/' && "

            "docker cp spark-jupyter:/home/jovyan/data/raw_ecommerce/stream/. /tmp/stream/ && "
            "docker cp spark-jupyter:/home/jovyan/data/raw_ecommerce/static/. /tmp/static/ && "

            "docker cp /tmp/stream/. hadoop-namenode:/tmp/stream/ && "
            "docker cp /tmp/static/. hadoop-namenode:/tmp/static/ && "

            "docker exec -e HADOOP_USER_NAME=root hadoop-namenode "
            "sh -c '/opt/hadoop-3.2.1/bin/hdfs dfs -put -f /tmp/stream/*.json hdfs://hadoop-namenode:9000/user/jovyan/raw_ecommerce/stream/' && "

            "docker exec -e HADOOP_USER_NAME=root hadoop-namenode "
            "sh -c '/opt/hadoop-3.2.1/bin/hdfs dfs -put -f /tmp/static/*.json hdfs://hadoop-namenode:9000/user/jovyan/raw_ecommerce/static/'"
        )
    )

    task2 = BashOperator(
        task_id="Extracting_data",
        bash_command=(
            "docker exec -e HADOOP_USER_NAME=root spark-jupyter "
            "spark-submit /home/jovyan/work/e.py"
        )
    )

    task3 = BashOperator(
        task_id="Transformation",
        bash_command=(
            "docker exec -e HADOOP_USER_NAME=root spark-jupyter "
            "spark-submit /home/jovyan/work/t.py"
        )
    )

    task4 = BashOperator(
        task_id="Archive_raw_files_to_HDFS",
        bash_command=(
            "rm -rf /tmp/archive_stream/ /tmp/archive_static/ && "
            "mkdir -p /tmp/archive_stream/ /tmp/archive_static/ && "

            "docker exec -e HADOOP_USER_NAME=root hadoop-namenode "
            "sh -c '/opt/hadoop-3.2.1/bin/hdfs dfs -mkdir -p /user/root/datalake/bronze/archived/stream/' && "

            "docker exec -e HADOOP_USER_NAME=root hadoop-namenode "
            "sh -c '/opt/hadoop-3.2.1/bin/hdfs dfs -mkdir -p /user/root/datalake/bronze/archived/static/' && "

            "docker cp spark-jupyter:/home/jovyan/data/raw_ecommerce/stream/. /tmp/archive_stream/ && "
            "docker cp spark-jupyter:/home/jovyan/data/raw_ecommerce/static/. /tmp/archive_static/ && "

            "docker cp /tmp/archive_stream/. hadoop-namenode:/tmp/archive_stream/ && "
            "docker cp /tmp/archive_static/. hadoop-namenode:/tmp/archive_static/ && "

            "docker exec -e HADOOP_USER_NAME=root hadoop-namenode "
            "sh -c '/opt/hadoop-3.2.1/bin/hdfs dfs -put -f /tmp/archive_stream/*.json /user/root/datalake/bronze/archived/stream/ || true' && "

            "docker exec -e HADOOP_USER_NAME=root hadoop-namenode "
            "sh -c '/opt/hadoop-3.2.1/bin/hdfs dfs -put -f /tmp/archive_static/*.json /user/root/datalake/bronze/archived/static/ || true' && "

            "docker exec spark-jupyter sh -c 'rm -f /home/jovyan/data/raw_ecommerce/stream/*.json || true' && "
            "docker exec spark-jupyter sh -c 'rm -f /home/jovyan/data/raw_ecommerce/static/*.json || true'"
        )
    )

    task5 = BashOperator(
        task_id="Loading",
        bash_command=(
            "docker exec -e HADOOP_USER_NAME=root spark-jupyter "
            "spark-submit "
            "--packages net.snowflake:snowflake-jdbc:3.13.33,"
            "net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.3 "
            "/home/jovyan/work/l.py"
        )
    )


    task0 >> task1 >> task2 >> task3 >> task4 >> task5
