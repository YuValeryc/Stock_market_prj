from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,   # debug thì không cần retry
}

with DAG(
    dag_id="stock_model_training_dag",
    default_args=default_args,
    description="Train stock prediction model every 5 minutes (DEBUG)",
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,   # QUAN TRỌNG
) as dag:

    train_model = BashOperator(
        task_id="train_spark_model",
        bash_command="""
        docker exec batch-layer \
        /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /app/src/train_model.py
        """
    )

    train_model
