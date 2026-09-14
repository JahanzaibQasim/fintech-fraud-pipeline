from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="fintech_dbt_transformation_pipeline",
    default_args=default_args,
    description="Orchestrates dbt transformations and tests inside Linux container",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["fintech", "dbt", "bigquery"],
) as dag:

    # Install dbt-bigquery on the fly if missing, then run dbt models
    run_dbt_models = BashOperator(
        task_id="dbt_run",
        bash_command="pip install --no-cache-dir dbt-bigquery && cd /opt/airflow/fintech_dbt && dbt run",
    )

    # Execute dbt tests
    run_dbt_tests = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/fintech_dbt && dbt test",
    )

    run_dbt_models >> run_dbt_tests