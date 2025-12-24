from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

CONFIG_DIR = Path(__file__).parent.parent.parent / "configs" / "economic-calendar"

with DAG(
    dag_id="economic_calendar_fxstreet",
    default_args=default_args,
    description="Extract economic calendar data from fxstreet",
    start_date=datetime(2020, 1, 1),
    catchup=False,
    tags=["economic-calendar", "fxstreet"],
) as dag:

    run_start = datetime.now().strftime("%Y%m%d")
    run_end = (datetime.now() + timedelta(days=1)).strftime("%Y%m%d")

    extract_task = DockerOperator(
        task_id="extract_fxstreet_events",
        image="lean-dev:latest",
        docker_conn_id="docker_default",
        network_mode="bridge",
        command=[
            "-s", run_start,
            "-e", run_end,
            "-c", "/app/config/fxstreet.yaml",
            "--log-config", "/app/config/log.conf",
        ],
        volumes=[f"{CONFIG_DIR}:/app/config:ro"],
    )

