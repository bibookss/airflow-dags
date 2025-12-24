from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

# REQUIRED Airflow Variables (must exist)
IMAGE = Variable.get("ECON_CAL_IMAGE")
DOCKER_NETWORK = Variable.get("DOCKER_NETWORK")

with DAG(
    dag_id="economic_calendar_fxstreet",
    description="Extract economic calendar data from fxstreet",
    default_args=default_args,
    start_date=datetime(2020, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["economic-calendar", "fxstreet"],
) as dag:

    extract_task = DockerOperator(
        task_id="extract_fxstreet_events",
        image=IMAGE,
        docker_url="unix://var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        mount_tmp_dir=False,
        command=[
            "-s", "{{ ds_nodash }}",
            "-e", "{{ macros.ds_add(ds, 1) | replace('-', '') }}",
        ],
        environment={
            "S3_ENDPOINT": Variable.get("MINIO_ENDPOINT"), 
            "S3_ACCESS_KEY": Variable.get("MINIO_ACCESS_KEY"),
            "S3_SECRET_KEY": Variable.get("MINIO_SECRET_KEY"),
        },
    )

