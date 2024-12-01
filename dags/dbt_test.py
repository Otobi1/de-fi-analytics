from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 1, 1),
}

with DAG('dbt_docker_dag', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:
    run_dbt = DockerOperator(
        task_id='run_dbt',
        image='gcr.io/sapient-hub-442421-b5/de-fi-app:latest',
        command='dbt run',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        volumes=['/path/on/host:/path/in/container'],  # Adjust as needed
    )
