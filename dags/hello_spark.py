# dags/mon_pipeline.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id='pipeline_traitement_spark',
    start_date=datetime(2024, 1, 1),
    schedule_interval='*/5 * * * *', 
    catchup=False,
    tags=['spark', 'batch_processing']
) as dag:

    # 1. Le point de départ
    start = EmptyOperator(
        task_id='start'
    )

    # 2. Ta tâche de calcul
    executer_spark = BashOperator(
        task_id='lancer_job_spark',
        bash_command='python3 /opt/airflow/jobs/process_data.py'
    )

    # 3. Le point d'arrivée
    end = EmptyOperator(
        task_id='end'
    )

    # 4. LA DÉFINITION DU FLUX
    start >> executer_spark >> end