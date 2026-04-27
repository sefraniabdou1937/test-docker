from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='pipeline_traitement_spark_v2',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='*/5 * * * *', 
    catchup=False,
    tags=['spark', 'medallion', 'silver']
) as dag:

    start = EmptyOperator(task_id='start')

    # 1. Le Sensor : Attend que le fichier source apparaisse dans le dossier
    attendre_fichier_bronze = FileSensor(
        task_id='attendre_fichier_bronze',
        filepath='/opt/airflow/data/source_brute.json',
        poke_interval=30, # Vérifie toutes les 30 secondes
        timeout=300 # S'arrête si rien n'arrive après 5 minutes
    )

    # 2. Le vrai lanceur Spark (remplace le BashOperator)
    lancer_job_spark = SparkSubmitOperator(
        task_id='lancer_job_spark',
        application='/opt/airflow/jobs/process_data.py',
        conn_id='spark_default', # Connexion locale par défaut
        verbose=False
    )

    end = EmptyOperator(task_id='end')

    # À ajouter juste après ta tâche lancer_job_spark existante

lancer_job_gold = SparkSubmitOperator(
    task_id='lancer_job_gold',
    application='/opt/airflow/jobs/process_gold.py',
    conn_id='spark_default',
    verbose=False
)

# Et on met à jour la ligne finale du flux d'exécution :
start >> attendre_fichier_bronze >> lancer_job_spark >> lancer_job_gold >> end

    # Ordre d'exécution
