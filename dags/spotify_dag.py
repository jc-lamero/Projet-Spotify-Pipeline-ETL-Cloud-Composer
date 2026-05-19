from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator
)
from datetime import datetime, timedelta

# Variables d'environnement GCP
PROJECT_ID = "spotify-trends-analysis-495611" 
REGION = "us-east1"  # Modifié suite à l'erreur 503 (Ressources indisponibles)
CLUSTER_NAME = "spotify-transform-cluster-{{ ds_nodash }}"
BUCKET_NAME = "spotify-analysis-data-jc-lamero"
PYSPARK_SCRIPT = f"gs://{BUCKET_NAME}/scripts/spotify_transform.py"

default_args = {
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 7),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'spotify_trends_analysis_v1',
    default_args=default_args,
    description='Pipeline ETL Spotify orchestré par Airflow sur Dataproc',
    schedule_interval=None,
    catchup=False,
) as dag:

    # 1. Création du cluster Dataproc (Taille réduite pour optimiser le budget et les quotas)
    create_cluster = DataprocCreateClusterOperator(
        task_id='create_dataproc_cluster',
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        cluster_config={
            "master_config": {"num_instances": 1, "machine_type_uri": "n1-standard-1"},
            "worker_config": {"num_instances": 2, "machine_type_uri": "n1-standard-1"},
            "gce_cluster_config": {
                "zone_uri": ""  # Allocation dynamique de la zone par GCP
            }
        },
    )

    # 2. Soumission du Job PySpark avec passage d'arguments dynamiques
    submit_pyspark = DataprocSubmitJobOperator(
        task_id='submit_pyspark_job',
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": PYSPARK_SCRIPT,
                "args": [
                    f"gs://{BUCKET_NAME}/raw-data/track.csv",
                    f"{PROJECT_ID}.spotify_dataset.album_stats",
                    f"{BUCKET_NAME}/temp"
                ],
            },
        },
    )

    # 3. Destruction systématique du cluster (Règle all_done pour éviter les frais résiduels)
    delete_cluster = DataprocDeleteClusterOperator(
        task_id='delete_dataproc_cluster',
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule='all_done'
    )

    # Arbre de dépendances des tâches
    create_cluster >> submit_pyspark >> delete_cluster
