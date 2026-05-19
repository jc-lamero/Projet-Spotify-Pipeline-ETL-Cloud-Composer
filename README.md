# 🎵 Pipeline ETL Spotify Trends Analysis - Cloud Composer & PySpark

Ce projet démontre la conception et le déploiement d'un pipeline ETL automatisé et orchestré sur **Google Cloud Platform (GCP)** pour analyser la performance et les tendances des albums Spotify. Il s'inscrit dans la continuité de ma montée en compétences sur l'industrialisation des workflows Data Engineering.

## 🛠️ Architecture Technique & Technologies
* **Orchestration :** Cloud Composer (Apache Airflow 2).
* **Traitement Distribué (Calcul) :** PySpark via un cluster éphémère Google Cloud Dataproc.
* **Stockage Source (Data Lake) :** Google Cloud Storage (GCS).
* **Entrepôt de Données (Data Warehouse) :** Google BigQuery.
* **Sécurité & Droits :** Google Cloud IAM (Gestion fine des privilèges des comptes de service).

---

## 🚀 Réalisations Techniques

### 1. Configuration & Sécurisation de l'Infrastructure GCP
* Activation des API Cloud Native indispensables (`Composer`, `Dataproc`, `BigQuery`, `Storage`) via la console et gcloud CLI.
* Application du principe de moindre privilège dans **Google Cloud IAM** : Configuration d'un compte de service dédié avec les rôles nécessaires et suffisants pour l'exécution automatique :
    * `Nœud de calcul Composer` (Composer Worker)
    * `Administrateur Storage`
    * `Administrateur BigQuery`

### 2. Conception du Modèle de Données & Ingestion (GCS)
* Création d'une structure de stockage organisée sur Google Cloud Storage (`/raw-data`, `/scripts`, `/temp`).
* Préparation de l'ingestion d'un échantillon du dataset relationnel Spotify lié aux pistes musicales (`track.csv`).

### 3. Développement du Script de Transformation (PySpark)
Écriture d'un script PySpark (`spotify_transform.py`) optimisé pour le traitement distribué :
* **Data Quality & Nettoyage :** Filtrage des valeurs manquantes (`Nulls`), casting explicite des types de données (`duration` en float, `popularity` en float) et déduplication stricte basée sur les identifiants uniques (`id`).
* **Feature Engineering :** Agrégation par `album_id` pour calculer l'index de popularité moyen d'un album (`album_popularity_index`), la durée moyenne des pistes (`avg_track_duration_ms`) et le volume total de morceaux (`number_of_tracks`).
* **Persistance :** Exportation des insights calculés de manière optimisée directement dans une table cible de l'entrepôt BigQuery en utilisant un bucket GCS temporaire.

### 4. Automatisation & Orchestration (Apache Airflow DAG)
Développement du workflow d'orchestration (`spotify_dag.py`) matérialisé par un Graphe Orienté Acyclique (DAG) comprenant :
* **`DataprocCreateClusterOperator` :** Initialisation et provisionnement automatique d'un cluster Spark Dataproc.
* **`DataprocSubmitJobOperator` :** Soumission dynamique du script PySpark en lui passant les arguments requis (chemins d'entrée/sortie).
* **`DataprocDeleteClusterOperator` :** Destruction systématique du cluster Dataproc dès la fin du traitement (grâce à la règle `trigger_rule='all_done'`), assurant une gestion rigoureuse des coûts d'infrastructure.

---

## 💡 Apprentissages Clés & Compétences FinOps (Gestion des Coûts)

Durant ce projet, j'ai été confronté à deux défis majeurs de l'ingénierie cloud qui ont enrichi mon expérience :

1.  **Gestion de la disponibilité des ressources (Erreur 503 GCE) :** Face à une indisponibilité matérielle temporaire de serveurs dans une zone GCP (`us-central1-c`), j'ai su adapter dynamiquement le DAG Airflow pour réduire la taille des machines virtuelles (`n1-standard-1`) et basculer l'infrastructure sur une région alternative (`us-east1`) tout en laissant Google gérer l'allocation automatique de la zone.
2.  **Sensibilisation FinOps (Contrôle du Budget Cloud) :** Cloud Composer (Airflow managé) maintient des services actifs 24h/24 (Kubernetes, SQL proxy, serveurs Web). Suite à une hausse rapide de la facturation liée aux frais fixes du service, j'ai appliqué les protocoles de secours et de gestion des coûts en coupant les ressources, en isolant puis en supprimant le projet, et en désactivant la facturation pour sécuriser l'environnement.

Ces imprévus m'ont permis de comprendre concrètement les réalités budgétaires du Cloud Computing et l'importance d'une surveillance stricte des coûts (*FinOps*), une compétence hautement valorisée en entreprise pour un Data Engineer.