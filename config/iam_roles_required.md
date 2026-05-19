# 🔐 Configuration de la Sécurité & Rôles IAM (GCP)

Dans le cadre du déploiement du pipeline ETL Spotify, la gestion des accès a été configurée selon le **principe de moindre privilège** (Principle of Least Privilege - PoLP). Au lieu d'utiliser des droits d'administrateur globaux, un compte de service dédié a été configuré avec des rôles granulaires stricts.

---

## 👤 Compte de Service Cible
Le pipeline utilise le compte de service Compute Engine par défaut (ou un compte de service personnalisé dédié) pour permettre à l'orchestrateur **Cloud Composer (Apache Airflow)** d'interagir de manière autonome avec les autres services GCP :

* **Format de l'identifiant :** `[PROJECT_NUMBER]-compute@developer.gserviceaccount.com`

---

## 🛠️ Rôles IAM Requis et Justifications

Pour que le DAG Airflow s'exécute sans erreur, les trois rôles Cloud native suivants ont été explicitement rattachés au compte de service dans la console **IAM & Administration** :

| Service | Nom du Rôle GCP (Console) | Identifiant Technique du Rôle | Justification Métier / Technique |
| :--- | :--- | :--- | :--- |
| **Cloud Composer** | Nœud de calcul Composer | `roles/composer.worker` | Permet aux workers d'Airflow d'exécuter les tâches du DAG, de synchroniser les logs et de communiquer avec l'environnement géré. |
| **Cloud Storage** | Administrateur Storage | `roles/storage.admin` | Requis pour lire le fichier source (`track.csv`), récupérer le script PySpark (`spotify_transform.py`) et utiliser l'espace `/temp` comme zone tampon. |
| **BigQuery** | Administrateur BigQuery | `roles/bigquery.admin` | Nécessaire pour permettre au connecteur Spark-BigQuery de créer la table cible (`album_stats`) et d'y injecter les données agrégées en mode `overwrite`. |

---

## 🔍 Validation de la Configuration

La validation des permissions a été validée lors de la phase d'initialisation du DAG. L'absence de ces rôles (notamment `roles/composer.worker`) provoque une erreur d'initialisation de l'infrastructure dès la première tâche (`DataprocCreateClusterOperator`). 

*Note : En environnement de production d'entreprise, le rôle `roles/bigquery.admin` et `roles/storage.admin` pourraient être encore plus restreints à des buckets et datasets spécifiques (rôles d'édition/lecture seule) pour verrouiller totalement l'environnement.*
