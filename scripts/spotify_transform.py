import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, round, desc

def run_spotify_transformation(input_path, target_table, temp_gcs_bucket):
    """
    Pipeline ETL PySpark : Nettoyage et Agrégation des données d'albums Spotify.
    """
    # Initialisation de la session Spark
    spark = SparkSession.builder \
        .appName("Spotify-Performance-Analytics") \
        .getOrCreate()

    print(f"🚀 Début du traitement Spark pour : {input_path}")

    try:
        # 1. Lecture du dataset CSV source
        df_tracks = spark.read.csv(input_path, header=True, inferSchema=True)

        # 2. Nettoyage & Qualité des données (Data Quality)
        df_clean = df_tracks.filter(col("id").isNotNull()) \
            .withColumn("duration_ms", col("duration").cast("float")) \
            .withColumn("popularity_score", col("popularity").cast("float")) \
            .dropDuplicates(["id"])

        # 3. Calcul des indicateurs métiers (Feature Engineering)
        df_album_stats = df_clean.groupBy("album_id").agg(
            round(avg("duration_ms"), 2).alias("avg_track_duration_ms"),
            round(avg("popularity_score"), 2).alias("album_popularity_index"),
            count("id").alias("number_of_tracks")
        ).filter(col("album_id").isNotNull()) \
         .orderBy(desc("album_popularity_index"))

        # 4. Écriture des résultats vers l'entrepôt BigQuery
        df_album_stats.write.format("bigquery") \
            .option("temporaryGcsBucket", temp_gcs_bucket) \
            .option("table", target_table) \
            .mode("overwrite") \
            .save()

        print(f"✅ Transformation terminée avec succès. Données envoyées vers {target_table}")

    except Exception as e:
        print(f"❌ Erreur critique durant le job Spark : {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: spotify_transform.py <input_path> <target_table> <temp_gcs_bucket>")
        sys.exit(-1)

    run_spotify_transformation(sys.argv[1], sys.argv[2], sys.argv[3])