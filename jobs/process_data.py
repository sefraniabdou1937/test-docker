import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    spark = SparkSession.builder.appName("TraitementBatteries_Medallion").getOrCreate()

    chemin_bronze_json = "/opt/airflow/data/source_brute.json"
    chemin_silver_parquet = "/opt/airflow/data/silver_batteries_vehicules"
    
    logger.info(f"[BRONZE] Lecture des données brutes depuis {chemin_bronze_json}")
    
    # On utilise inferSchema pour capturer toutes les colonnes (modele, id_sav, dtc, etc.)
    df_bronze = spark.read \
        .option("multiline", "true") \
        .option("inferSchema", "true") \
        .json(chemin_bronze_json)

    logger.info("[SILVER] Nettoyage : Suppression des valeurs nulles sur le VIN et SOH")
    df_silver = df_bronze.dropna(subset=["vin", "soh_mesure_pct"])

    logger.info("[SILVER] Cast des types pour les mesures de batteries")
    # On s'assure que les indicateurs sont bien des nombres (Double) pour les calculs Gold
    df_silver = df_silver \
        .withColumn("soh_mesure_pct", col("soh_mesure_pct").cast("double")) \
        .withColumn("nb_cycles_reels", col("nb_cycles_reels").cast("double")) \
        .withColumn("kilometrage_total", col("kilometrage_total").cast("double")) \
        .withColumn("age_vehicule_annees", col("age_vehicule_annees").cast("double"))

    logger.info(f"[SILVER] Écriture des données au format PARQUET vers {chemin_silver_parquet}")
    df_silver.write.mode("overwrite").parquet(chemin_silver_parquet)
    
    logger.info("Traitement de la couche Silver terminé avec succès !")
    spark.stop()

if __name__ == "__main__":
    main()