import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    spark = SparkSession.builder.appName("TraitementBatteries_Medallion").getOrCreate()

    # 1. Utilisation des variables dynamiques du fichier .env
    chemin_bronze_json = os.getenv("BRONZE_DATA_PATH", "/opt/airflow/data/source_brute.json")
    chemin_silver_parquet = os.getenv("SILVER_DATA_PATH", "/opt/airflow/data/silver_batteries_vehicules")
    
    logger.info(f"[BRONZE] Lecture des données brutes depuis {chemin_bronze_json}")
    
    # 2. Ingestion automatique du schéma
    df_bronze = spark.read \
        .option("multiline", "true") \
        .option("inferSchema", "true") \
        .json(chemin_bronze_json)

    logger.info("[SILVER] Nettoyage : Suppression des valeurs nulles sur le VIN et SOH")
    df_silver = df_bronze.dropna(subset=["vin", "soh_mesure_pct"])

    logger.info("[SILVER] Cast des types pour les mesures globales et la télémétrie BMS")
    
    # 3. Sécurisation des types numériques pour la BI
    df_silver = df_silver \
        .withColumn("soh_mesure_pct", col("soh_mesure_pct").cast("double")) \
        .withColumn("nb_cycles_reels", col("nb_cycles_reels").cast("double")) \
        .withColumn("kilometrage_total", col("kilometrage_total").cast("double")) \
        .withColumn("age_vehicule_annees", col("age_vehicule_annees").cast("double")) \
        .withColumn("voltage_12v_v", col("voltage_12v_v").cast("double")) \
        .withColumn("temperature_bms_c", col("temperature_bms_c").cast("double")) \
        .withColumn("ipack_charge_a", col("ipack_charge_a").cast("double")) \
        .withColumn("vpack_v", col("vpack_v").cast("double")) \
        .withColumn("vcell_v", col("vcell_v").cast("double"))

    logger.info(f"[SILVER] Écriture des données au format PARQUET vers {chemin_silver_parquet}")
    
    df_silver.write.mode("overwrite").parquet(chemin_silver_parquet)
    
    logger.info("Traitement de la couche Silver terminé avec succès !")
    spark.stop()

if __name__ == "__main__":
    main()