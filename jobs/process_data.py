import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Configuration du logging natif Python
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    spark = SparkSession.builder \
        .appName("TraitementBatteries_Medallion") \
        .getOrCreate()

    chemin_bronze_json = "/opt/airflow/data/source_brute.json"
    chemin_silver_parquet = "/opt/airflow/data/silver_batteries_vehicules"
    
    # 1. Définition stricte du schéma attendu
    schema_batterie = StructType([
        StructField("vin", StringType(), True),
        StructField("soh_mesure_pct", DoubleType(), True),
        # Ajoute ici les autres colonnes de ton JSON (ex: voltage_12v_v, temperature...)
    ])

    logger.info(f"[BRONZE] Lecture des données brutes depuis {chemin_bronze_json}")
    
    # Lecture avec le schéma imposé
    df_bronze = spark.read \
        .option("multiline", "true") \
        .schema(schema_batterie) \
        .json(chemin_bronze_json)

    logger.info("[SILVER] Nettoyage : Suppression des valeurs nulles sur le VIN et SOH")
    df_silver = df_bronze.dropna(subset=["vin", "soh_mesure_pct"])

    logger.info(f"[SILVER] Écriture des données au format PARQUET vers {chemin_silver_parquet}")
    
    # Sauvegarde en Parquet au lieu de CSV
    df_silver.write \
        .mode("overwrite") \
        .parquet(chemin_silver_parquet)
    
    logger.info("Traitement de la couche Silver terminé avec succès !")
    spark.stop()

if __name__ == "__main__":
    main()