from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    # Initialisation de la session Spark
    spark = SparkSession.builder \
        .appName("TraitementBatteries_Medallion") \
        .getOrCreate()

    # Définition des chemins (faisant écho aux volumes configurés via Docker)
    chemin_bronze_json = "/opt/airflow/data/source_brute.json"
    chemin_silver_csv = "/opt/airflow/data/silver_batteries_vehicules"
    
    print(f"[BRONZE] Lecture des données brutes depuis {chemin_bronze_json}...")
    
    # Lecture du format JSON. 
    # L'option multiline=True est souvent nécessaire si le JSON est formaté sur plusieurs lignes.
    df_bronze = spark.read \
        .option("multiline", "true") \
        .json(chemin_bronze_json)
    
    print("Aperçu de la couche Bronze :")
    df_bronze.show(5)

    # --- Étape de transformation : De Bronze vers Silver ---
    print("[SILVER] Nettoyage et préparation des données...")
    
    # Exemple de nettoyage : on s'assure que les indicateurs critiques de santé (SOH) 
    # ou de tension (BMS) ne sont pas nuls avant de les passer à la couche suivante.
    df_silver = df_bronze.dropna(subset=["vin"])
    
    # Tu peux ajouter ici d'autres transformations (cast des types, renommage de colonnes, etc.)
    # df_silver = df_silver.withColumn("soh_mesure_pct", col("soh_mesure_pct").cast("double"))

    # Sauvegarde de la couche Silver en CSV
    print(f"[SILVER] Écriture des données nettoyées au format CSV vers {chemin_silver_csv}...")
    
    # L'écriture se fait dans un dossier. Spark va générer des fichiers partiels.
    df_silver.write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(chemin_silver_csv)
    
    print("Traitement de la couche Silver terminé avec succès !")
    
    spark.stop()

if __name__ == "__main__":
    main()