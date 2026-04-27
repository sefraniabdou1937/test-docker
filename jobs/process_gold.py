import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hash, date_format

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    spark = SparkSession.builder \
        .appName("TraitementBatteries_Gold_StarSchema") \
        .getOrCreate()

    # Utilisation des variables dynamiques du fichier .env
    chemin_silver = os.getenv("SILVER_DATA_PATH", "/opt/airflow/data/silver_batteries_vehicules")
    chemin_gold = os.getenv("GOLD_DATA_PATH", "/opt/airflow/data/gold")
    
    logger.info(f"Lecture des données Silver depuis {chemin_silver}...")
    df_silver = spark.read.parquet(chemin_silver)

    # ==========================================
    # 1. CRÉATION DES TABLES DE DIMENSIONS
    # ==========================================
    
    # Dim_Vehicule : On garde uniquement les colonnes du véhicule et on dédoublonne
    logger.info("Création de Dim_Vehicule...")
    dim_vehicule = df_silver.select("vin", "modele", "date_production").distinct()
    dim_vehicule.write.mode("overwrite").parquet(f"{chemin_gold}/dim_vehicule")

    # Dim_SAV : Informations sur le centre d'intervention
    logger.info("Création de Dim_SAV...")
    dim_sav = df_silver.select("id_sav", "ville", "coordonnees_gps").distinct()
    dim_sav.write.mode("overwrite").parquet(f"{chemin_gold}/dim_sav")

    # Dim_Diagnostic_Intervention : Regroupement des statuts et génération d'une clé technique (Surrogate Key)
    logger.info("Création de Dim_Diagnostic_Intervention...")
    colonnes_diag = [
        "dtc", "dtc_bms", "dtc_current", "dtc_voltage", 
        "type_planification", "flag_anomalie_cachee", 
        "garantie_vehicule_active", "garantie_batterie_active"
    ]
    # La fonction hash() crée un identifiant unique (id_diagnostic) basé sur la combinaison de ces colonnes
    dim_diagnostic = df_silver.select(*colonnes_diag).distinct() \
        .withColumn("id_diagnostic", hash(*colonnes_diag))
    dim_diagnostic.write.mode("overwrite").parquet(f"{chemin_gold}/dim_diagnostic")

    # ==========================================
    # 2. CRÉATION DE LA TABLE DE FAITS
    # ==========================================
    logger.info("Création de la table de faits Fait_Releve_Batterie...")
    
    # Préparation : on calcule les clés étrangères manquantes (id_diagnostic et formatage des dates en entier YYYYMMDD)
    fait_prepare = df_silver \
        .withColumn("id_diagnostic", hash(*colonnes_diag)) \
        .withColumn("date_intervention_id", date_format(col("date_intervention"), "yyyy-MM-dd").cast("int")) \
        .withColumn("date_planifiee_id", date_format(col("date_planifiee_prochaine"), "yyyy-MM-dd").cast("int"))

    # Sélection stricte des clés et des métriques
    colonnes_faits = [
        "id_dossier", "vin", "id_sav", "id_diagnostic", 
        "date_intervention_id", "date_planifiee_id",
        "soh_mesure_pct", "nb_cycles_reels", "kilometrage_total", "age_vehicule_annees",
        "ratio_dc_fast_charge", "delai_recommande_jours", "voltage_12v_v", 
        "temperature_bms_c", "vpack_v", "vcell_v", "vcell_delta_v", 
        "vdelta_cell_pack", "tcell_c", "ipack_charge_a", "ipack_discharge_a", "ipack_regen_a"
    ]
    
    df_faits = fait_prepare.select(*colonnes_faits)
    
    # Sauvegarde finale
    df_faits.write.mode("overwrite").parquet(f"{chemin_gold}/fait_releve_batterie")

    logger.info("Modélisation en étoile terminée ! Couche Gold générée avec succès.")
    spark.stop()

if __name__ == "__main__":
    main()