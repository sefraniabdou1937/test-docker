# jobs/process_data.py
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("TraitementBatteries").getOrCreate()

    # Le chemin est celui à l'intérieur du conteneur (défini dans le docker-compose)
    path = "/opt/airflow/data/historique_complet_vehicules.csv"
    
    print(f"Lecture des données depuis {path}...")
    df = spark.read.csv(path, header=True, inferSchema=True)
    
    # Exemple de petit traitement
    df.show()
    print("Traitement terminé !")
    
    spark.stop()

if __name__ == "__main__":
    main()