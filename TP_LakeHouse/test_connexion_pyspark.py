#!/usr/bin/env python3
"""
Test de connexion PySpark -> PostgreSQL (Windows-friendly)

Instructions:
- Remplacez `JAR_PATH` par le chemin vers votre driver PostgreSQL si nécessaire.
- Exécutez avec votre venv activé :
    python test_connexion_pyspark_fixed.py
  ou, si vous préférez, utilisez spark-submit :
    spark-submit --jars C:/TP_DataWarehouse/drivers/postgresql-42.7.8.jar test_connexion_pyspark_fixed.py
"""

import os
import sys
import traceback
from pathlib import Path

# Diagnostics & versions
try:
    import importlib.metadata as importlib_metadata
except Exception:
    import importlib_metadata

def get_pkg_version(pkg_names):
    for name in pkg_names:
        try:
            return importlib_metadata.version(name)
        except Exception:
            continue
    return "unknown"

# --- CONFIGURATION UTILISATEUR ---
# Chemin vers le driver PostgreSQL (Windows). Remplacez-le si besoin.
JAR_PATH = r"D:\TP_DataWarehouse\drivers\postgresql-42.7.8.jar"

# Connexion PostgreSQL (modifiez user/password/host/db si nécessaire)
POSTGRES_CONFIG = {
    "url": "jdbc:postgresql://localhost:5432/postgres",
    "dbtable": "pg_database",
    "user": "postgres",
    "password": "1234",
    "driver": "org.postgresql.Driver"
}
# -------------------------------

def to_file_uri(path_str):
    """
    Convertit un chemin local en URI file:/// approprié pour Spark/Hadoop (cross-platform).
    Utilise pathlib.Path.as_uri() qui fonctionne sur Windows et *nix.
    """
    p = Path(path_str).expanduser().resolve()
    return p.as_uri(), p

def print_env_info(jar_path, jar_exists):
    print("=== Environnement / diagnostics ===")
    print("Platform:", sys.platform)
    print("Python executable:", sys.executable)
    print("JAVA_HOME:", os.environ.get("JAVA_HOME", "<not set>"))
    print("SPARK_HOME:", os.environ.get("SPARK_HOME", "<not set>"))
    print("Path to JDBC jar:", jar_path)
    print("JAR exists on filesystem:", jar_exists)
    print("pyspark version:", get_pkg_version(["pyspark"]))
    # Delta python package often named delta-spark on PyPI
    print("delta package (PyPI name 'delta-spark') version:", get_pkg_version(["delta-spark", "delta"]))
    print("psycopg2 version:", get_pkg_version(["psycopg2", "psycopg2-binary"]))
    print("pandas version:", get_pkg_version(["pandas"]))
    print("====================================\n")

def main():
    # Import PySpark and Delta helpers lazily to show env info before JVM launch.
    try:
        from pyspark.sql import SparkSession
    except Exception as e:
        print("Erreur d'import de pyspark. Vérifiez votre venv et installation.")
        traceback.print_exc()
        return

    try:
        # delta.configure helper (may be missing if delta python package not installed;
        # configure_spark_with_delta_pip is available when delta-python package is installed)
        from delta import configure_spark_with_delta_pip
    except Exception:
        # We'll still try to set delta configs manually, but prefer configure_spark_with_delta_pip
        configure_spark_with_delta_pip = None

    # Convert the local JAR path to file URI to avoid "No FileSystem for scheme 'C'" on Windows.
    jar_uri, jar_file = to_file_uri(JAR_PATH)
    jar_exists = jar_file.exists()

    print_env_info(jar_uri, jar_exists)

    if not jar_exists:
        print("ATTENTION: Le fichier JAR n'existe pas au chemin indiqué.")
        print(" - Vérifiez JAR_PATH en haut du script.")
        print(" - Vous pouvez aussi exécuter via spark-submit avec --jars pour bypasser ce chemin.")
        # We continue; Spark may still start if jar provided elsewhere (spark-submit, SPARK_HOME/jars, ...)
    try:
        # Construire le builder Spark
        builder = SparkSession.builder \
            .appName("Test Connexion PostgreSQL") \
            .master("local[*]")

        # Utiliser URI file:/// pour spark.jars et driver classpath (important sur Windows)
        builder = builder.config("spark.jars", jar_uri)
        builder = builder.config("spark.driver.extraClassPath", jar_uri)

        # (Optionnel) configs Delta (si vous comptez utiliser Delta)
        # Si configure_spark_with_delta_pip est disponible, on l'utilise (téléchargera les jars Delta adaptés).
        # Sinon on ajoute les extensions (nécessite que les jars Delta soient disponibles via spark.jars ou SPARK_HOME/jars)
        builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        # Finaliser / démarrer Spark (utilise configure_spark_with_delta_pip si présent)
        if configure_spark_with_delta_pip is not None:
            # Cette fonction modifie le builder pour ajouter les dépendances Python/Java nécessaires à Delta
            spark = configure_spark_with_delta_pip(builder).getOrCreate()
        else:
            spark = builder.getOrCreate()

        print("Session Spark créée avec succès")
        print("Version de Spark:", spark.version)
        print("Spark master:", spark.sparkContext.master)
        print()

        # Tentative de lecture via JDBC
        print("Tentative de lecture JDBC depuis PostgreSQL...")
        df = spark.read.format("jdbc").options(**POSTGRES_CONFIG).load()

        # Action pour forcer la connexion (compte les lignes)
        n = df.count()
        print(f"Connexion réussie ! Nombre de lignes lues: {n}")

        print("\nSchéma de la table :")
        df.printSchema()

        print("\nAperçu (5 lignes) :")
        df.show(5, truncate=False)

        print("\n=== TEST RÉUSSI: PySpark peut lire depuis PostgreSQL ===")

    except Exception as e:
        print("\nERREUR lors de la connexion / démarrage Spark :")
        # Afficher trace complète pour aider au debug (incluant la cause Java gateway exited)
        traceback.print_exc()
        print("\nConseils rapides si vous voyez 'No FileSystem for scheme \"C\"' :")
        print(" - Assurez-vous que spark.jars utilise une URI file:/// (ce script le fait).")
        print(" - Essayez d'exécuter avec spark-submit --jars C:/path/to/driver.jar")
        print(" - Vérifiez JAVA_HOME et utilisez une version de Java supportée par Spark (Java 11/17 selon Spark).")
        print(" - Si 'Java gateway process exited before sending its port number', regardez les logs JVM en début de sortie pour la cause.")
    finally:
        # Arrêter Spark proprement s'il a été créé
        try:
            if 'spark' in locals() and spark is not None:
                spark.stop()
                print("\nSession Spark arrêtée.")
        except Exception:
            pass

if __name__ == "__main__":
    main()