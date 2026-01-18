# -*- coding: utf-8 -*-
import os
import shutil
import ctypes
from pathlib import Path

# ---------------------------
# Auto-detection / installation minimale des natives Hadoop (winutils / DLL)
# This will:
# - try to find an existing winutils.exe in common locations
# - copy native files to a simple target HADOOP path (D:\hadoop) for Spark to use
# - set HADOOP_HOME, HADOOP_COMMON_LIB_NATIVE_DIR, JAVA_TOOL_OPTIONS for the current process
# - attempt to load hadoop.dll to give a clear message if the Visual C++ runtime is missing
# ---------------------------

def find_winutils_in_candidates(candidates):
    for p in candidates:
        bin_path = Path(p)
        if bin_path.is_dir():
            candidate = bin_path / "winutils.exe"
            if candidate.exists():
                return str(bin_path.resolve())
    return None

def copy_native_files(src_bin, target_root):
    """
    Copy native files (winutils.exe, *.dll, *.lib) from src_bin into target_root\bin.
    Also copy other native files into target_root\lib\native if found.
    Returns target_bin path.
    """
    src_bin = Path(src_bin)
    target_bin = Path(target_root) / "bin"
    target_lib_native = Path(target_root) / "lib" / "native"
    target_bin.mkdir(parents=True, exist_ok=True)
    target_lib_native.mkdir(parents=True, exist_ok=True)

    # Copy everything from src_bin into target_bin
    for item in src_bin.iterdir():
        if item.is_file():
            if item.suffix.lower() in (".exe", ".dll", ".lib") or item.name.lower().startswith("winutils"):
                try:
                    shutil.copy2(item, target_bin / item.name)
                except Exception as e:
                    print(f"⚠️ Impossible de copier {item} -> {target_bin}: {e}")

    # If there's a lib/native sibling folder, copy its dlls too
    possible_native = src_bin.parent / "lib" / "native"
    if possible_native.exists():
        for item in possible_native.iterdir():
            if item.is_file() and item.suffix.lower() == ".dll":
                try:
                    shutil.copy2(item, target_lib_native / item.name)
                except Exception as e:
                    print(f"⚠️ Impossible de copier {item} -> {target_lib_native}: {e}")

    return str(target_bin.resolve())

# Candidate locations to search for winutils (add more if you have different layout)
candidates = []

# Honor any existing HADOOP_HOME in environment
if os.environ.get("HADOOP_HOME"):
    candidates.append(os.path.join(os.environ["HADOOP_HOME"], "bin"))

# Common paths you mentioned / typical locations
candidates += [
    r"D:\hadoop-3.4.1-src\dev-support\bin",
    r"D:\hadoop-3.4.1-src\bin",
    r"D:\hadoop-3.4.1-src\dev-support",
    r"D:\hadoop\bin",
    r"C:\hadoop\bin",
    r"D:\hadoop-3.4.1-src",
]

# Also try PATH lookup
which_winutils = shutil.which("winutils.exe")
if which_winutils:
    candidates.insert(0, str(Path(which_winutils).parent))

src_bin = find_winutils_in_candidates(candidates)

# Target consistent HADOOP location we will use
TARGET_HADOOP = Path(r"D:\hadoop")

if src_bin:
    print(f"✓ winutils trouvé dans : {src_bin}")
    print(f"→ Copie des natifs vers {TARGET_HADOOP}")
    target_bin = copy_native_files(src_bin, TARGET_HADOOP)
    HADOOP_DIR = str(TARGET_HADOOP)
else:
    # If not found, still set HADOOP_DIR to a sensible default and instruct user what to do
    print("✗ winutils.exe non trouvé automatiquement dans les emplacements courants.")
    print("Veuillez placer winutils.exe et les DLL natives Hadoop dans D:\\hadoop\\bin (ou indiquez le chemin dans la variable HADOOP_DIR ci-dessous).")
    # Keep existing path used earlier if you want to override
    HADOOP_DIR = r'D:\hadoop-3.4.1-src\dev-support'  # fallback to the path you originally used

# Set environment variables for this Python process (and for the JVM launched by this process)
os.environ['HADOOP_HOME'] = HADOOP_DIR
os.environ['HADOOP_COMMON_LIB_NATIVE_DIR'] = os.path.join(HADOOP_DIR, 'bin')
# Ensure JAVA_TOOL_OPTIONS contains the library path for the JVM started by PySpark
os.environ['JAVA_TOOL_OPTIONS'] = os.environ.get('JAVA_TOOL_OPTIONS', '') + ' -Djava.library.path=' + os.path.join(HADOOP_DIR, 'bin')

# Try to load a common native DLL to surface a helpful error if VC++ runtime is missing
hadoop_dll_path = Path(HADOOP_DIR) / "bin" / "hadoop.dll"
if hadoop_dll_path.exists():
    try:
        ctypes.WinDLL(str(hadoop_dll_path))
        print(f"✓ hadoop.dll chargé depuis {hadoop_dll_path}")
    except OSError as e:
        print(f"✗ Échec du chargement de {hadoop_dll_path} : {e}")
        print("  → Probablement un runtime Visual C++ manquant ou DLL incompatible.")
        print("  → Installez le 'Microsoft Visual C++ Redistributable for Visual Studio 2015-2022 (x64)'.")
else:
    # Maybe the native is named differently or not present; warn but continue
    print(f"⚠️ hadoop.dll introuvable dans {Path(HADOOP_DIR)/'bin'} — vérifiez que les DLL natives ont été copiées.")

# ---------------------------
# Spark ingestion script (rest of the code)
# ---------------------------

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import current_timestamp, lit

print("✓ Bibliothèques importées")

POSTGRES_CONFIG = {
    'url': 'jdbc:postgresql://localhost:5432/retailpro_dwh',
    'user': 'postgres',
    'password': '1234',  # changez si besoin
    'driver': 'org.postgresql.Driver'
}

BRONZE_PATH = 'D:/lakehouse/bronze'
print(f"✓ Configuration définie - Bronze path: {BRONZE_PATH}")

def creer_session_spark():
    print("\nCréation de la session Spark...")
    builder = SparkSession.builder \
        .appName("Bronze Ingestion") \
        .master("local[*]")

    # Préparer le driver JDBC local si disponible
    local_jar = r"D:\TP_DataWarehouse\drivers\postgresql-42.7.8.jar"
    if os.path.exists(local_jar):
        jar_uri = "file:///" + local_jar.replace('\\','/')
        builder = builder.config("spark.jars", jar_uri)
        print(f"✓ Utilisation du driver JDBC local : {jar_uri}")
    else:
        builder = builder.config("spark.jars.packages", "org.postgresql:postgresql:42.7.8")
        print("✓ Driver JDBC local non trouvé — utilisation de spark.jars.packages pour télécharger le driver")

    # Configurer le java.library.path pour que les natives Hadoop (winutils, .dll) soient chargés
    native_lib_path = os.path.join(HADOOP_DIR, 'bin').replace('\\','/')
    builder = builder \
        .config("spark.driver.extraJavaOptions", f"-Djava.library.path={native_lib_path}") \
        .config("spark.executor.extraJavaOptions", f"-Djava.library.path={native_lib_path}") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f"✓ Session Spark créée - Version: {spark.version}")
    return spark

def lire_table_postgres(spark, nom_table):
    print(f"\nLecture de la table '{nom_table}' depuis PostgreSQL...")
    df = spark.read \
        .format("jdbc") \
        .option("url", POSTGRES_CONFIG['url']) \
        .option("dbtable", nom_table) \
        .option("user", POSTGRES_CONFIG['user']) \
        .option("password", POSTGRES_CONFIG['password']) \
        .option("driver", POSTGRES_CONFIG['driver']) \
        .load()
    nb_lignes = df.count()
    print(f"✓ {nb_lignes} lignes lues depuis '{nom_table}'")
    print(f"  Schéma de {nom_table}:")
    df.printSchema()
    return df

def ajouter_metadata(df, nom_table_source):
    df_enrichi = df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_system", lit("PostgreSQL")) \
        .withColumn("source_table", lit(nom_table_source))
    print(f"✓ Métadonnées ajoutées (3 colonnes)")
    return df_enrichi

def ecrire_bronze_delta(df, nom_table):
    # Utiliser schema file:/// pour éviter les problèmes de schéma Windows
    chemin_complet = f"file:///{BRONZE_PATH.replace('\\','/')}/{nom_table}"
    print(f"\nÉcriture en Delta Lake : {chemin_complet}")
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(chemin_complet)
    nb_lignes = df.count()
    print(f"✓ {nb_lignes} lignes écrites en Delta Lake")
    print(f"  Emplacement : {chemin_complet}")

def main():
    print("=" * 70)
    print("INGESTION BRONZE : PostgreSQL → Delta Lake")
    print("=" * 70)

    spark = None
    try:
        spark = creer_session_spark()
        tables = ['clients_source', 'produits_source', 'ventes_source']
        for table_source in tables:
            print(f"\n{'=' * 70}")
            print(f"Traitement de : {table_source}")
            print(f"{'=' * 70}")
            df = lire_table_postgres(spark, table_source)
            df_enrichi = ajouter_metadata(df, table_source)
            nom_table_bronze = table_source.replace('_source', '')
            ecrire_bronze_delta(df_enrichi, nom_table_bronze)

        print(f"\n{'=' * 70}")
        print("✓ INGESTION BRONZE TERMINÉE AVEC SUCCÈS")
        print(f"{'=' * 70}")

    except Exception as e:
        print(f"\n✗ ERREUR : {e}")
        import traceback
        traceback.print_exc()

    finally:
        if spark is not None:
            try:
                spark.stop()
                print("\n✓ Session Spark arrêtée")
            except Exception:
                pass
        else:
            print("\nSession Spark non démarrée — rien à arrêter.")

if __name__ == "__main__":
    main()