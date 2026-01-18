# -*- coding: utf-8 -*-
"""
Bronze -> Silver (ventes) Windows-friendly transform.

- Ensures Hadoop natives (winutils / DLLs) are available to the JVM.
- Reads Bronze 'ventes' Delta table (expects D:/lakehouse/bronze/ventes with _delta_log).
- Cleans and normalizes columns: dedupe, parse date_vente, cast montant_total to double, filter bad rows.
- Writes Delta to D:/lakehouse/silver/ventes
"""
import os
import shutil
import ctypes
from pathlib import Path
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import col, to_date, trim
from pyspark.sql.types import DoubleType

# Paths
BRONZE_VENTES = Path(r"D:\lakehouse\bronze\ventes")
SILVER_VENTES = Path(r"D:\lakehouse\silver\ventes")
TARGET_HADOOP = Path(r"D:\hadoop")  # adjust if you keep natives elsewhere

# ---------------------------
# Minimal winutils / native helper (same pattern used previously)
# ---------------------------
def find_winutils(candidates):
    for p in candidates:
        pth = Path(p)
        if pth.is_dir() and (pth / "winutils.exe").exists():
            return str(pth.resolve())
    return None

def copy_natives(src_bin, target_root):
    src = Path(src_bin)
    target_bin = Path(target_root) / "bin"
    target_lib_native = Path(target_root) / "lib" / "native"
    target_bin.mkdir(parents=True, exist_ok=True)
    target_lib_native.mkdir(parents=True, exist_ok=True)
    for item in src.iterdir():
        if item.is_file() and (item.suffix.lower() in (".exe", ".dll", ".lib") or item.name.lower().startswith("winutils")):
            try:
                shutil.copy2(item, target_bin / item.name)
            except Exception as e:
                print(f"⚠️ Failed to copy {item} -> {target_bin}: {e}")
    possible_native = src.parent / "lib" / "native"
    if possible_native.exists():
        for item in possible_native.iterdir():
            if item.is_file() and item.suffix.lower() == ".dll":
                try:
                    shutil.copy2(item, target_lib_native / item.name)
                except Exception as e:
                    print(f"⚠️ Failed to copy native {item} -> {target_lib_native}: {e}")
    return str(target_bin.resolve())

candidates = []
if os.environ.get("HADOOP_HOME"):
    candidates.append(os.path.join(os.environ["HADOOP_HOME"], "bin"))
candidates += [
    r"D:\hadoop-3.4.1-src\dev-support\bin",
    r"D:\hadoop-3.4.1-src\bin",
    r"D:\hadoop\bin",
    r"C:\hadoop\bin",
]
which_winutils = shutil.which("winutils.exe")
if which_winutils:
    candidates.insert(0, str(Path(which_winutils).parent))

src_bin = find_winutils(candidates)
if src_bin:
    print(f"✓ winutils found in: {src_bin}")
    print(f"→ copying native files to {TARGET_HADOOP}")
    copy_natives(src_bin, TARGET_HADOOP)
    HADOOP_DIR = str(TARGET_HADOOP)
else:
    print("⚠️ winutils.exe NOT found automatically. Put winutils.exe and native DLLs in D:\\hadoop\\bin or adjust TARGET_HADOOP.")
    HADOOP_DIR = str(TARGET_HADOOP)

# Export to process env (JVM inherits these)
os.environ['HADOOP_HOME'] = HADOOP_DIR
os.environ['HADOOP_COMMON_LIB_NATIVE_DIR'] = os.path.join(HADOOP_DIR, 'bin')
os.environ['JAVA_TOOL_OPTIONS'] = os.environ.get('JAVA_TOOL_OPTIONS', '') + ' -Djava.library.path=' + os.path.join(HADOOP_DIR, 'bin')

# Try to load hadoop.dll to give a clear hint if VC++ runtime is missing
hadoop_dll = Path(HADOOP_DIR) / "bin" / "hadoop.dll"
if hadoop_dll.exists():
    try:
        ctypes.WinDLL(str(hadoop_dll))
        print(f"✓ hadoop.dll loaded from {hadoop_dll}")
    except OSError as e:
        print(f"✗ Failed to load {hadoop_dll}: {e}")
        print("  → Install Microsoft Visual C++ Redistributable (2015-2022 x64).")
else:
    print(f"⚠️ hadoop.dll not found in {Path(HADOOP_DIR)/'bin'} — verify native DLLs if needed.")

# ---------------------------
# Spark session builder
# ---------------------------
def create_spark():
    native_lib_path = os.path.join(HADOOP_DIR, 'bin').replace('\\','/')
    builder = SparkSession.builder.appName("Silver Ventes Transformation").master("local[*]") \
        .config("spark.driver.extraJavaOptions", f"-Djava.library.path={native_lib_path}") \
        .config("spark.executor.extraJavaOptions", f"-Djava.library.path={native_lib_path}") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f"✓ Spark session created (version {spark.version})")
    return spark

# ---------------------------
# Transform ventes
# ---------------------------
def transform_ventes():
    if not BRONZE_VENTES.exists():
        print(f"✗ Bronze ventes path not found: {BRONZE_VENTES}")
        print("Check that the Bronze job wrote D:/lakehouse/bronze/ventes")
        return

    if not (BRONZE_VENTES / "_delta_log").exists():
        print(f"✗ Bronze ventes path is not a Delta table (missing _delta_log): {BRONZE_VENTES}")
        return

    spark = None
    try:
        spark = create_spark()
        path_str = str(BRONZE_VENTES.resolve())
        print(f"\nReading Bronze ventes Delta from: {path_str}")
        df = spark.read.format("delta").load(path_str)

        print("Bronze ventes schema:", df.schema.simpleString())

        # Basic cleaning & normalization:
        # - drop duplicates (by vente_id if present else all columns)
        # - parse date_vente into date and ensure montant_total is numeric
        key_cols = []
        if 'vente_id' in df.columns:
            key_cols = ['vente_id']
        elif 'id' in df.columns:
            key_cols = ['id']

        if key_cols:
            df = df.dropDuplicates(key_cols)
        else:
            df = df.dropDuplicates()

        # Normalize montant_total
        if 'montant_total' in df.columns:
            df = df.withColumn('montant_total', col('montant_total').cast(DoubleType()))
        elif 'amount' in df.columns:
            df = df.withColumn('montant_total', col('amount').cast(DoubleType()))
        else:
            print("⚠️ No montant_total/amount column found — Gold aggregation needs an amount column.")
            # still continue to write a Silver ventes if you want, but Gold will need amount
        # Parse date_vente
        if 'date_vente' in df.columns:
            df = df.withColumn('date_vente', to_date(trim(col('date_vente'))))
        elif 'timestamp' in df.columns:
            df = df.withColumn('date_vente', to_date(trim(col('timestamp'))))
        else:
            print("⚠️ No date_vente/timestamp column found — Gold aggregation needs a date column.")

        # Optionally drop rows with nulls in essential cols
        essential = []
        if 'montant_total' in df.columns:
            essential.append('montant_total')
        if 'date_vente' in df.columns:
            essential.append('date_vente')
        if essential:
            df = df.na.drop(subset=essential)

        print(f"Rows after cleaning: {df.count()}")
        print("Writing Silver ventes to:", str(SILVER_VENTES.resolve()))
        df.write.format("delta").mode("overwrite").save(str(SILVER_VENTES.resolve()))
        print("✓ Silver ventes created")
    except Exception as e:
        print("\n✗ ERROR:", e)
        import traceback
        traceback.print_exc()
    finally:
        if spark is not None:
            try:
                spark.stop()
                print("\n✓ Spark stopped")
            except Exception:
                pass

if __name__ == "__main__":
    transform_ventes()