# -*- coding: utf-8 -*-
"""
Rapport final (Windows-friendly).
- Detects/copies winutils + native Hadoop DLLs (if present) into D:\hadoop
- Sets HADOOP_HOME, HADOOP_COMMON_LIB_NATIVE_DIR and java.library.path for the JVM
- Creates Spark session configured for Delta
- Discovers/validates Gold Delta table (ventes_quotidiennes) and generates summary report
"""
import os
import shutil
import ctypes
from pathlib import Path
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg as _avg
from delta import configure_spark_with_delta_pip

# ---------------------------
# Configuration
# ---------------------------
GOLD_ROOT = Path(r"D:\lakehouse\gold")
TARGET_HADOOP = Path(r"D:\hadoop")
PREFERRED_TABLE = "ventes_quotidiennes"

# ---------------------------
# Native Hadoop / winutils helper
# ---------------------------
def find_winutils_in_candidates(candidates):
    for p in candidates:
        bin_path = Path(p)
        if bin_path.is_dir() and (bin_path / "winutils.exe").exists():
            return str(bin_path.resolve())
    return None

def copy_native_files(src_bin, target_root):
    src_bin = Path(src_bin)
    target_bin = Path(target_root) / "bin"
    target_lib_native = Path(target_root) / "lib" / "native"
    target_bin.mkdir(parents=True, exist_ok=True)
    target_lib_native.mkdir(parents=True, exist_ok=True)

    for item in src_bin.iterdir():
        if item.is_file() and (item.suffix.lower() in (".exe", ".dll", ".lib") or item.name.lower().startswith("winutils")):
            try:
                shutil.copy2(item, target_bin / item.name)
            except Exception as e:
                print(f"⚠️ copy failed {item} -> {target_bin}: {e}")

    possible_native = src_bin.parent / "lib" / "native"
    if possible_native.exists():
        for item in possible_native.iterdir():
            if item.is_file() and item.suffix.lower() == ".dll":
                try:
                    shutil.copy2(item, target_lib_native / item.name)
                except Exception as e:
                    print(f"⚠️ copy failed {item} -> {target_lib_native}: {e}")

    return str(target_bin.resolve())

# discover candidate locations for winutils
candidates = []
if os.environ.get("HADOOP_HOME"):
    candidates.append(os.path.join(os.environ["HADOOP_HOME"], "bin"))
candidates += [
    r"D:\hadoop\bin",
    r"C:\hadoop\bin",
    r"D:\hadoop-3.4.1-src\dev-support\bin",
    r"D:\hadoop-3.4.1-src\bin",
    r"D:\hadoop-3.4.1-src",
]
which_winutils = shutil.which("winutils.exe")
if which_winutils:
    candidates.insert(0, str(Path(which_winutils).parent))

src_bin = find_winutils_in_candidates(candidates)
if src_bin:
    print(f"✓ winutils found in: {src_bin}")
    print(f"→ copying native files to {TARGET_HADOOP}")
    copy_native_files(src_bin, TARGET_HADOOP)
    HADOOP_DIR = str(TARGET_HADOOP)
else:
    print("⚠️ winutils.exe not found automatically. Ensure winutils.exe and native DLLs are in D:\\hadoop\\bin or update TARGET_HADOOP.")
    HADOOP_DIR = str(TARGET_HADOOP)

# Export environment variables for this process (JVM inherits these)
os.environ['HADOOP_HOME'] = HADOOP_DIR
os.environ['HADOOP_COMMON_LIB_NATIVE_DIR'] = os.path.join(HADOOP_DIR, 'bin')
os.environ['JAVA_TOOL_OPTIONS'] = os.environ.get('JAVA_TOOL_OPTIONS', '') + ' -Djava.library.path=' + os.path.join(HADOOP_DIR, 'bin')

# Try to load a common native DLL to surface a helpful error if VC++ runtime is missing
hadoop_dll_path = Path(HADOOP_DIR) / "bin" / "hadoop.dll"
if hadoop_dll_path.exists():
    try:
        ctypes.WinDLL(str(hadoop_dll_path))
        print(f"✓ hadoop.dll loaded from {hadoop_dll_path}")
    except OSError as e:
        print(f"✗ Failed to load {hadoop_dll_path}: {e}")
        print("  → Probably missing 'Microsoft Visual C++ Redistributable for Visual Studio 2015-2022 (x64)'.")
else:
    print(f"⚠️ hadoop.dll not found in {Path(HADOOP_DIR)/'bin'} — verify native DLLs are present.")

# ---------------------------
# Spark session factory
# ---------------------------
def create_spark():
    builder = SparkSession.builder.appName("Rapport Final").master("local[*]")
    native_lib_path = os.path.join(HADOOP_DIR, 'bin').replace('\\', '/')
    builder = builder \
        .config("spark.driver.extraJavaOptions", f"-Djava.library.path={native_lib_path}") \
        .config("spark.executor.extraJavaOptions", f"-Djava.library.path={native_lib_path}") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f"✓ Spark session created (version {spark.version})")
    return spark

# ---------------------------
# Report generation
# ---------------------------
def discover_gold_table(root: Path, preferred_name: str):
    if not root.exists():
        print(f"✗ GOLD root does not exist: {root}")
        return None
    entries = [p for p in root.iterdir() if p.is_dir()]
    names = [p.name for p in entries]
    print(f"Found {len(entries)} entries under GOLD root: {names}")
    # prefer explicit name
    candidate = root / preferred_name
    if candidate.exists() and (candidate / "_delta_log").exists():
        print(f"Using preferred Gold table: {candidate}")
        return candidate
    # fallback: first folder with _delta_log
    for p in entries:
        if (p / "_delta_log").exists():
            print(f"Auto-discovered Gold table: {p}")
            return p
    print("No Delta table discovered under GOLD root.")
    return None

def generate_report():
    spark = None
    try:
        gold_table = discover_gold_table(GOLD_ROOT, PREFERRED_TABLE)
        if gold_table is None:
            print("\n✗ Aborting: no Delta table found under GOLD root.")
            return

        spark = create_spark()
        # use plain absolute path (Windows)
        gold_path_str = str(gold_table.resolve())
        print(f"\nReading Gold Delta table from: {gold_path_str}")
        df_ventes_quot = spark.read.format("delta").load(gold_path_str)

        print("\n" + "="*70)
        print("RAPPORT DATA WAREHOUSE - " + datetime.now().strftime("%Y-%m-%d %H:%M"))
        print("="*70 + "\n")

        # Global statistics
        stats_row = df_ventes_quot.agg(
            _sum('nb_ventes').alias('total_ventes'),
            _sum('ca_total').alias('ca_global'),
            _avg('panier_moyen').alias('panier_moyen_global')
        ).collect()
        if not stats_row:
            print("✗ No data in gold table.")
            return
        stats = stats_row[0]

        total_ventes = stats['total_ventes'] if stats['total_ventes'] is not None else 0
        ca_global = float(stats['ca_global']) if stats['ca_global'] is not None else 0.0
        panier_moyen_global = float(stats['panier_moyen_global']) if stats['panier_moyen_global'] is not None else 0.0

        print("STATISTIQUES GLOBALES")
        print("-"*70)
        print(f"Total des ventes : {total_ventes}")
        print(f"Chiffre d'affaires : {ca_global:.2f} €")
        print(f"Panier moyen : {panier_moyen_global:.2f} €")

        print("\n" + "="*70)
        print("TOP 5 MEILLEURS JOURS")
        print("="*70)
        df_ventes_quot.orderBy(col('ca_total').desc()).show(5, truncate=False)

        print("\n✓ Rapport généré avec succès")
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
    generate_report()