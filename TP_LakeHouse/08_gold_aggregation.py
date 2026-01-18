# -*- coding: utf-8 -*-
"""
Gold creation with schema discovery (Windows-friendly).
- Detects/copies winutils + native DLLs (if present) into D:\hadoop (like prior scripts).
- Scans subfolders under SILVER_ROOT for Delta tables, inspects schemas,
  and picks a table that contains (date-like column) + (amount-like column).
- Aggregates sales per day and writes Gold.
- If no suitable table found, prints diagnostics (tables and their columns).
"""
import os
import shutil
import ctypes
from pathlib import Path
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import col, to_date, count, sum as _sum, avg

# ---------------------------
# Configuration
# ---------------------------
SILVER_ROOT = Path(r"D:\lakehouse\silver")
GOLD_PATH = Path(r"D:\lakehouse\gold")
TARGET_HADOOP = Path(r"D:\hadoop")

# Preferred column name candidates
DATE_CANDIDATES = ["date_vente", "date", "sale_date", "transaction_date", "created_at", "timestamp"]
AMOUNT_CANDIDATES = ["montant_total", "amount", "total_amount", "price", "montant", "total"]

# Optional overrides via env:
# - SILVER_TABLE_NAME : exact subfolder name under SILVER_ROOT to use
# - DATE_COLUMN and AMOUNT_COLUMN : force the column names to use
ENV_TABLE = os.environ.get("SILVER_TABLE_NAME")
ENV_DATE_COL = os.environ.get("DATE_COLUMN")
ENV_AMOUNT_COL = os.environ.get("AMOUNT_COLUMN")

# ---------------------------
# Small winutils / native helper (same approach as other scripts)
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
    print("⚠️ winutils.exe not found automatically. Ensure winutils + native DLLs are available under D:\\hadoop\\bin.")
    HADOOP_DIR = str(TARGET_HADOOP)

os.environ['HADOOP_HOME'] = HADOOP_DIR
os.environ['HADOOP_COMMON_LIB_NATIVE_DIR'] = os.path.join(HADOOP_DIR, 'bin')
os.environ['JAVA_TOOL_OPTIONS'] = os.environ.get('JAVA_TOOL_OPTIONS', '') + ' -Djava.library.path=' + os.path.join(HADOOP_DIR, 'bin')

hadoop_dll_path = Path(HADOOP_DIR) / "bin" / "hadoop.dll"
if hadoop_dll_path.exists():
    try:
        ctypes.WinDLL(str(hadoop_dll_path))
        print(f"✓ hadoop.dll loaded from {hadoop_dll_path}")
    except OSError as e:
        print(f"✗ Failed to load {hadoop_dll_path}: {e}")
        print("  → Possibly missing Microsoft Visual C++ Redistributable (2015-2022 x64).")
else:
    print(f"⚠️ hadoop.dll not found in {Path(HADOOP_DIR)/'bin'} — verify native DLLs are present if you expect them.")

# ---------------------------
# Spark session
# ---------------------------
def create_spark():
    builder = SparkSession.builder.appName("Gold Aggregation").master("local[*]")
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
# Discovery + aggregation
# ---------------------------
def inspect_silver_tables(root: Path):
    if not root.exists() or not root.is_dir():
        print(f"✗ SILVER root not found or not a directory: {root}")
        return []
    entries = [p for p in root.iterdir() if p.is_dir()]
    results = []
    for p in sorted(entries):
        has_delta = (p / "_delta_log").exists()
        results.append((p.name, str(p.resolve()), has_delta))
    return results

def find_candidate_table(spark):
    # If env override specified and valid, use it
    if ENV_TABLE:
        candidate = SILVER_ROOT / ENV_TABLE
        if candidate.exists() and (candidate / "_delta_log").exists():
            print(f"Using override SILVER_TABLE_NAME={ENV_TABLE}")
            return candidate, ENV_DATE_COL, ENV_AMOUNT_COL
        else:
            print(f"Env override specified but not a Delta table or not present: {candidate}")

    # Try to discover tables and their schemas
    entries = inspect_silver_tables(SILVER_ROOT)
    if not entries:
        print("No subfolders found under SILVER root.")
        return None, None, None

    print("Silver entries found:")
    for name, path_str, has_delta in entries:
        print(f" - {name} (delta: {has_delta})")

    for name, path_str, has_delta in entries:
        if not has_delta:
            continue
        table_path = path_str
        try:
            df = spark.read.format("delta").load(table_path)
            cols = [c.lower() for c in df.columns]
            # choose date and amount columns (env override columns take precedence if present)
            date_col = None
            amount_col = None
            if ENV_DATE_COL:
                if ENV_DATE_COL.lower() in cols:
                    date_col = ENV_DATE_COL
                else:
                    print(f"Env DATE_COLUMN={ENV_DATE_COL} not present in {name}")
            if ENV_AMOUNT_COL:
                if ENV_AMOUNT_COL.lower() in cols:
                    amount_col = ENV_AMOUNT_COL
                else:
                    print(f"Env AMOUNT_COLUMN={ENV_AMOUNT_COL} not present in {name}")
            # find automatic if not forced
            if not date_col:
                for cand in DATE_CANDIDATES:
                    if cand.lower() in cols:
                        date_col = cand
                        break
            if not amount_col:
                for cand in AMOUNT_CANDIDATES:
                    if cand.lower() in cols:
                        amount_col = cand
                        break
            if date_col and amount_col:
                print(f"Selected table '{name}' with date_col='{date_col}' and amount_col='{amount_col}'")
                return Path(table_path), date_col, amount_col
            else:
                print(f"Table '{name}' schema did not match requirements. Columns: {df.columns}")
        except Exception as e:
            print(f"Failed to read table '{name}': {e}")
    return None, None, None

def create_gold():
    spark = None
    try:
        # quick pre-check to see what's under SILVER root
        entries = inspect_silver_tables(SILVER_ROOT)
        if not entries:
            print(f"\n✗ No entries under SILVER root: {SILVER_ROOT}")
            return

        spark = create_spark()
        table_path, date_col, amount_col = find_candidate_table(spark)
        if table_path is None:
            print("\n✗ No suitable Silver table found for aggregation.")
            print("Diagnostics: list of Silver subfolders and whether they are Delta tables:")
            for name, path_str, has_delta in entries:
                print(f" - {name} : delta={has_delta}")
            print("\nEither:")
            print(" - Ensure your Silver pipeline produced a 'ventes' (or sales) Delta table with columns like 'date_vente' and 'montant_total',")
            print(" - Or set environment variables to override names before running this script:")
            print("     SILVER_TABLE_NAME=<folder_name>")
            print("     DATE_COLUMN=<date_column_name>")
            print("     AMOUNT_COLUMN=<amount_column_name>")
            return

        print(f"\nReading Silver Delta table from: {table_path}")
        df = spark.read.format("delta").load(str(table_path))

        # Build aggregation using discovered columns
        ventes_quotidiennes = df \
            .withColumn('date', to_date(col(date_col))) \
            .groupBy('date') \
            .agg(
                count('*').alias('nb_ventes'),
                _sum(col(amount_col)).alias('ca_total'),
                avg(col(amount_col)).alias('panier_moyen')
            ) \
            .orderBy('date')

        print("\nSample of result:")
        ventes_quotidiennes.show(10, truncate=False)

        dest = GOLD_PATH / "ventes_quotidiennes"
        dest_str = str(dest.resolve())
        print(f"\nWriting Gold table to: {dest_str}")
        ventes_quotidiennes.write.format("delta").mode("overwrite").save(dest_str)
        print("✓ Gold ventes_quotidiennes created")
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
    create_gold()