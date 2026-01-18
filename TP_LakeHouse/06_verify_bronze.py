# -*- coding: utf-8 -*-
import os
import shutil
import ctypes
from pathlib import Path
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# ---------------------------
# Minimal native-hadoop (winutils / DLL) helper for Windows
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

# Candidate locations (add your own if needed)
candidates = []
if os.environ.get("HADOOP_HOME"):
    candidates.append(os.path.join(os.environ["HADOOP_HOME"], "bin"))
candidates += [
    r"D:\hadoop\bin",
    r"C:\hadoop\bin",
    r"D:\hadoop-3.4.1-src\dev-support\bin",
    r"D:\hadoop-3.4.1-src\bin",
]
which_winutils = shutil.which("winutils.exe")
if which_winutils:
    candidates.insert(0, str(Path(which_winutils).parent))

src_bin = find_winutils_in_candidates(candidates)
TARGET_HADOOP = Path(r"D:\hadoop")

if src_bin:
    print(f"✓ winutils found in: {src_bin}")
    print(f"→ copying native files to {TARGET_HADOOP}")
    target_bin = copy_native_files(src_bin, TARGET_HADOOP)
    HADOOP_DIR = str(TARGET_HADOOP)
else:
    print("✗ winutils.exe NOT found automatically in common locations.")
    print("Place winutils.exe and the Hadoop native DLLs in D:\\hadoop\\bin or update TARGET_HADOOP.")
    # fallback - you can change this to your own folder that contains winutils + native DLLs
    HADOOP_DIR = r"D:\hadoop"

# Set environment variables for current process / JVM
os.environ['HADOOP_HOME'] = HADOOP_DIR
os.environ['HADOOP_COMMON_LIB_NATIVE_DIR'] = os.path.join(HADOOP_DIR, 'bin')
os.environ['JAVA_TOOL_OPTIONS'] = os.environ.get('JAVA_TOOL_OPTIONS', '') + ' -Djava.library.path=' + os.path.join(HADOOP_DIR, 'bin')

# Try to load a common native DLL to surface helpful error if VC++ runtime missing
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
# Spark + Delta read (verify bronze)
# ---------------------------

def create_spark():
    builder = SparkSession.builder.appName("Verify Bronze").master("local[*]")

    # optional: if you have a local JDBC jar, set spark.jars to it (not required for this verify script)
    # builder = builder.config("spark.jars", "file:///D:/TP_DataWarehouse/drivers/postgresql-42.7.8.jar")

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

def verify():
    spark = None
    try:
        spark = create_spark()
        # Use file:/// URI for local paths on Windows
        path = "file:///D:/lakehouse/bronze/clients"
        print(f"\nReading Delta at: {path}")
        df = spark.read.format("delta").load(path)
        print("Données Bronze clients :")
        df.show(5, truncate=False)
        print("\nSchéma :")
        df.printSchema()
        print(f"\nNombre de lignes : {df.count()}")
    except Exception as e:
        print("\n✗ ERROR:", e)
        import traceback
        traceback.print_exc()
    finally:
        if spark is not None:
            spark.stop()
            print("\n✓ Spark stopped")

if __name__ == "__main__":
    verify()