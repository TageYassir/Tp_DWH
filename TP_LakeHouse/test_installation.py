import importlib
import importlib.util
import sys

try:
    # Python 3.8+
    from importlib import metadata as importlib_metadata
except Exception:
    # fallback for older Python (requires importlib-metadata package)
    import importlib_metadata

def module_version(module_name, pypi_names=None):
    # Try importing module and reading __version__
    try:
        mod = importlib.import_module(module_name)
        ver = getattr(mod, "__version__", None)
        if ver:
            return ver, getattr(mod, "__file__", "<no __file__>")
    except Exception:
        pass

    # Try package metadata names from PyPI
    candidates = pypi_names or [module_name]
    for name in candidates:
        try:
            return importlib_metadata.version(name), f"(package metadata for {name})"
        except Exception:
            continue

    return "unknown", ""

packages = {
    "pyspark": ["pyspark"],
    "delta (module)": ["delta", "delta-spark"],
    "psycopg2": ["psycopg2", "psycopg2-binary"],
    "pandas": ["pandas"]
}

for label, candidates in packages.items():
    module_name = candidates[0]
    ver, info = module_version(module_name, candidates)
    print(f"{label} version: {ver} {info}")
print("\nToutes les bibliothèques sont installées (ou leur état est indiqué ci‑dessus).")