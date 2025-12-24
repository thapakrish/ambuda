import os
import psutil
import gc
import sys
from pathlib import Path


# For nice imports
sys.path.append(str(Path(__file__).parent.parent))


def get_memory_usage():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)


def test_import_overhead(module_names):
    gc.collect()
    baseline = get_memory_usage()

    print(f"Baseline Memory: {baseline:.2f} MB")

    for module_name in module_names.split(","):
        print(f"Importing {module_name}...")

        try:
            __import__(module_name)
        except ImportError:
            print(f"Error: Module '{module_name}' not found.")
            return

    gc.collect()
    after_import = get_memory_usage()
    overhead = after_import - baseline

    print(f"Memory after import: {after_import:.2f} MB")
    print(f"--- Total Overhead for '{module_names}': {overhead:.2f} MB ---")


if __name__ == "__main__":
    test_import_overhead(sys.argv[1])
