import sys
from pathlib import Path

utils_dir = Path(__file__).resolve().parent.parent / 'utils'
sys.path.insert(0, str(utils_dir))

try:
    import common
    print("✅ common.py imported successfully")
    print("Project root is:", common.get_project_root())
except Exception as e:
    print("❌ Import or usage failed:", e)
