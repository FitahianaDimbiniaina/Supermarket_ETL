from pathlib import Path
from sqlalchemy import create_engine
import pandas as pd
import logging
import sys
import shutil
from datetime import datetime

PROJECT_ROOT = Path(__file__).parents[2]  
SQL_DIR = PROJECT_ROOT / 'sql'
EXPORTS_DIR = PROJECT_ROOT / 'exports'
DB_URI = 'postgresql://postgres:4499405@localhost:5432/supermarket_etl'

EXPORTS_DIR.mkdir(exist_ok=True, parents=True)
(EXPORTS_DIR / 'charts').mkdir(exist_ok=True)
(EXPORTS_DIR / 'csv').mkdir(exist_ok=True)
(EXPORTS_DIR / 'pdf').mkdir(exist_ok=True)
SQL_DIR.mkdir(exist_ok=True, parents=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(PROJECT_ROOT / 'scripts' / 'analysis.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def extract_sql_block(sql_file_path: Path, block_name: str) -> str:
    with open(sql_file_path, 'r', encoding='utf-8') as f:
        sql = f.read()

    start_marker = f'-- {block_name}'
    lines = sql.splitlines()

    inside_block = False
    extracted_lines = []

    for line in lines:
        if line.strip().startswith('--') and line.strip() == start_marker:
            inside_block = True
            continue
        elif line.strip().startswith('--') and inside_block:
            break  
        elif inside_block:
            extracted_lines.append(line)

    if not extracted_lines:
        raise ValueError(f"SQL block '{block_name}' not found in {sql_file_path}")

    return '\n'.join(extracted_lines).strip()


def create_db_engine():
    try:
        return create_engine(
            DB_URI,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            pool_recycle=3600,
            connect_args={'connect_timeout': 10}
        )
    except Exception as e:
        logger.error(f"Database engine creation failed: {e}")
        raise

def archive_old_exports(format_type: str):
    format_dir = EXPORTS_DIR / format_type
    archive_dir = EXPORTS_DIR / 'archive' / format_type
    archive_dir.mkdir(parents=True, exist_ok=True)
  
    for item in format_dir.iterdir():
        if item.is_file():
            target = archive_dir / item.name
            try:
                shutil.move(str(item), str(target))
                logger.info(f"Archived old export {item.name} to {archive_dir}")
            except Exception as e:
                logger.error(f"Failed to archive {item}: {e}")

def export_data(df, caller_module, format_type='csv'):
    """
    Export a dataframe into exports/<process_folder>/<date>/<script_name>/<timestamped_file>.
    `caller_module` should be __name__ of the calling script.
    Process folder is automatically inferred from the module path under scripts.analysis
    """
    now = datetime.now()
    date_str = now.strftime('%Y-%m-%d')
    timestamp = now.strftime('%H-%M-%S')

    parts = caller_module.split('.')
    try:
        idx = parts.index('analysis')
        process_folder = parts[idx + 1]  # first folder under analysis
    except (ValueError, IndexError):
        process_folder = 'analysis'  # fallback

    script_name = parts[-1]

    # Paths
    process_dir = EXPORTS_DIR / process_folder / date_str / script_name
    archive_dir = EXPORTS_DIR / 'archive' / process_folder / date_str / script_name

    process_dir.mkdir(parents=True, exist_ok=True)
    archive_dir.mkdir(parents=True, exist_ok=True)

    # Archive existing files
    for file in process_dir.iterdir():
        if file.is_file() and file.suffix in ['.csv']:
            archived_path = archive_dir / f"{file.stem}_{timestamp}{file.suffix}"
            shutil.move(str(file), str(archived_path))
            logger.info(f"Archived {file.name} to {archived_path}")

    # Filename
    filename = f"{script_name}_{timestamp}.csv"
    output_path = process_dir / filename

    # Export CSV only
    df.to_csv(output_path, index=False)
    logger.info(f"Exported CSV to {output_path}")

    return output_path



def get_project_root() -> Path:
    return PROJECT_ROOT

def get_sql_path() -> Path:
    return SQL_DIR

def get_exports_path() -> Path:
    return EXPORTS_DIR
