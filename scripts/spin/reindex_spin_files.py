"""Re-index every spin file so its coverage comes from its rows.

Rows indexed before coverage was read from the file hold the day-of-year range
from the filename. Run this from an environment configured like the lambdas
(database secret and S3 bucket) after the indexer that reads coverage from the
file has deployed and before the spin coverage requirement deploys, in each
environment. Re-indexing is idempotent and leaves ingestion dates alone.
"""

import logging
import sys

from sds_data_manager.lambda_code.SDSCode.database import database as db
from sds_data_manager.lambda_code.SDSCode.database import models
from sds_data_manager.lambda_code.SDSCode.pipeline_lambdas.spice_indexer import (
    index_spin_file,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main() -> int:
    """Re-index every spin file in the database and report failures."""
    with db.Session() as session:
        file_paths = [
            row.file_path for row in session.query(models.SpinFiles.file_path)
        ]

    failed = []
    for count, file_path in enumerate(file_paths, start=1):
        try:
            index_spin_file(file_path)
            logger.info(f"Re-indexed {count}/{len(file_paths)}: {file_path}")
        except Exception:
            logger.exception(f"Failed to re-index {file_path}")
            failed.append(file_path)

    logger.info(f"{len(file_paths) - len(failed)} re-indexed, {len(failed)} failed")
    for file_path in failed:
        logger.error(f"Not re-indexed: {file_path}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
