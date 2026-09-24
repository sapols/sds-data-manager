"""Contains all functions needed to calculate spin file dependencies."""

import datetime
import logging
from contextlib import nullcontext
from os.path import basename

from sqlalchemy import desc, func
from sqlalchemy.orm import aliased

from sds_data_manager.lambda_code.SDSCode.database import database as db
from sds_data_manager.lambda_code.SDSCode.database import models

# Logger setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# A file's end is its last spin start plus that spin's estimated period, so
# adjacent files overlap or gap by a few tens of milliseconds. A real gap is
# at least one spin, about fifteen seconds.
SPIN_GAP_TOLERANCE = datetime.timedelta(seconds=1)


def verify_spin_coverage(
    records: list,
    start_date: datetime,
    end_date: datetime,
) -> bool:
    """Verify that spin files cover the entire time range without gaps.

    A spin file covers its first spin start through the end of its last spin,
    as recorded when the file was indexed. This function verifies:
    1. First record starts at or before the input start_date
    2. Each record starts no later than the coverage accumulated so far
    3. Accumulated coverage reaches the input end_date

    If gaps are found, they are logged at INFO level.

    Parameters
    ----------
    records : list
        List of SpinFiles records with file_path, start_date, end_date.
    start_date : datetime
        Expected coverage start time.
    end_date : datetime
        Expected coverage end time.

    Returns
    -------
    bool
        True if coverage is complete, False if gaps exist.
    """
    if not records:
        logger.info(f"No spin files found for {start_date} to {end_date}")
        return False

    sorted_records = sorted(records, key=lambda r: r.start_date)

    if sorted_records[0].start_date.replace(
        tzinfo=datetime.timezone.utc
    ) > start_date.replace(tzinfo=datetime.timezone.utc):
        logger.info(
            f"Spin coverage gap at start: {start_date} to "
            f"{sorted_records[0].start_date}"
        )
        return False

    covered_through = sorted_records[0].end_date
    for record in sorted_records[1:]:
        if record.start_date > covered_through + SPIN_GAP_TOLERANCE:
            logger.info(
                f"Spin coverage gap between files: {covered_through} to "
                f"{record.start_date}"
            )
            return False
        covered_through = max(covered_through, record.end_date)

    if covered_through.replace(tzinfo=datetime.timezone.utc) < end_date.replace(
        tzinfo=datetime.timezone.utc
    ):
        logger.info(f"Spin coverage gap at end: {covered_through} to {end_date}")
        return False

    logger.info(
        f"Spin coverage verified for {start_date} to {end_date}: "
        f"{len(records)} file(s) cover range"
    )
    return True


def get_spin_files(
    session,
    start_date: datetime,
    end_date: datetime,
) -> list:
    """Get spin input.

    Query the spin table for the latest version of each file, then keep the
    files overlapping the given time range.

    Parameters
    ----------
    session : orm session
        Database session.
    start_date : datetime
        Start of the time range to find spin files for.
    end_date : datetime
        End of the time range to find spin files for.

    Returns
    -------
    list
        SpinFiles records with file_path, start_date, end_date and version,
        oldest ingested first.
    """
    spin = aliased(models.SpinFiles)

    # Versions of a file share its name apart from the version suffix. Their
    # coverage can differ, so the name is what identifies them.
    series = func.regexp_replace(spin.file_path, r"_\d+\.spin.*$", "")
    row_number = (
        func.row_number()
        .over(partition_by=series, order_by=desc(spin.version))
        .label("row_num")
    )
    latest = session.query(
        spin.file_path,
        spin.start_date,
        spin.end_date,
        spin.version,
        spin.ingestion_date,
        row_number,
    ).subquery()

    records = (
        session.query(
            latest.c.file_path,
            latest.c.start_date,
            latest.c.end_date,
            latest.c.version,
        )
        .filter(
            latest.c.row_num == 1,
            latest.c.start_date <= end_date,
            latest.c.end_date >= start_date,
        )
        .order_by(latest.c.ingestion_date)
        .all()
    )

    return records


def get_upstream_dependency_inputs_spin(
    start_date: datetime,
    end_date: datetime,
    require_coverage: bool = False,
    open_session: db.Session = None,
):
    """Construct a ProcessingInputCollection of dependency files.

    For each dependency, query for existing files in s3 and add any matching files
    found to a ProcessingInputCollection.

    Parameters
    ----------
    dependencies : list
        List of dependency dictionaries either downstream or upstream from the
        dependency in the query parameters.
    start_date : datetime
        Start date to find dependent files with.
    end_date : datetime
        End date to find dependent files with.
    repoint : int or list[int], optional
        If provided, will be used to filter files by repoint number(s). Can be a
        single int or a list of ints.
    require_coverage : bool, optional
        If True gathered dependencies will be checked for complete coverage of
        start_date to end_date or repoint coverage.
    open_session : db.Session, optional
        Database session. If not provided, a new session will be created.

    Returns
    -------
    ProcessingInputCollection
        Dependency files that can include Ancillary, SPICE, or Science inputs.
    """
    # Use provided session or create a new one
    session_context = nullcontext(open_session) if open_session else db.Session()
    with session_context as session:
        spin_records = get_spin_files(session, start_date, end_date)
        if not spin_records:
            logger.info(f"No spin files found for {start_date} to {end_date}")
            return None
        # Verify spin coverage
        if require_coverage and not verify_spin_coverage(
            spin_records, start_date, end_date
        ):
            return None
        spin_files = [basename(record.file_path) for record in spin_records]
        logger.info(f"Found spin files: {spin_files}. Adding to collection.")

    return spin_files
