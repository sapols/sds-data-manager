"""Test that a required spin dependency must cover the partition window.

Spin coverage is judged from the dates in the spin filenames, so a daily job
waits for the file that starts on its day and a pointing job waits for the
file that starts on the day after its pointing begins.
"""

import datetime
from collections import namedtuple

import pytest

from sds_data_manager.orchestration import imap_job
from sds_data_manager.orchestration.dagster_utilities import (
    parse_dates_from_partition_key,
)
from sds_data_manager.orchestration.imap_dagster import job_handlers
from sds_data_manager.orchestration.spin import verify_spin_coverage
from tests.orchestration.conftest import _insert_spin_file

SpinRecord = namedtuple("SpinRecord", ["file_path", "start_date", "end_date"])

# Filename dates of the production spin files around 2026-09-10
SEPT_9_TO_10 = SpinRecord(
    "imap_2026_252_2026_253_01.spin",
    datetime.datetime(2026, 9, 9),
    datetime.datetime(2026, 9, 10),
)
SEPT_10_TO_11 = SpinRecord(
    "imap_2026_253_2026_254_01.spin",
    datetime.datetime(2026, 9, 10),
    datetime.datetime(2026, 9, 11),
)

DAY_PARTITION = "daily_2026-09-10T00:00:00_to_2026-09-11T00:00:00"
POINTING_PARTITION = "repoint367_2026-09-09T10:03:11_to_2026-09-10T10:03:28"


def _day(month: int, day: int) -> datetime.datetime:
    return datetime.datetime(2026, month, day)


def _job(source: str, data_type: str, descriptor: str):
    """Look up the registered job handler for a product."""
    return next(
        handler
        for handler in job_handlers
        if (
            handler.job_config.source,
            handler.job_config.data_type,
            handler.job_config.descriptor,
        )
        == (source, data_type, descriptor)
    )


def _insert(session, record: SpinRecord):
    _insert_spin_file(
        session,
        record.file_path,
        start_date=record.start_date,
        end_date=record.end_date,
    )


@pytest.mark.parametrize("product", [("mag", "l1d", "norm-srf"), ("swe", "l2", "sci")])
def test_daily_job_waits_for_the_spin_file_starting_on_its_day(
    mock_db_session, product
):
    """A daily job is missing spin until a file dated on its day exists."""
    job = _job(*product)
    target_start, target_end = parse_dates_from_partition_key(DAY_PARTITION)

    _insert(mock_db_session, SEPT_9_TO_10)
    with pytest.raises(imap_job.MissingDependenciesError, match="spin"):
        job.get_spin_files_inputs(mock_db_session, target_start, target_end)

    _insert(mock_db_session, SEPT_10_TO_11)
    spin_files = job.get_spin_files_inputs(mock_db_session, target_start, target_end)
    assert set(spin_files) == {SEPT_9_TO_10.file_path, SEPT_10_TO_11.file_path}


def test_pointing_job_waits_for_the_spin_file_starting_the_next_day(mock_db_session):
    """Filename dates are day-granular, so a pointing needs the next day's file."""
    job = _job("hi", "l1b", "45sensor-de")
    target_start, target_end = parse_dates_from_partition_key(POINTING_PARTITION)

    _insert(mock_db_session, SEPT_9_TO_10)
    with pytest.raises(imap_job.MissingDependenciesError, match="spin"):
        job.get_spin_files_inputs(mock_db_session, target_start, target_end)

    _insert(mock_db_session, SEPT_10_TO_11)
    spin_files = job.get_spin_files_inputs(mock_db_session, target_start, target_end)
    assert set(spin_files) == {SEPT_9_TO_10.file_path, SEPT_10_TO_11.file_path}


def test_verify_spin_coverage_longer_file_spans_shorter_ones():
    """A longer file covers the window despite shorter files sorted around it."""
    records = [
        SpinRecord("imap_2026_204_2026_205_01.spin", _day(7, 23), _day(7, 24)),
        SpinRecord("imap_2026_204_2026_206_02.spin", _day(7, 23), _day(7, 25)),
        SpinRecord("imap_2026_205_2026_205_01.spin", _day(7, 24), _day(7, 24)),
        SpinRecord("imap_2026_206_2026_206_02.spin", _day(7, 25), _day(7, 25)),
        SpinRecord("imap_2026_206_2026_207_02.spin", _day(7, 25), _day(7, 26)),
    ]

    assert verify_spin_coverage(records, _day(7, 24), _day(7, 25))


def test_verify_spin_coverage_same_day_file_listed_after_two_day_file():
    """A same-day file sorted after a two-day file does not end the coverage."""
    records = [
        SpinRecord("imap_2026_051_2026_052_01.spin", _day(2, 20), _day(2, 21)),
        SpinRecord("imap_2026_051_2026_051_01.spin", _day(2, 20), _day(2, 20)),
    ]

    assert verify_spin_coverage(records, _day(2, 20), _day(2, 21))
