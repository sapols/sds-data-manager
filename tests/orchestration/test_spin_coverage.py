"""Test that a required spin dependency must cover the partition window.

Spin file coverage is recorded at spin precision when the file is indexed, so a
daily job waits for the file that carries its day past midnight while a
pointing job is satisfied by the files overlapping its pointing.
"""

import datetime
from collections import namedtuple

import pytest
from dagster import AssetMaterialization, DagsterRunStatus, build_sensor_context

from sds_data_manager.orchestration import imap_job
from sds_data_manager.orchestration.dagster_utilities import (
    parse_dates_from_partition_key,
)
from sds_data_manager.orchestration.imap_dagster import defs, job_handlers
from sds_data_manager.orchestration.spin import (
    SPIN_GAP_TOLERANCE,
    verify_spin_coverage,
)
from tests.orchestration.conftest import _insert_spin_file

SpinRecord = namedtuple("SpinRecord", ["file_path", "start_date", "end_date"])

# Coverage of the production spin files around 2026-09-10, read from their rows.
SEPT_8_TO_9 = SpinRecord(
    "imap_2026_251_2026_252_01.spin",
    datetime.datetime(2026, 9, 8, 21, 23, 13, 161010),
    datetime.datetime(2026, 9, 9, 21, 23, 7, 110312),
)
SEPT_9_TO_10 = SpinRecord(
    "imap_2026_252_2026_253_01.spin",
    datetime.datetime(2026, 9, 9, 21, 23, 7, 110457),
    datetime.datetime(2026, 9, 10, 13, 47, 15, 336434),
)
SEPT_10_TO_11 = SpinRecord(
    "imap_2026_253_2026_254_01.spin",
    datetime.datetime(2026, 9, 10, 13, 47, 15, 336381),
    datetime.datetime(2026, 9, 11, 13, 48, 11, 659971),
)

DAY_PARTITION = "daily_2026-09-10T00:00:00_to_2026-09-11T00:00:00"
POINTING_PARTITION = "repoint367_2026-09-09T10:03:11_to_2026-09-10T10:03:28"


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
def test_daily_job_waits_for_spin_file_that_ends_its_day(mock_db_session, product):
    """A daily job is missing spin until a file carries coverage past midnight."""
    job = _job(*product)
    target_start, target_end = parse_dates_from_partition_key(DAY_PARTITION)

    _insert(mock_db_session, SEPT_9_TO_10)
    with pytest.raises(imap_job.MissingDependenciesError, match="spin"):
        job.get_spin_files_inputs(mock_db_session, target_start, target_end)

    _insert(mock_db_session, SEPT_10_TO_11)
    spin_files = job.get_spin_files_inputs(mock_db_session, target_start, target_end)
    assert set(spin_files) == {SEPT_9_TO_10.file_path, SEPT_10_TO_11.file_path}


def test_pointing_job_needs_only_the_files_overlapping_its_pointing(mock_db_session):
    """A pointing job runs once its pointing is covered, without the next file."""
    job = _job("hi", "l1b", "45sensor-de")
    target_start, target_end = parse_dates_from_partition_key(POINTING_PARTITION)

    _insert(mock_db_session, SEPT_9_TO_10)
    with pytest.raises(imap_job.MissingDependenciesError, match="spin"):
        job.get_spin_files_inputs(mock_db_session, target_start, target_end)

    _insert(mock_db_session, SEPT_8_TO_9)
    spin_files = job.get_spin_files_inputs(mock_db_session, target_start, target_end)
    assert set(spin_files) == {SEPT_8_TO_9.file_path, SEPT_9_TO_10.file_path}


def test_pointing_job_does_not_need_coverage_before_its_pointing(mock_db_session):
    """Coverage is judged from the pointing start, not from midnight."""
    job = _job("hi", "l1b", "45sensor-de")
    target_start, target_end = parse_dates_from_partition_key(POINTING_PARTITION)
    exact_pointing = SpinRecord(
        "imap_2026_252_2026_253_01.spin", target_start, target_end
    )

    _insert(mock_db_session, exact_pointing)

    assert job.get_spin_files_inputs(mock_db_session, target_start, target_end) == [
        exact_pointing.file_path
    ]


def test_spin_arrival_retries_a_partition_that_never_produced_output(
    mock_db_session, ephemeral_instance
):
    """Hi L1B does not trigger from spin, yet a partition that only skipped reruns."""
    job = _job("hi", "l1b", "45sensor-de")
    sensor = defs.get_sensor_def(f"{job.job_config.to_dagster_name()}_kickoff_sensor")
    partition = "repoint2_2026-01-02T00:00:00_to_2026-01-02T23:59:59"

    # A run that skipped for missing dependencies ends in SUCCESS without output
    ephemeral_instance.create_run_for_job(
        defs.resolve_job_def(job.dagster_job_name),
        status=DagsterRunStatus.SUCCESS,
        tags={"dagster/partition": partition},
    )
    _insert_spin_file(
        mock_db_session,
        "imap_2026_002_2026_002_01.spin",
        start_date=datetime.datetime(2026, 1, 2, 6),
        end_date=datetime.datetime(2026, 1, 2, 18),
    )

    run_requests = list(sensor(build_sensor_context(instance=ephemeral_instance)))
    assert [request.partition_key for request in run_requests] == [partition]

    # Once the partition has produced output, a new spin file does not rerun it
    ephemeral_instance.report_runless_asset_event(
        AssetMaterialization(
            asset_key=job.job_config.outputs[0].to_dagster_asset(),
            partition=partition,
        )
    )
    _insert_spin_file(
        mock_db_session,
        "imap_2026_002_2026_002_02.spin",
        upload_time=1,
        start_date=datetime.datetime(2026, 1, 2, 6),
        end_date=datetime.datetime(2026, 1, 2, 18),
    )

    run_requests = list(sensor(build_sensor_context(instance=ephemeral_instance)))
    assert run_requests == []

    # A failed run that left partial output is still retried, as before
    failed_partition = "repoint3_2026-01-03T00:00:00_to_2026-01-03T23:59:59"
    ephemeral_instance.create_run_for_job(
        defs.resolve_job_def(job.dagster_job_name),
        status=DagsterRunStatus.FAILURE,
        tags={"dagster/partition": failed_partition},
    )
    ephemeral_instance.report_runless_asset_event(
        AssetMaterialization(
            asset_key=job.job_config.outputs[0].to_dagster_asset(),
            partition=failed_partition,
        )
    )
    _insert_spin_file(
        mock_db_session,
        "imap_2026_003_2026_003_01.spin",
        upload_time=2,
        start_date=datetime.datetime(2026, 1, 3, 6),
        end_date=datetime.datetime(2026, 1, 3, 18),
    )

    run_requests = list(sensor(build_sensor_context(instance=ephemeral_instance)))
    assert [request.partition_key for request in run_requests] == [failed_partition]


def test_verify_spin_coverage_tolerates_the_seam_between_files():
    """Adjacent files meet within a fraction of a spin period."""
    late_by_20ms = SEPT_9_TO_10._replace(
        start_date=SEPT_8_TO_9.end_date + datetime.timedelta(milliseconds=20)
    )

    assert verify_spin_coverage(
        [SEPT_8_TO_9, late_by_20ms, SEPT_10_TO_11],
        datetime.datetime(2026, 9, 9),
        datetime.datetime(2026, 9, 11),
    )


def test_verify_spin_coverage_seam_tolerance_is_less_than_a_spin():
    """A seam up to the tolerance passes; a missing spin does not."""
    seam = SEPT_9_TO_10._replace(start_date=SEPT_8_TO_9.end_date + SPIN_GAP_TOLERANCE)
    missing_spin = SEPT_9_TO_10._replace(
        start_date=SEPT_8_TO_9.end_date + datetime.timedelta(seconds=15)
    )
    window = (datetime.datetime(2026, 9, 9), datetime.datetime(2026, 9, 10, 12))

    assert verify_spin_coverage([SEPT_8_TO_9, seam], *window)
    assert not verify_spin_coverage([SEPT_8_TO_9, missing_spin], *window)


def test_verify_spin_coverage_reports_a_missing_file():
    """A hole the size of a file is a gap."""
    assert not verify_spin_coverage(
        [SEPT_8_TO_9, SEPT_10_TO_11],
        datetime.datetime(2026, 9, 9),
        datetime.datetime(2026, 9, 11),
    )


def test_verify_spin_coverage_longer_file_spans_shorter_ones():
    """A longer file covers the window despite shorter files sorted around it."""
    records = [
        SpinRecord(
            "imap_2026_204_2026_205_01.spin",
            datetime.datetime(2026, 7, 23, 10),
            datetime.datetime(2026, 7, 24, 10),
        ),
        SpinRecord(
            "imap_2026_204_2026_206_02.spin",
            datetime.datetime(2026, 7, 23, 10),
            datetime.datetime(2026, 7, 25, 10),
        ),
        SpinRecord(
            "imap_2026_205_2026_205_01.spin",
            datetime.datetime(2026, 7, 24, 10),
            datetime.datetime(2026, 7, 24, 22),
        ),
        SpinRecord(
            "imap_2026_206_2026_206_02.spin",
            datetime.datetime(2026, 7, 25, 0, 30),
            datetime.datetime(2026, 7, 25, 10),
        ),
        SpinRecord(
            "imap_2026_206_2026_207_02.spin",
            datetime.datetime(2026, 7, 25, 10),
            datetime.datetime(2026, 7, 26, 10),
        ),
    ]

    assert verify_spin_coverage(
        records, datetime.datetime(2026, 7, 24), datetime.datetime(2026, 7, 25)
    )
