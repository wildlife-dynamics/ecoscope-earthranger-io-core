import uuid
from datetime import datetime, timedelta
from typing import Any, AsyncIterable, Callable, Generator

import geoarrow.pyarrow as ga  # type: ignore[import-untyped]
import pyarrow
import pytest

from ecoscope_earthranger_io_core.arrow import (
    OBSERVATIONS_SCHEMA__EARTHRANGER_FULL_V1,
    PATROLS_NESTED_SCHEMA_V1,
)
from ecoscope_earthranger_io_core.query import ObservationsQuery, PatrolsQuery


def _split_datetime_range_by_delta(
    range_start: datetime,
    range_end: datetime,
    delta: timedelta,
) -> Generator[datetime, None, None]:
    yield range_start
    current_start_date = range_start
    while current_start_date < range_end:
        current_end_date = min(current_start_date + delta, range_end)
        yield current_end_date
        current_start_date = current_end_date


def _mock_observations_generator(
    tenant_domain: str,
    range_start: datetime,
    range_end: datetime,
    subject_ids: list[str] | None = None,
    columns: list[str] | None = None,
    include_patrol_details: bool = False,
    **kwargs,  # Accept and ignore additional kwargs like subject_group_name, patrol_type_value, etc.
) -> Generator:
    """Generate mock observation records.

    Args:
        tenant_domain: The tenant domain
        range_start: Start of time range
        range_end: End of time range
        subject_ids: List of subject IDs to generate data for
        columns: Optional list of columns to include in output
        include_patrol_details: If True, include patrol-related fields in output
        **kwargs: Additional kwargs (ignored) for compatibility with ObservationsQuery fields
    """
    # Default subject_ids for testing if not provided
    if subject_ids is None:
        subject_ids = ["subject1", "subject2"]

    # Mock patrol data for testing
    mock_patrol_id = str(uuid.uuid4())
    mock_patrol_serial = 12345

    for dt in _split_datetime_range_by_delta(
        range_start=range_start,
        range_end=range_end,
        delta=timedelta(minutes=5.0),
    ):
        mock_das_tenant_id = str(uuid.uuid4())
        for subject_id in subject_ids:
            manufacturer_id = str(uuid.uuid4())
            source_id = str(uuid.uuid4())
            location: bytes = ga.as_wkb(["POINT (0 1)"])[0].wkb

            # Base record with standard observation fields
            record: dict[str, Any] = {
                "created_at": dt.isoformat(),
                "exclusion_flags": "mock-exclusion-flags",
                "is_active": True,
                "location": location.hex().upper(),
                "manufacturer_id": manufacturer_id,
                "recorded_at": dt.isoformat(),
                "subject_id": subject_id,
                "subject_name": "mock-subject-name",
                "subject_subtype_id": "mock-subject-subtype",
                "das_tenant_id": mock_das_tenant_id,
                "domain": "https://mock-site.padas.org",
                "observation_id": str(uuid.uuid4()),
                "source_id": source_id,
            }

            # Add patrol fields if requested
            if include_patrol_details:
                record.update(
                    {
                        "patrol_id": mock_patrol_id,
                        "patrol_title": "Mock Patrol Title",
                        "patrol_serial_number": mock_patrol_serial,
                        "patrol_status": "done",
                        "patrol_type_value": "routine_patrol",
                        "patrol_type_display": "Routine Patrol",
                        "patrol_start_time": range_start.isoformat(),
                        "patrol_end_time": range_end.isoformat(),
                    }
                )

            if columns is None or not columns:
                yield record
            else:
                yield {col: record[col] for col in columns if col in record}


def create_mock_observations_record_batch(
    query: ObservationsQuery,
    columns: list[str] | None = None,
    schema: pyarrow.Schema = OBSERVATIONS_SCHEMA__EARTHRANGER_FULL_V1,
    nrecords: int = 1000,
) -> pyarrow.RecordBatch:
    """Create a mock RecordBatch from an ObservationsQuery.

    Args:
        query: The observations query with filter parameters
        columns: Optional list of columns to include
        schema: The PyArrow schema for the output RecordBatch
        nrecords: Maximum number of records to generate
    """
    pylist = []
    for item in _mock_observations_generator(**query.model_dump(), columns=columns):
        pylist.append(item)
        if len(pylist) == nrecords:
            break
    if not pylist:
        raise ValueError("No records generated, check the query parameters.")
    return pyarrow.RecordBatch.from_pylist(pylist, schema=schema)


def get_async_rb_generator_from_storage_backend(
    query: ObservationsQuery | Any,
    columns: list[str] | None,
    schema: pyarrow.Schema,
) -> Callable[[], AsyncIterable[pyarrow.RecordBatch]]:
    """Create an async generator function that yields mock RecordBatches.

    This simulates reading from a storage backend like Iceberg or DuckDB.

    Args:
        query: The observations query with filter parameters
        columns: Optional list of columns to include
        schema: The PyArrow schema for the output RecordBatches
    """

    async def _async_generator() -> AsyncIterable[pyarrow.RecordBatch]:
        for _ in range(1):  # Simulate a single batch for testing
            yield create_mock_observations_record_batch(
                query=query,
                columns=columns,
                schema=schema,
            )

    return _async_generator


def _mock_patrols_generator(
    range_start: datetime,
    range_end: datetime,
    num_patrols: int = 3,
    **kwargs: Any,
) -> Generator:
    """Generate mock patrol records with nested patrol_segments.

    Args:
        range_start: Start of time range
        range_end: End of time range
        num_patrols: Number of patrols to generate
        **kwargs: Additional kwargs (ignored) for compatibility
    """
    for i in range(num_patrols):
        patrol_id = str(uuid.uuid4())
        segment_start = range_start + timedelta(hours=i)
        segment_end = segment_start + timedelta(hours=2)

        # Create nested patrol_segments list
        patrol_segments = [
            {
                "id": str(uuid.uuid4()),
                "patrol_type": "routine_patrol",
                "patrol_type_display": "Routine Patrol",
                "leader_id": str(uuid.uuid4()),
                "time_range_start": segment_start.isoformat(),
                "time_range_end": segment_end.isoformat(),
                "scheduled_start": segment_start.isoformat(),
                "scheduled_end": segment_end.isoformat(),
                "start_location": None,
                "end_location": None,
            }
        ]

        yield {
            "id": patrol_id,
            "serial_number": 1000 + i,
            "priority": 0,
            "state": "done",
            "title": f"Mock Patrol {i + 1}",
            "objective": "Test objective",
            "created_at": range_start.isoformat(),
            "updated_at": range_start.isoformat(),
            "patrol_segments": patrol_segments,
        }


def create_mock_patrols_record_batch(
    query: PatrolsQuery,
    num_patrols: int = 3,
) -> pyarrow.RecordBatch:
    """Create a mock RecordBatch of patrols from a PatrolsQuery.

    Args:
        query: The patrols query with filter parameters
        num_patrols: Number of patrols to generate
    """
    pylist = list(
        _mock_patrols_generator(
            range_start=query.range_start,
            range_end=query.range_end,
            num_patrols=num_patrols,
        )
    )
    return pyarrow.RecordBatch.from_pylist(pylist, schema=PATROLS_NESTED_SCHEMA_V1)


def get_async_patrols_rb_generator(
    query: PatrolsQuery,
    num_patrols: int = 3,
) -> Callable[[], AsyncIterable[pyarrow.RecordBatch]]:
    """Create an async generator function that yields mock patrol RecordBatches."""

    async def _async_generator() -> AsyncIterable[pyarrow.RecordBatch]:
        yield create_mock_patrols_record_batch(query=query, num_patrols=num_patrols)

    return _async_generator


@pytest.fixture
def nrecords() -> int:
    """Fixture that provides the number of records to generate."""
    return 1000
