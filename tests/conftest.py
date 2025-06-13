import uuid
from datetime import datetime, timedelta
from typing import Generator

import pyarrow
import geoarrow.pyarrow as ga  # type: ignore[import-untyped]
import pytest

from ecoscope_earthranger_io_core.query import ObservationsQuery
from ecoscope_earthranger_io_core.arrow import OBSERVATIONS_ARROW_SCHEMA


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
    tenant_id: str,
    subject_ids: list[str],
    range_start: datetime,
    range_end: datetime,
    columns: list[str] | None = None,
) -> Generator:
    for dt in _split_datetime_range_by_delta(
        range_start=range_start,
        range_end=range_end,
        delta=timedelta(minutes=5.0),
    ):
        for subject_id in subject_ids:
            manufacturer_id = str(uuid.uuid4())
            source_id = str(uuid.uuid4())
            location: bytes = ga.as_wkb(["POINT (0 1)"])[0].wkb
            record = {
                "created_at": dt.isoformat(),
                "exclusion_flags": "mock-exclusion-flags",
                "is_active": True,
                "location": location.hex().upper(),
                "manufacturer_id": manufacturer_id,
                "recorded_at": dt.isoformat(),
                "subject_id": subject_id,
                "subject_name": "mock-subject-name",
                "subject_subtype_id": "mock-subject-subtype",
                "das_tenant_id": tenant_id,
                "domain": "https://mock-site.padas.org",
                "observation_id": str(uuid.uuid4()),
                "source_id": source_id,
            }
            if columns is None or not columns:
                yield record
            else:
                yield {col: record[col] for col in columns if col in record}


def _create_mock_observations_record_batch(
    query: ObservationsQuery,
    nrecords: int = 1000,
    schema: pyarrow.Schema = OBSERVATIONS_ARROW_SCHEMA,
) -> pyarrow.RecordBatch:
    pylist = []
    for item in _mock_observations_generator(**query.model_dump()):
        pylist.append(item)
        if len(pylist) == nrecords:
            break
    if not pylist:
        raise ValueError("No records generated, check the query parameters.")
    return pyarrow.RecordBatch.from_pylist(pylist, schema=schema)


@pytest.fixture
def mock_observations_record_batch() -> pyarrow.RecordBatch:
    """Fixture that provides a mock record batch of observations."""
    query = ObservationsQuery(
        tenant_id="tenant123",
        subject_ids=["subject1", "subject2"],
        range_start=datetime(2023, 1, 1),
        range_end=datetime(2023, 12, 31),
    )
    return _create_mock_observations_record_batch(query, nrecords=1000)
