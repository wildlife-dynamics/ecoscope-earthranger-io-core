import uuid
from datetime import datetime, timedelta
from typing import AsyncIterable, Callable, Generator

import pyarrow
import geoarrow.pyarrow as ga  # type: ignore[import-untyped]
import pytest

from ecoscope_earthranger_io_core.query import ObservationsQuery
from ecoscope_earthranger_io_core.arrow import OBSERVATIONS_SCHEMA__EARTHRANGER_FULL_V1


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
        mock_das_tenant_id = str(uuid.uuid4())
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
                "das_tenant_id": mock_das_tenant_id,
                "domain": "https://mock-site.padas.org",
                "observation_id": str(uuid.uuid4()),
                "source_id": source_id,
            }
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
    pylist = []
    for item in _mock_observations_generator(**query.model_dump(), columns=columns):
        pylist.append(item)
        if len(pylist) == nrecords:
            break
    if not pylist:
        raise ValueError("No records generated, check the query parameters.")
    return pyarrow.RecordBatch.from_pylist(pylist, schema=schema)


def get_async_rb_generator_from_storage_backend(
    query: ObservationsQuery,
    columns: list[str] | None,
    schema: pyarrow.Schema,
) -> Callable[[], AsyncIterable[pyarrow.RecordBatch]]:
    async def _async_generator() -> AsyncIterable[pyarrow.RecordBatch]:
        for _ in range(1):  # Simulate a single batch for testing
            yield create_mock_observations_record_batch(
                query=query,
                columns=columns,
                schema=schema,
            )

    return _async_generator


@pytest.fixture
def nrecords() -> int:
    """Fixture that provides the number of records to generate."""
    return 1000


@pytest.fixture
def mock_observations_record_batch(nrecords: int) -> pyarrow.RecordBatch:
    """Fixture that provides a mock record batch of observations."""
    query = ObservationsQuery(
        tenant_domain="some-site.pamdas.org",
        subject_ids=["subject1", "subject2"],
        range_start=datetime(2023, 1, 1),
        range_end=datetime(2023, 12, 31),
    )
    return create_mock_observations_record_batch(query, nrecords=nrecords)
