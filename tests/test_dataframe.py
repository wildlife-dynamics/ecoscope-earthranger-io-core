import datetime as dt
import io
import re

import pyarrow
import pytest

pytest.importorskip(
    "geopandas",
    reason="geopandas is required for testing dataframe.py",
)
pytest.importorskip("pandera", reason="pandera is required for testing dataframe.py")

import geopandas as gpd  # type: ignore[import-untyped]
import pandera.pandas
from shapely.geometry import Point

from ecoscope_earthranger_io_core.arrow import TRANSFORMS, SchemaChoices
from ecoscope_earthranger_io_core.dataframe import ObservationsGDFSchema
from ecoscope_earthranger_io_core.query import ObservationsQuery

from conftest import create_mock_observations_record_batch


def test_observations_gdf_schema():
    gdf = gpd.GeoDataFrame(
        {
            "geometry": [Point((0, 0)), Point((0, 1))],
            "groupby_col": ["subject1", "subject1"],
            "fixtime": [
                dt.datetime.fromisoformat("2023-01-01T00:00:00+00:00"),
                dt.datetime.fromisoformat("2023-01-02T00:00:00+00:00"),
            ],
            "junk_status": [False, False],
        }
    )
    ObservationsGDFSchema.validate(gdf)


def test_observations_gdf_schema_missing_column_raises():
    gdf = gpd.GeoDataFrame(
        {
            "geometry": [Point((0, 0)), Point((0, 1))],
            "groupby_col": ["subject1", "subject1"],
            "junk_status": [False, False],
        }
    )
    match = re.escape(
        "column 'fixtime' not in dataframe. "
        "Columns in dataframe: ['geometry', 'groupby_col', 'junk_status']"
    )
    with pytest.raises(pandera.pandas.errors.SchemaError, match=match):
        ObservationsGDFSchema.validate(gdf)


def test_observations_from_arrow():
    query = ObservationsQuery(
        tenant_domain="some-site.pamdas.org",
        subject_ids=["subject1", "subject2"],
        range_start=dt.datetime(2023, 1, 1),
        range_end=dt.datetime(2023, 12, 31),
    )
    transform = TRANSFORMS[SchemaChoices.ECOSCOPE_SLIM_V1]
    rb = create_mock_observations_record_batch(
        query=query,
        columns=transform.required_columns,
        schema=transform.pre_transform_schema,
    )
    as_ecoscope_rb = transform.transform(rb)
    table = pyarrow.Table.from_batches([as_ecoscope_rb])
    obs = gpd.GeoDataFrame.from_arrow(table)
    assert len(obs) > 0
    ObservationsGDFSchema.validate(obs, lazy=True)


def test_observations_ipc_roundtrip_geometry_not_naive_for_to_crs():
    """CRS must survive IPC so GeoPandas is not naive (avoids to_crs ValueError).

    Regression: ``geometry.to_crs(4326)`` raises
    ``ValueError: Cannot transform naive geometries`` when Arrow/GeoArrow
    CRS metadata is missing after ``GeoDataFrame.from_arrow``.
    """
    query = ObservationsQuery(
        tenant_domain="some-site.pamdas.org",
        subject_ids=["subject1", "subject2"],
        range_start=dt.datetime(2023, 1, 1),
        range_end=dt.datetime(2023, 12, 31),
    )
    transform = TRANSFORMS[SchemaChoices.ECOSCOPE_SLIM_V1]
    rb = create_mock_observations_record_batch(
        query=query,
        columns=transform.required_columns,
        schema=transform.pre_transform_schema,
    )
    as_ecoscope_rb = transform.transform(rb)
    sink = io.BytesIO()
    writer = pyarrow.ipc.new_stream(sink, transform.stream_schema)
    writer.write_batch(as_ecoscope_rb)
    writer.close()
    sink.seek(0)
    table = pyarrow.ipc.open_stream(sink).read_all()
    obs = gpd.GeoDataFrame.from_arrow(table)
    assert obs.geometry.crs is not None
    assert obs.geometry.crs.to_epsg() == 4326
    obs.geometry.to_crs(4326)
