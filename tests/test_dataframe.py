import datetime as dt
import re

import pytest

pytest.importorskip(
    "geopandas",
    reason="geopandas is required for testing dataframe.py",
)
pytest.importorskip("pandera", reason="pandera is required for testing dataframe.py")

import geopandas as gpd
import pandera.pandas as pa
from shapely.geometry import Point

from ecoscope_earthranger_io_core.dataframe import ObservationsGDFSchema


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
    with pytest.raises(pa.errors.SchemaError, match=match):
        ObservationsGDFSchema.validate(gdf)
