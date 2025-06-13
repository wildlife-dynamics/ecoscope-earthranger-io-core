import pyarrow as pa
import pytest
import geopandas as gpd

from ecoscope_earthranger_io_core.dataframe import ObservationsGDFSchema


@pytest.mark.xfail(reason="FIXME: rename columns to match dataframe schema")
def test_observations(mock_observations_record_batch: pa.RecordBatch):
    table = pa.Table.from_batches([mock_observations_record_batch])
    gdf = gpd.GeoDataFrame.from_arrow(table)
    assert len(gdf) > 0
    # FIXME: rename columns to match dataframe schema (location -> geometry, subject_name -> extra__subject__name, etc.)
    # E           pandera.errors.SchemaError: column 'geometry' not in dataframe.
    # Columns in dataframe: ['location', 'recorded_at', 'subject_id', 'subject_name', 'subject_subtype_id']
    #
    ObservationsGDFSchema.validate(gdf)
