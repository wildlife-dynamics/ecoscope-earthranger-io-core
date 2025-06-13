"""Dataframe schemas for EarthRanger data. This schemas are for validation of dataframes by clients,
but are not expected to be used by servers, thus absolving servers of the responsibilty of taking on
a dependency on pandera, pandas, and geopandas.
"""

try:
    import pandas as pd
    import pandera.pandas as pa
    import pandera.typing as pa_typing
    import pandera.typing.geopandas as pa_typing_geopandas
except (ImportError, ModuleNotFoundError) as e:
    raise ImportError(
        "Pandera and geopandas are required for the dataframe schemas module but at least one of them "
        "is not installed. The conda release of ecoscope-earthranger-io-core packages these dependencies. "
        "They are not included in the pip release because they are not necessary on server side."
    ) from e


# This is very similar to
# `ecoscope_workflows_ext_ecoscope.schemas.SubjectGroupObservationsGDF`
# but we do not need:
#   - json serialization workarounds
#   - incremental application of placeholders for missing columns (?)
class ObservationsGDFSchema(pa.DataFrameModel):
    # required
    geometry: pa_typing_geopandas.GeoSeries = pa.Field()
    groupby_col: pa_typing.Series[str] = pa.Field()
    fixtime: pa_typing.Series[pd.DatetimeTZDtype] = pa.Field(
        dtype_kwargs={"unit": "ns", "tz": "UTC"}
    )
    junk_status: pa_typing.Series[bool] = pa.Field()

    # optional
    extra__subject__name: pa_typing.Series[str] = pa.Field(nullable=True)
    extra__subject__subject_subtype: pa_typing.Series[str] = pa.Field(nullable=True)
    extra__subject__sex: pa_typing.Series[str] = pa.Field(nullable=True)
