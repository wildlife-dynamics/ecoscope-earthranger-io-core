from fastapi import APIRouter, FastAPI, HTTPException, Depends, Query
from fastapi.responses import StreamingResponse

from ecoscope_earthranger_io_core.arrow import TRANSFORMS, SchemaChoices
from ecoscope_earthranger_io_core.query import ObservationsQuery

from conftest import get_async_rb_generator_from_storage_backend

app = FastAPI()
observations = APIRouter(prefix="/observations")


@observations.get("/stream/arrow")
async def get_observations_streaming_arrow(
    query: ObservationsQuery = Depends(ObservationsQuery.from_query_params),
    schema: SchemaChoices = Query(
        "ECOSCOPE_SLIM_V1",
        description="Schema to use for the response",
    ),
):
    """Stream observations as an Arrow IPC stream."""
    transform = TRANSFORMS[schema]
    async_batch_generator = get_async_rb_generator_from_storage_backend(
        query,
        columns=transform.required_columns,
        schema=transform.pre_transform_schema,
    )
    content_stream = transform.generate_bytes(
        async_batch_generator=async_batch_generator()
    )
    try:
        return StreamingResponse(
            content_stream,
            media_type="application/vnd.apache.arrow.stream",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read data: {str(e)}")


app.include_router(observations)
