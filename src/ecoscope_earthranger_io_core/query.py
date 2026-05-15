from datetime import datetime
from typing import Literal

from fastapi import Query
from pydantic import BaseModel, Field

QueryEngine = Literal["auto", "iceberg-bq", "iceberg-dd"]


class _WarehouseQuery(BaseModel):
    tenant_domain: str
    range_start: datetime | None = None
    range_end: datetime | None = None


PatrolStatus = Literal["active", "overdue", "done", "cancelled"]


_PATROLS_OVERLAP_DATERANGE_DESCRIPTION = (
    "If True (default), include patrols whose time range overlaps "
    "[range_start, range_end]; if False, include only patrols "
    "starting within that range."
)


class ObservationsQuery(_WarehouseQuery):
    """An EarthRanger observations query.

    Examples:

    ```python
    >>> from ecoscope_earthranger_io_core.query import ObservationsQuery
    >>> query = ObservationsQuery(
    ...     tenant_domain="some-site.pamdas.org",
    ...     range_start=datetime(2023, 1, 1),
    ...     range_end=datetime(2023, 12, 31),
    ...     subject_group_name="elephants",
    ... )
    >>>
    ```

    Or with patrol filters:

    ```python
    >>> query = ObservationsQuery(
    ...     tenant_domain="some-site.pamdas.org",
    ...     range_start=datetime(2023, 1, 1),
    ...     range_end=datetime(2023, 12, 31),
    ...     patrol_type_value=["routine_patrol"],
    ...     patrol_status=["done"],
    ...     include_patrol_details=True,
    ... )
    >>>
    ```
    """

    subject_ids: list[str] | None = None
    subject_group_name: str | None = None
    patrol_ids: list[str] | None = None
    patrol_type_value: list[str] | None = None
    patrol_status: list[PatrolStatus] | None = None
    patrols_overlap_daterange: bool = Field(
        default=True,
        description=_PATROLS_OVERLAP_DATERANGE_DESCRIPTION,
    )
    include_patrol_details: bool = False
    exclusion_flags: int | None = Field(
        default=None,
        ge=0,
        description=(
            "Bitmask filter. None=no filter, 0=clean only, "
            ">0=AND with bitmask must be >0."
        ),
    )

    @classmethod
    def from_query_params(
        cls,
        tenant_domain: str = Query(...),
        range_start: datetime | None = Query(None),
        range_end: datetime | None = Query(None),
        subject_ids: list[str] | None = Query(None),
        subject_group_name: str | None = Query(None),
        patrol_ids: list[str] | None = Query(None),
        patrol_type_value: list[str] | None = Query(None),
        patrol_status: list[PatrolStatus] | None = Query(None),
        patrols_overlap_daterange: bool = Query(True),
        include_patrol_details: bool = Query(False),
        exclusion_flags: int | None = Query(
            None,
            ge=0,
            description=(
                "Bitmask filter. None=no filter, 0=clean only, "
                ">0=AND with bitmask must be >0."
            ),
        ),
    ) -> "ObservationsQuery":
        return cls(
            tenant_domain=tenant_domain,
            range_start=range_start,
            range_end=range_end,
            subject_ids=subject_ids,
            subject_group_name=subject_group_name,
            patrol_ids=patrol_ids,
            patrol_type_value=patrol_type_value,
            patrol_status=patrol_status,
            patrols_overlap_daterange=patrols_overlap_daterange,
            include_patrol_details=include_patrol_details,
            exclusion_flags=exclusion_flags,
        )


class EventsQuery(_WarehouseQuery):
    """An EarthRanger events query.

    Examples:

    ```python
    >>> from ecoscope_earthranger_io_core.query import EventsQuery
    >>> query = EventsQuery(
    ...     tenant_domain="some-site.pamdas.org",
    ...     range_start=datetime(2023, 1, 1),
    ...     range_end=datetime(2023, 12, 31),
    ...     event_ids=["subject1", "subject2"],
    ... )
    >>>
    ```
    """

    event_ids: list[str]


class PatrolsQuery(_WarehouseQuery):
    """Query for fetching patrols from the warehouse API.

    Examples:

    ```python
    >>> from ecoscope_earthranger_io_core.query import PatrolsQuery
    >>> query = PatrolsQuery(
    ...     tenant_domain="some-site.pamdas.org",
    ...     range_start=datetime(2023, 1, 1),
    ...     range_end=datetime(2023, 12, 31),
    ...     patrol_type_value=["routine_patrol"],
    ...     patrol_status=["done"],
    ... )
    >>>
    ```
    """

    patrol_ids: list[str] | None = None
    patrol_type_value: list[str] | None = None
    patrol_status: list[PatrolStatus] | None = None
    patrols_overlap_daterange: bool = Field(
        default=True,
        description=_PATROLS_OVERLAP_DATERANGE_DESCRIPTION,
    )
    include_patrol_segments: bool = False
    flat: bool = True

    @classmethod
    def from_query_params(
        cls,
        tenant_domain: str = Query(...),
        range_start: datetime | None = Query(None),
        range_end: datetime | None = Query(None),
        patrol_ids: list[str] | None = Query(None),
        patrol_type_value: list[str] | None = Query(None),
        patrol_status: list[PatrolStatus] | None = Query(None),
        patrols_overlap_daterange: bool = Query(True),
        include_patrol_segments: bool = Query(False),
        flat: bool = Query(True),
    ) -> "PatrolsQuery":
        return cls(
            tenant_domain=tenant_domain,
            range_start=range_start,
            range_end=range_end,
            patrol_ids=patrol_ids,
            patrol_type_value=patrol_type_value,
            patrol_status=patrol_status,
            patrols_overlap_daterange=patrols_overlap_daterange,
            include_patrol_segments=include_patrol_segments,
            flat=flat,
        )


class _PatrolsQuery(_WarehouseQuery):
    # ToDo: conciliate this with PatrolsQuery (to remove this class)
    # Kept for backward compatibility with PatrolEventsQuery, until we support events in the warehouse API
    patrol_ids: list[str]
    patrol_statuses: list[PatrolStatus]


class PatrolEventsQuery(_PatrolsQuery):
    """An EarthRanger patrol events query.

    Examples:

    ```python
    >>> from ecoscope_earthranger_io_core.query import PatrolEventsQuery
    >>> query = PatrolEventsQuery(
    ...     tenant_domain="some-site.pamdas.org",
    ...     range_start=datetime(2023, 1, 1),
    ...     range_end=datetime(2023, 12, 31),
    ...     patrol_ids=["patrol1", "patrol2"],
    ...     patrol_statuses=["done"],
    ...     event_type_ids=["event1", "event2"],
    ... )
    >>>
    ```
    """

    event_type_ids: list[str]
