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
    ...     event_type=["wildlife_sighting"],
    ... )
    >>>
    ```
    """

    event_type: list[str] | None = None
    include_null_geometry: bool = Field(
        default=True,
        description=(
            "If True (default), include events with no geometry; "
            "if False, exclude them."
        ),
    )
    include_details: bool = Field(
        default=False,
        description=(
            "If True, include the event_details payload (a typed struct derived "
            "from the event type's schema unless raw_details is set); if False "
            "(default), omit event_details."
        ),
    )
    raw_details: bool = Field(
        default=False,
        description=(
            "Format override: serve event_details as a flat JSON string "
            "instead of the typed struct. Works across any number of event "
            "types. Mutually exclusive with the typed-only options below."
        ),
    )
    parse_detail_datetimes: bool = Field(
        default=False,
        description=(
            "Typed-struct only: map event_details date-time/date fields to Arrow "
            "timestamp/date instead of strings."
        ),
    )
    invalid_details: Literal["drop", "coerce"] = Field(
        default="drop",
        description=(
            "Typed-struct only: how to handle event_details that fail schema "
            "validation -- 'drop' (default) excludes the event, 'coerce' keeps "
            "it with offending fields nulled. Ignored when raw_details is True."
        ),
    )
    invalid_only: bool = Field(
        default=False,
        description=(
            "Typed-struct only: return ONLY the events the active invalid_details "
            "policy treats as invalid (for bad-data inspection)."
        ),
    )

    @classmethod
    def from_query_params(
        cls,
        tenant_domain: str = Query(...),
        range_start: datetime | None = Query(None),
        range_end: datetime | None = Query(None),
        event_type: list[str] | None = Query(None),
        include_null_geometry: bool = Query(True),
        include_details: bool = Query(False),
        raw_details: bool = Query(False),
        parse_detail_datetimes: bool = Query(False),
        invalid_details: Literal["drop", "coerce"] = Query("drop"),
        invalid_only: bool = Query(False),
    ) -> "EventsQuery":
        return cls(
            tenant_domain=tenant_domain,
            range_start=range_start,
            range_end=range_end,
            event_type=event_type,
            include_null_geometry=include_null_geometry,
            include_details=include_details,
            raw_details=raw_details,
            parse_detail_datetimes=parse_detail_datetimes,
            invalid_details=invalid_details,
            invalid_only=invalid_only,
        )


class EventTypesQuery(BaseModel):
    """Query for the warehouse /event_types listing (tenant-scoped)."""

    tenant_domain: str

    @classmethod
    def from_query_params(cls, tenant_domain: str = Query(...)) -> "EventTypesQuery":
        return cls(tenant_domain=tenant_domain)


class EventTypeSchemaQuery(BaseModel):
    """Lookup for a single event type's ``event_details`` schema.

    Used by the warehouse ``/events/schema`` discovery endpoint, which serves
    exactly one event type (the typed ``event_details`` struct is derived from
    that type's JSON-Schema).

    Examples:

    ```python
    >>> from ecoscope_earthranger_io_core.query import EventTypeSchemaQuery
    >>> query = EventTypeSchemaQuery(
    ...     tenant_domain="some-site.pamdas.org",
    ...     event_type="wildlife_sighting",
    ... )
    >>>
    ```
    """

    tenant_domain: str
    event_type: str

    @classmethod
    def from_query_params(
        cls,
        tenant_domain: str = Query(...),
        event_type: str = Query(...),
    ) -> "EventTypeSchemaQuery":
        return cls(tenant_domain=tenant_domain, event_type=event_type)


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
    include_events: bool = True
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
        include_events: bool = Query(True),
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
            include_events=include_events,
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
