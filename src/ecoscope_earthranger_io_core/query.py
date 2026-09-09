from datetime import datetime
from typing import Literal

from fastapi import Query
from pydantic import BaseModel, Field

QueryEngine = Literal["auto", "iceberg-bq", "iceberg-dd"]


class _TenantQuery(BaseModel):
    """Base for any tenant-scoped warehouse query. ``tenant_domain`` is the
    user-facing input; the API resolves it to an internal ``tenant_id`` on a
    subclass."""

    tenant_domain: str


class _WarehouseQuery(_TenantQuery):
    range_start: datetime | None = None
    range_end: datetime | None = None


PatrolStatus = Literal["active", "overdue", "done", "cancelled"]

EventState = Literal["new", "active", "resolved", "review"]


_EVENT_STATE_DESCRIPTION = (
    "Filter to events in any of these lifecycle states "
    "(new, active, resolved, review); None (default) = no state filter."
)


_PATROLS_OVERLAP_DATERANGE_DESCRIPTION = (
    "If True (default), include patrols whose time range overlaps "
    "[range_start, range_end]; if False, include only patrols "
    "starting within that range."
)


_INCLUDE_SUBJECT_ADDITIONAL_DESCRIPTION = (
    "If True, populate the subject `additional` JSON (surfaced as "
    "`extra__subject__additional`); if False (default), the column is still "
    "present in the schema but null, and the underlying JSON is not read. "
    "Opt in only when a consumer needs it -- e.g. per-subject track colouring "
    "reads the `rgb` key -- since it is a wide, free-form column."
)


_INCLUDE_PAUSES_DESCRIPTION = (
    "If True, include patrol legs flagged as a pause rather than active "
    "patrolling; if False (default), exclude them -- matching EarthRanger, so "
    "totals such as distance and duration agree with what the product reports. "
    "A leg ingested before the pause flag existed has no value for it and is "
    "treated as not-a-pause, so excluding pauses never silently drops history."
)

_PATROL_RAW_DETAILS_DESCRIPTION = (
    "Format override: serve `segment_details` and `type_details` as flat JSON "
    "strings instead of typed structs. Set this to query across several patrol "
    "types at once -- `type_details` is shaped by each leg's own patrol type, so "
    "typing it requires the query to name exactly one. (`segment_details` is "
    "shaped by the tenant's single site-wide segment schema and carries no such "
    "restriction.)"
)

_PATROL_PARSE_DETAIL_DATETIMES_DESCRIPTION = (
    "Typed-struct only: map `segment_details` / `type_details` date-time and "
    "date fields to Arrow timestamp/date instead of strings. Ignored when "
    "raw_details is True."
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
    include_subject_additional: bool = Field(
        default=False,
        description=_INCLUDE_SUBJECT_ADDITIONAL_DESCRIPTION,
    )
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
        include_subject_additional: bool = Query(
            False,
            description=_INCLUDE_SUBJECT_ADDITIONAL_DESCRIPTION,
        ),
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
            include_subject_additional=include_subject_additional,
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
    ...     state=["active"],
    ... )
    >>>
    ```
    """

    event_type: list[str] | None = None
    state: list[EventState] | None = Field(
        default=None,
        description=_EVENT_STATE_DESCRIPTION,
    )
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
        state: list[EventState] | None = Query(
            None, description=_EVENT_STATE_DESCRIPTION
        ),
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
            state=state,
            include_null_geometry=include_null_geometry,
            include_details=include_details,
            raw_details=raw_details,
            parse_detail_datetimes=parse_detail_datetimes,
            invalid_details=invalid_details,
            invalid_only=invalid_only,
        )


class EventTypesQuery(_TenantQuery):
    """Query for the warehouse /event_types listing (tenant-scoped)."""

    @classmethod
    def from_query_params(cls, tenant_domain: str = Query(...)) -> "EventTypesQuery":
        return cls(tenant_domain=tenant_domain)


class EventTypeSchemaQuery(_TenantQuery):
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
    include_pauses: bool = Field(
        default=False,
        description=_INCLUDE_PAUSES_DESCRIPTION,
    )
    raw_details: bool = Field(
        default=False,
        description=_PATROL_RAW_DETAILS_DESCRIPTION,
    )
    parse_detail_datetimes: bool = Field(
        default=False,
        description=_PATROL_PARSE_DETAIL_DATETIMES_DESCRIPTION,
    )

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
        include_pauses: bool = Query(
            False,
            description=_INCLUDE_PAUSES_DESCRIPTION,
        ),
        raw_details: bool = Query(
            False,
            description=_PATROL_RAW_DETAILS_DESCRIPTION,
        ),
        parse_detail_datetimes: bool = Query(
            False,
            description=_PATROL_PARSE_DETAIL_DATETIMES_DESCRIPTION,
        ),
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
            include_pauses=include_pauses,
            raw_details=raw_details,
            parse_detail_datetimes=parse_detail_datetimes,
        )


class PatrolTypesQuery(_TenantQuery):
    """Query for the warehouse /patrol_types listing (tenant-scoped).

    The patrol counterpart of ``EventTypesQuery``. The listing itself streams
    against ``PATROL_TYPES_SCHEMA_V1``, which carries display names but not the
    per-type schema documents; ``include_schema`` asks for those documents
    alongside it, so a consumer building a form for every patrol type does not
    have to follow up with one ``PatrolTypeSchemaQuery`` per type.

    What "schema document" means: das stores each patrol type's schema as a
    ``{"json": <JSON-Schema>, "ui": <form layout>}`` envelope. The API derives
    the typed ``type_details`` Arrow struct from the ``json`` half -- so a
    schema endpoint can serve either the derived Arrow struct or the stored
    document, and neither belongs in a listing row.

    Examples:

    ```python
    >>> from ecoscope_earthranger_io_core.query import PatrolTypesQuery
    >>> query = PatrolTypesQuery(tenant_domain="some-site.pamdas.org")
    >>> query.include_schema
    False

    ```

    Or asking for the schema documents too:

    ```python
    >>> query = PatrolTypesQuery(
    ...     tenant_domain="some-site.pamdas.org",
    ...     include_schema=True,
    ... )
    >>>
    ```
    """

    include_schema: bool = Field(
        default=False,
        description=(
            "If True, return each patrol type's schema document alongside the "
            "listing; if False (default), return the listing alone. A schema "
            "document is never a column of the Arrow listing -- it is a "
            "per-type document served in its own right, the same way "
            "`/events/schema` serves an event type's."
        ),
    )

    @classmethod
    def from_query_params(
        cls,
        tenant_domain: str = Query(...),
        include_schema: bool = Query(False),
    ) -> "PatrolTypesQuery":
        return cls(tenant_domain=tenant_domain, include_schema=include_schema)


class PatrolTypeSchemaQuery(_TenantQuery):
    """Lookup for a single patrol type's ``type_details`` schema.

    The patrol counterpart of ``EventTypeSchemaQuery``, and single-keyed for the
    same reason: the typed ``type_details`` struct is derived from exactly one
    patrol type's schema document.

    ``patrol_type_value`` accepts either the patrol type's UUID or its ``value``
    slug, as das does -- one key, two accepted spellings of it, rather than two
    fields where a caller could set both.

    Examples:

    ```python
    >>> from ecoscope_earthranger_io_core.query import PatrolTypeSchemaQuery
    >>> query = PatrolTypeSchemaQuery(
    ...     tenant_domain="some-site.pamdas.org",
    ...     patrol_type_value="routine_patrol",
    ... )
    >>>
    ```
    """

    patrol_type_value: str

    @classmethod
    def from_query_params(
        cls,
        tenant_domain: str = Query(...),
        patrol_type_value: str = Query(...),
    ) -> "PatrolTypeSchemaQuery":
        return cls(tenant_domain=tenant_domain, patrol_type_value=patrol_type_value)


class SegmentSchemaQuery(_TenantQuery):
    """Lookup for the tenant's site-wide patrol segment ("leg") schema.

    Tenant is the only key: unlike patrol types, there is exactly one segment
    schema per tenant, and it shapes the ``segment_details`` of every leg. That
    is also why ``segment_details`` can always be served as a typed struct, with
    no restriction on how many patrol types a patrol query spans.

    A tenant that never authored a schema is a normal state, not an absent one:
    das stores its canonical empty ``{json, ui}`` envelope, so this lookup has no
    "not found" case to model.

    Examples:

    ```python
    >>> from ecoscope_earthranger_io_core.query import SegmentSchemaQuery
    >>> query = SegmentSchemaQuery(tenant_domain="some-site.pamdas.org")
    >>>
    ```
    """

    @classmethod
    def from_query_params(
        cls,
        tenant_domain: str = Query(...),
    ) -> "SegmentSchemaQuery":
        return cls(tenant_domain=tenant_domain)


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
