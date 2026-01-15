from datetime import datetime
from typing import Literal

from fastapi import Query
from pydantic import BaseModel


class _WarehouseQuery(BaseModel):
    tenant_domain: str
    range_start: datetime
    range_end: datetime


PatrolStatus = Literal["active", "overdue", "done", "cancelled"]


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

    Or with subject group name and patrol filters:

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
    patrol_type_value: list[str] | None = None
    patrol_status: list[PatrolStatus] | None = None
    include_patrol_details: bool = False

    @classmethod
    def from_query_params(
        cls,
        tenant_domain: str = Query(...),
        range_start: datetime = Query(...),
        range_end: datetime = Query(...),
        subject_ids: list[str] | None = Query(None),
        subject_group_name: str | None = Query(None),
        patrol_type_value: list[str] | None = Query(None),
        patrol_status: list[PatrolStatus] | None = Query(None),
        include_patrol_details: bool = Query(False),
    ) -> "ObservationsQuery":
        return cls(
            tenant_domain=tenant_domain,
            range_start=range_start,
            range_end=range_end,
            subject_ids=subject_ids,
            subject_group_name=subject_group_name,
            patrol_type_value=patrol_type_value,
            patrol_status=patrol_status,
            include_patrol_details=include_patrol_details,
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


class _PatrolsQuery(_WarehouseQuery):
    patrol_ids: list[str]
    patrol_statuses: list[PatrolStatus]


class PatrolObservationsQuery(_PatrolsQuery):
    """An EarthRanger patrol observations query.

    Examples:

    ```python
    >>> from ecoscope_earthranger_io_core.query import PatrolObservationsQuery
    >>> query = PatrolObservationsQuery(
    ...     tenant_domain="some-site.pamdas.org",
    ...     range_start=datetime(2023, 1, 1),
    ...     range_end=datetime(2023, 12, 31),
    ...     patrol_ids=["patrol1", "patrol2"],
    ...     patrol_statuses=["done"],
    ... )
    >>>
    ```
    """

    pass


class PatrolEventsQuery(_PatrolsQuery):
    """An EarthRanger patrol observations query.

    Examples:

    ```python
    >>> from ecoscope_earthranger_io_core.query import PatrolObservationsQuery
    >>> query = PatrolObservationsQuery(
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
