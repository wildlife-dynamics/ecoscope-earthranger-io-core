from datetime import datetime
from typing import Literal

from pydantic import BaseModel


class _WarehouseQuery(BaseModel):
    tenant_id: str
    range_start: datetime
    range_end: datetime


class ObservationsQuery(_WarehouseQuery):
    """An EarthRanger observations query.

    Examples:

    ```python
    >>> from ecoscope_earthranger_io_core.query import ObservationsQuery
    >>> query = ObservationsQuery(
    ...     tenant_id="tenant123",
    ...     range_start=datetime(2023, 1, 1),
    ...     range_end=datetime(2023, 12, 31),
    ...     subject_ids=["subject1", "subject2"],
    ... )
    >>>
    ```
    """

    subject_ids: list[str]


class EventsQuery(_WarehouseQuery):
    """An EarthRanger events query.

    Examples:

    ```python
    >>> from ecoscope_earthranger_io_core.query import EventsQuery
    >>> query = EventsQuery(
    ...     tenant_id="tenant123",
    ...     range_start=datetime(2023, 1, 1),
    ...     range_end=datetime(2023, 12, 31),
    ...     event_ids=["subject1", "subject2"],
    ... )
    >>>
    ```
    """

    event_ids: list[str]


PatrolStatus = Literal["active", "overdue", "done", "cancelled"]


class _PatrolsQuery(_WarehouseQuery):
    patrol_ids: list[str]
    patrol_statuses: list[PatrolStatus]


class PatrolObservationsQuery(_PatrolsQuery):
    """An EarthRanger patrol observations query.

    Examples:

    ```python
    >>> from ecoscope_earthranger_io_core.query import PatrolObservationsQuery
    >>> query = PatrolObservationsQuery(
    ...     tenant_id="tenant123",
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
    ...     tenant_id="tenant123",
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
