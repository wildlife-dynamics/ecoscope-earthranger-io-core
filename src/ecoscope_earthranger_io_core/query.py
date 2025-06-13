from datetime import datetime
from typing import Literal

from pydantic import BaseModel


class _WarehouseQuery(BaseModel):
    tenant_id: str
    range_start: datetime
    range_end: datetime
    columns: list[
        str
    ]  # e.g. for events ["id", "time", "event_type", "event_category", "reported_by", "serial_number", "geometry"]


class ObservationsQuery(_WarehouseQuery):
    subject_ids: list[str]


class EventsQuery(_WarehouseQuery):
    event_ids: list[str]


PatrolStatus = Literal["active", "overdue", "done", "cancelled"]


class _PatrolsQuery(_WarehouseQuery):
    patrol_ids: list[str]
    patrol_statuses: list[PatrolStatus]


class PatrolObservationsQuery(_PatrolsQuery):
    pass


class PatrolEventsQuery(_PatrolsQuery):
    event_type_ids: list[str]
