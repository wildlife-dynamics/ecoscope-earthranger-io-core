"""Translate EarthRanger ``EventType.schema`` (draft-04 JSON-Schema) into a
pyarrow struct type, and coerce JSON ``event_details`` strings into it.

An EarthRanger event type carries a JSON schema describing the shape of its
``event_details``. We map that schema to a pyarrow ``StructType`` so
``event_details`` can be exposed as a typed nested column rather than opaque
JSON text.

Security note: the raw schema TEXT is untrusted and may contain Django template
tags (``{{ ... }}`` / ``{% ... %}``) in ``enum``/``enumNames`` value positions.
We NEVER render those templates — they are replaced with a harmless quoted
literal purely so the surrounding JSON parses. We only ever read structural
keys (``type``/``items``/``properties``/``format``), never enum values, so the
substituted content is irrelevant.

Datetime opt-in: when ``parse_datetimes=True`` the JSON-Schema ``format``
keyword is honored — ``format: "date-time"`` maps to ``timestamp("us","UTC")``
and ``format: "date"`` maps to ``date32`` (instead of ``string``). The value
path then parses those ISO-8601 strings into Arrow temporal values (naive
datetimes assumed UTC); an unparseable value is treated exactly like any other
type mismatch (nulled in coerce mode, row dropped in drop mode). When the flag
is False (default) ``format`` is ignored and such fields stay ``string`` — the
historical behavior.
"""

import json
import re
from dataclasses import dataclass
from dataclasses import field as _dc_field
from datetime import date, datetime, timezone

import pyarrow as pa


# Maximum nesting depth and total field count we are willing to materialize.
# Beyond these, we stop recursing (treat the offending node as a string) rather
# than raising, so a pathological schema can never blow up the translator.
_MAX_DEPTH = 8
_MAX_FIELDS = 1000

# Matches Django template runs (``{{ ... }}`` or ``{% ... %}``) that may appear
# in value positions of the raw schema text and would otherwise break JSON
# parsing. A single ``{% for %}...{% endfor %}`` block can emit a comma-less
# sequence of literals (e.g. ``"a""b"``) that is itself invalid JSON, so we
# greedily collapse a whole *run* of template tags together with any
# template-emitted scaffolding between them (whitespace, quotes, commas,
# brackets) into one quoted, JSON-safe literal. We never render the templates.
_TEMPLATE_RE = re.compile(
    r"(?:\{\{.*?\}\}|\{%.*?%\})(?:[\s,\"'\[\]]*(?:\{\{.*?\}\}|\{%.*?%\}))*",
    re.DOTALL,
)

# JSON-safe placeholder substituted for any template run. The leading/trailing
# quotes turn the run into a valid JSON string literal in value positions.
_TEMPLATE_PLACEHOLDER = '"__ERTEMPLATE__"'

# JSON-Schema scalar type name -> Arrow type factory.
_SCALAR_TYPES = {
    "string": pa.string,
    "number": pa.float64,
    "integer": pa.int64,
    "boolean": pa.bool_,
}

# Arrow temporal types used when ``parse_datetimes`` honors the ``format`` hint.
_DATETIME_TYPE = pa.timestamp("us", tz="UTC")
_DATE_TYPE = pa.date32()


def _neutralize_templates(raw_schema_text: str) -> str:
    """Replace Django template runs with a quoted JSON-safe literal.

    Templates are treated as opaque data and are never rendered.
    """
    return _TEMPLATE_RE.sub(_TEMPLATE_PLACEHOLDER, raw_schema_text)


class _FieldBudget:
    """Mutable counter that caps the total number of materialized fields."""

    __slots__ = ("count",)

    def __init__(self) -> None:
        self.count = 0

    def take(self) -> bool:
        """Consume one field from the budget; return False once exhausted."""
        if self.count >= _MAX_FIELDS:
            return False
        self.count += 1
        return True


@dataclass(frozen=True)
class RequiredSpec:
    """The ``required`` constraints of one object level of an event schema.

    ``required`` is the set of field names that must be present (and non-null)
    at this level. ``nested`` maps a field name to ``(kind, child)`` where
    ``kind`` is ``"object"`` (the field is a struct) or ``"array"`` (the field
    is a list whose items are structs), and ``child`` is that nested level's
    spec — so required constraints are checked recursively into present nested
    objects and array items. Kept separate from the Arrow struct type (which
    stays all-nullable) so that required-ness never affects array building or
    serialization, only drop-mode row filtering.
    """

    required: frozenset[str] = frozenset()
    nested: dict[str, tuple[str, "RequiredSpec"]] = _dc_field(default_factory=dict)


_EMPTY_REQUIRED = RequiredSpec()


def _node_to_arrow(
    prop: object, depth: int, budget: _FieldBudget, parse_datetimes: bool
) -> tuple[pa.DataType, tuple[str, RequiredSpec] | None]:
    """Map one JSON-Schema property node to ``(arrow_type, nested_required)``.

    ``nested_required`` is ``("object", spec)`` when the node is an object,
    ``("array", item_spec)`` when it is an array of objects, else ``None``.
    Falls back to ``pa.string()`` for anything unknown/malformed/union/enum or
    when the recursion-depth budget is exhausted.
    """
    if depth > _MAX_DEPTH or not isinstance(prop, dict):
        return pa.string(), None

    # An enum is always surfaced as a string, regardless of declared ``type``
    # or ``format`` — enum precedes format.
    if "enum" in prop:
        return pa.string(), None

    declared_type = prop.get("type")

    # A union type (list) or a missing/non-string type degrades to string.
    if not isinstance(declared_type, str):
        return pa.string(), None

    if declared_type in _SCALAR_TYPES:
        # The JSON-Schema ``format`` keyword is honored only as an opt-in
        # (``parse_datetimes``); otherwise these arrive as ISO-8601 JSON strings
        # and are kept as ``pa.string()`` (``format`` is an often-violated UI
        # hint and per-value parsing is lossy). When opted in, ``date-time`` and
        # ``date`` string fields become Arrow temporal types and the value path
        # parses them (null/drop on failure).
        if parse_datetimes and declared_type == "string":
            fmt = prop.get("format")
            if fmt == "date-time":
                return _DATETIME_TYPE, None
            if fmt == "date":
                return _DATE_TYPE, None
        return _SCALAR_TYPES[declared_type](), None

    if declared_type == "array":
        item_type, item_nested = _node_to_arrow(
            prop.get("items"), depth + 1, budget, parse_datetimes
        )
        # Carry the item's required spec only when the items are objects.
        nested = (
            ("array", item_nested[1])
            if item_nested is not None and item_nested[0] == "object"
            else None
        )
        return pa.list_(item_type), nested

    if declared_type == "object":
        struct_type, req = _properties_to_struct(
            prop.get("properties"),
            prop.get("required"),
            depth + 1,
            budget,
            parse_datetimes,
        )
        return struct_type, ("object", req)

    # Unknown declared type.
    return pa.string(), None


def _properties_to_struct(
    properties: object,
    required: object,
    depth: int,
    budget: _FieldBudget,
    parse_datetimes: bool,
) -> tuple[pa.StructType, RequiredSpec]:
    """Build ``(struct_type, RequiredSpec)`` from a JSON-Schema object level.

    Preserves JSON property order (dict insertion order). Struct fields are
    always nullable; ``required`` is captured separately. Returns an empty
    struct + empty spec when ``properties`` is missing/not a mapping or the
    depth budget is exhausted.
    """
    if depth > _MAX_DEPTH or not isinstance(properties, dict):
        return pa.struct([]), _EMPTY_REQUIRED

    required_names = set(required) if isinstance(required, list) else set()
    fields: list[pa.Field] = []
    present_required: set[str] = set()
    nested: dict[str, tuple[str, RequiredSpec]] = {}
    for key, prop in properties.items():
        if not budget.take():
            break
        skey = str(key)
        field_type, child = _node_to_arrow(prop, depth, budget, parse_datetimes)
        fields.append(pa.field(skey, field_type))
        # Only enforce required for names we actually model as fields.
        if skey in required_names:
            present_required.add(skey)
        if child is not None:
            nested[skey] = child
    return pa.struct(fields), RequiredSpec(frozenset(present_required), nested)


def _extract_schema_object(parsed: dict) -> dict | None:
    """Locate the JSON-Schema object (the dict carrying ``properties``).

    EarthRanger stores event-type schemas in two shapes that have coexisted as
    the form builder evolved:

    - **Modern forms** (draft 2020-12): ``{"json": {"properties": {...},
      "required": [...]}, "ui": {...}}`` — the JSON-Schema lives under ``json``.
    - **Legacy forms** (draft-04): ``{"schema": {"properties": {...},
      "required": [...]}, "definition": [...]}`` — it lives under ``schema``.

    A bare top-level object is also accepted as a fallback. The first container
    that carries a ``properties`` mapping wins; ``json`` is checked first
    because the legacy ``definition`` array never carries fields.
    """
    for container_key in ("json", "schema"):
        container = parsed.get(container_key)
        if isinstance(container, dict) and isinstance(
            container.get("properties"), dict
        ):
            return container
    if isinstance(parsed.get("properties"), dict):
        return parsed
    return None


def event_details_schema(
    raw_schema_text: str | None,
    *,
    parse_datetimes: bool = False,
) -> tuple[pa.StructType, RequiredSpec]:
    """Translate an EarthRanger event-type schema into ``(struct, RequiredSpec)``.

    ``raw_schema_text`` is the raw TEXT stored on ``EventType.schema`` (modern
    forms nest the JSON-Schema under ``json``, legacy forms under
    ``schema``). The struct type is fully nullable; required constraints are
    returned separately. Returns ``(pa.struct([]), empty spec)`` on any parse
    failure or empty/missing input. Deterministic for a given input + flag.

    When ``parse_datetimes`` is True, ``date-time``/``date`` ``format`` hints
    map to Arrow temporal leaf types (see module docstring).
    """
    if not raw_schema_text:
        return pa.struct([]), _EMPTY_REQUIRED

    try:
        parsed = json.loads(_neutralize_templates(raw_schema_text))
    except (ValueError, TypeError):
        return pa.struct([]), _EMPTY_REQUIRED

    if not isinstance(parsed, dict):
        return pa.struct([]), _EMPTY_REQUIRED

    container = _extract_schema_object(parsed)
    if container is None:
        return pa.struct([]), _EMPTY_REQUIRED

    return _properties_to_struct(
        container.get("properties"),
        container.get("required"),
        depth=1,
        budget=_FieldBudget(),
        parse_datetimes=parse_datetimes,
    )


def json_schema_to_arrow_struct(
    raw_schema_text: str | None, *, parse_datetimes: bool = False
) -> pa.StructType:
    """Translate an event-type schema into a pyarrow struct type (struct only).

    Thin wrapper over :func:`event_details_schema` for callers (e.g. the schema
    discovery endpoint) that only need the Arrow type, not the required spec.
    """
    return event_details_schema(raw_schema_text, parse_datetimes=parse_datetimes)[0]


# ---------------------------------------------------------------------------
# Value coercion
# ---------------------------------------------------------------------------


def _has_temporal(field_type: pa.DataType) -> bool:
    """Whether ``field_type`` contains any timestamp/date leaf (recursively)."""
    if pa.types.is_timestamp(field_type) or pa.types.is_date(field_type):
        return True
    if pa.types.is_struct(field_type):
        return any(_has_temporal(f.type) for f in field_type)
    if pa.types.is_list(field_type):
        return _has_temporal(field_type.value_type)
    return False


def _parse_timestamp_value(value: str) -> object:
    """Parse an ISO-8601 string into a tz-aware datetime (naive -> UTC).

    Returns the original string unchanged on failure, so the downstream pyarrow
    build rejects it like any type mismatch (null in coerce, drop in drop).
    """
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return value
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _parse_date_value(value: str) -> object:
    """Parse an ISO-8601 date string into a ``date`` (tolerating datetimes).

    Returns the original string unchanged on failure.
    """
    try:
        return date.fromisoformat(value)
    except (ValueError, TypeError):
        pass
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
    except (ValueError, TypeError):
        return value


def _normalize_temporal(value: object, field_type: pa.DataType) -> object:
    """Parse ISO strings at temporal leaves of ``field_type`` into datetime/date.

    Only declared temporal leaves are touched; everything else passes through
    unchanged. A parseable string becomes a datetime/date object (accepted by
    the pyarrow build); an unparseable string is left as-is so the build rejects
    it like a type mismatch. Recurses into structs and lists.
    """
    if value is None:
        return None
    if pa.types.is_timestamp(field_type):
        return _parse_timestamp_value(value) if isinstance(value, str) else value
    if pa.types.is_date(field_type):
        return _parse_date_value(value) if isinstance(value, str) else value
    if pa.types.is_struct(field_type):
        if not isinstance(value, dict):
            return value
        types = {f.name: f.type for f in field_type}
        return {
            k: (_normalize_temporal(v, types[k]) if k in types else v)
            for k, v in value.items()
        }
    if pa.types.is_list(field_type):
        if not isinstance(value, list):
            return value
        item_type = field_type.value_type
        return [_normalize_temporal(v, item_type) for v in value]
    return value


def _normalize_decoded(
    decoded: list[object], struct_type: pa.StructType
) -> list[object]:
    """Apply :func:`_normalize_temporal` to each decoded dict, if needed.

    No-op (returns the input unchanged) when ``struct_type`` has no temporal
    leaf, preserving the exact non-datetime behavior and its fast paths.
    Non-dict entries (``None`` / the ``_BAD`` sentinel) pass through untouched.
    """
    if not _has_temporal(struct_type):
        return decoded
    return [
        _normalize_temporal(d, struct_type) if isinstance(d, dict) else d
        for d in decoded
    ]


def _coerce_value(value: object, field_type: pa.DataType) -> object:
    """Best-effort coerce a single decoded value to ``field_type``.

    Returns ``None`` for any value that cannot be represented as ``field_type``.
    Recurses into nested structs and lists, nulling individual offending
    members rather than failing the whole value. Never raises. Temporal leaves
    are expected to already hold datetime/date objects (see
    :func:`_normalize_temporal`); a leftover string fails the probe and nulls.
    """
    if value is None:
        return None

    if pa.types.is_struct(field_type):
        if not isinstance(value, dict):
            return None
        return _coerce_dict(value, field_type)

    if pa.types.is_list(field_type):
        if not isinstance(value, list):
            return None
        item_type = field_type.value_type
        return [_coerce_value(item, item_type) for item in value]

    # Scalar field: probe with pyarrow, nulling on any cast failure.
    try:
        pa.array([value], type=field_type)
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError):
        return None
    return value


def _coerce_dict(row: dict, struct_type: pa.StructType) -> dict:
    """Coerce a dict to a struct type, nulling any field that fails to cast.

    Drops extra keys (only declared fields are kept) and recurses for nested
    struct/list fields.
    """
    result: dict[str, object] = {}
    for field in struct_type:
        result[field.name] = _coerce_value(row.get(field.name), field.type)
    return result


def coerce_details_to_struct(
    values: list[str | None], struct_type: pa.StructType
) -> pa.Array:
    """Coerce a batch of JSON ``event_details`` strings into ``struct_type``.

    Each element of ``values`` is a JSON object string (or ``None``). Returns a
    ``pa.Array`` of exactly ``struct_type`` with length ``len(values)``.

    Per the pyarrow-20 spike, ``pa.array(dicts, type=struct_type)`` already
    handles missing-key -> null, extra-key -> dropped, nested struct/list,
    None-row -> null cell, and numeric -> int truncation. The only gap is that
    it RAISES on a type-mismatch value (e.g. ``"abc"`` into an ``int64``). We
    therefore try the whole batch fast-path first, and on an Arrow error fall
    back to a per-row build that nulls only the offending field(s). A single
    dirty row never fails the batch. Temporal leaves are pre-parsed into
    datetime/date objects so a valid ISO string conforms and an invalid one
    nulls.
    """
    if not values:
        return pa.array([], type=struct_type)

    decoded: list[dict | None] = []
    for cell in values:
        if cell is None:
            decoded.append(None)
            continue
        try:
            obj = json.loads(cell)
        except (ValueError, TypeError):
            decoded.append(None)
            continue
        # A decoded non-dict (scalar/list) cannot populate a struct row.
        decoded.append(obj if isinstance(obj, dict) else None)

    decoded = _normalize_decoded(decoded, struct_type)

    # Fast path: let pyarrow build the whole batch at once.
    try:
        return pa.array(decoded, type=struct_type)
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError):
        pass

    # Fallback: build per row, sanitizing dirty rows field-by-field.
    cells: list[dict | None] = []
    for row in decoded:
        if row is None:
            cells.append(None)
            continue
        try:
            pa.array([row], type=struct_type)
            cells.append(row)
        except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError):
            cells.append(_coerce_dict(row, struct_type))

    return pa.array(cells, type=struct_type)


# Sentinel for a cell whose JSON is unparseable or not a JSON object; such a
# row never conforms to a struct type.
_BAD = object()


def _decode_details(values: list[str | None]) -> tuple[list[object], bool]:
    """Decode ``event_details`` JSON strings once.

    Returns ``(decoded, saw_bad)`` where each decoded element is ``None`` (absent
    cell), the parsed ``dict``, or the ``_BAD`` sentinel (unparseable JSON or a
    non-object value). ``saw_bad`` is ``True`` if any element is ``_BAD``.
    """
    decoded: list[object] = []
    saw_bad = False
    for cell in values:
        if cell is None:
            decoded.append(None)
            continue
        try:
            obj = json.loads(cell)
        except (ValueError, TypeError):
            decoded.append(_BAD)
            saw_bad = True
            continue
        if isinstance(obj, dict):
            decoded.append(obj)
        else:
            decoded.append(_BAD)
            saw_bad = True
    return decoded, saw_bad


def _required_is_empty(required: RequiredSpec) -> bool:
    """Whether ``required`` (recursively) imposes no required-field constraint."""
    if required.required:
        return False
    return all(_required_is_empty(child) for _, child in required.nested.values())


def _required_satisfied(value: object, required: RequiredSpec) -> bool:
    """Whether a decoded ``event_details`` value satisfies ``required``.

    A required field must be present and non-null. A ``None``/absent value is
    treated as an empty object, so it fails as soon as any field is required at
    this level. Recurses into present nested objects and array-of-object items
    (an absent optional nested object/array is fine; a present one must satisfy
    its own required constraints).
    """
    obj = value if isinstance(value, dict) else {}
    for name in required.required:
        if obj.get(name) is None:  # absent key or explicit null
            return False
    for name, (kind, child) in required.nested.items():
        nested_value = obj.get(name)
        if nested_value is None:
            continue
        if kind == "object":
            if isinstance(nested_value, dict) and not _required_satisfied(
                nested_value, child
            ):
                return False
        else:  # "array" of objects
            if isinstance(nested_value, list):
                for element in nested_value:
                    if isinstance(element, dict) and not _required_satisfied(
                        element, child
                    ):
                        return False
    return True


def build_details_struct(
    values: list[str | None],
    struct_type: pa.StructType,
    *,
    drop_invalid: bool,
    required: RequiredSpec | None = None,
) -> tuple[pa.Array, list[bool]]:
    """Build the typed ``event_details`` struct array, optionally dropping rows.

    Returns ``(struct_array, keep_mask)`` where ``keep_mask`` has one entry per
    input value.

    - ``drop_invalid=False`` (coerce): every row is kept (mask all ``True``) and
      non-conforming values are coerced to null per :func:`coerce_details_to_struct`.
      ``required`` is ignored. ``struct_array`` has length ``len(values)``.
    - ``drop_invalid=True`` (drop): non-conforming rows are excluded
      (``keep_mask`` ``False``) and ``struct_array`` contains only the kept rows
      (length ``sum(keep_mask)``), so callers must filter the rest of the batch
      by ``keep_mask``.

    In drop mode a cell conforms when it is a JSON object that (a) casts cleanly
    to ``struct_type`` (missing keys -> null and extra keys -> dropped both cast
    cleanly; only a genuine type mismatch fails — including an unparseable value
    at a temporal leaf) **and** (b) satisfies ``required`` if given (every
    required field, recursively, present and non-null). A ``None``/absent cell
    is kept as a null struct **unless** ``required`` imposes a top-level required
    field, in which case it is dropped (it cannot satisfy the schema).

    Temporal handling (timestamp/date leaves) is inferred from ``struct_type``,
    so no flag is needed here — the schema, built with ``parse_datetimes``,
    already encodes whether a leaf is temporal.

    Performance: JSON is decoded once. When no required constraint applies and a
    batch casts cleanly, the whole struct array is built in a single ``pa.array``
    call (no per-row work). Per-row work is reached only when the batch has a
    non-conforming row, or when required-field filtering is in effect.
    """
    if not drop_invalid:
        return coerce_details_to_struct(values, struct_type), [True] * len(values)

    enforce_required = required is not None and not _required_is_empty(required)
    decoded, saw_bad = _decode_details(values)
    decoded = _normalize_decoded(decoded, struct_type)

    # Fast path: no decode failures and the whole batch casts cleanly.
    if not saw_bad:
        try:
            arr = pa.array(decoded, type=struct_type)
        except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError):
            arr = None
        if arr is not None:
            if not enforce_required:
                # Every row type-conforms and no required constraint -> keep all.
                return arr, [True] * len(decoded)
            # Types all conform; filter on required-field presence only.
            keep = [_required_satisfied(item, required) for item in decoded]
            kept = [
                (None if item is None else item)
                for item, ok in zip(decoded, keep)
                if ok
            ]
            return pa.array(kept, type=struct_type), keep

    # Slow path (a non-conforming row exists, or the batch type-build failed):
    # classify each decoded value individually.
    keep_mask: list[bool] = []
    kept_rows: list[dict | None] = []
    for item in decoded:
        if item is _BAD:
            keep_mask.append(False)
            continue
        if item is not None:
            try:
                pa.array([item], type=struct_type)
            except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError):
                keep_mask.append(False)
                continue
        if enforce_required and not _required_satisfied(item, required):
            keep_mask.append(False)
            continue
        keep_mask.append(True)
        kept_rows.append(None if item is None else item)
    return pa.array(kept_rows, type=struct_type), keep_mask


def details_invalid_mask(
    values: list[str | None],
    struct_type: pa.StructType,
    *,
    required: RequiredSpec | None = None,
    enforce_required: bool = False,
) -> list[bool]:
    """Per-row "is this ``event_details`` value INVALID?" predicate.

    The inverse of the conformance test :func:`build_details_struct` applies in
    drop mode — used by the API's ``invalid_only`` mode to return exactly the
    rows that drop/coerce would discard/null. Returns one bool per input value
    (``True`` = invalid).

    - ``enforce_required=True`` mirrors drop-mode conformance EXACTLY: a row is
      invalid iff its JSON is unparseable/non-object, OR it fails to cast to
      ``struct_type`` (type mismatch, incl. an unparseable temporal leaf), OR it
      violates ``required`` (recursively). With this flag the result equals
      ``[not k for k in build_details_struct(values, struct_type,
      drop_invalid=True, required=required)[1]]``.
    - ``enforce_required=False`` is the coerce-mode predicate: a row is invalid
      iff its JSON is unparseable/non-object OR at least one declared field would
      be coercion-nulled (a type mismatch). ``required`` is ignored; a missing
      optional key is NOT invalid.

    Temporal handling is inferred from ``struct_type`` (see
    :func:`build_details_struct`).
    """
    enforce = (
        enforce_required and required is not None and not _required_is_empty(required)
    )
    decoded, _ = _decode_details(values)
    decoded = _normalize_decoded(decoded, struct_type)

    mask: list[bool] = []
    for item in decoded:
        if item is _BAD:
            mask.append(True)
            continue
        if item is not None:
            try:
                pa.array([item], type=struct_type)
            except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError):
                mask.append(True)
                continue
        if enforce and not _required_satisfied(item, required):
            mask.append(True)
            continue
        mask.append(False)
    return mask
