"""Collection-definition fingerprints and state compatibility policies.

A *collection definition* is everything about a stream that determines what a
sync means: the resource(s) being queried, the selected API fields, how those
fields are aliased into the emitted schema, the primary key, and the
replication method/key.  The selected-field set alone is not enough: the tap
can change aliases or resource mappings without touching the field list, and
those changes alter both the emitted schema and the identity of the records
(`_sdc_record_hash`).

Bookmarks are only meaningful for the definition that produced them.  When a
definition changes we therefore classify the change and pick an explicit
strategy instead of silently reusing the old bookmark:

``ADDITIVE`` (the current definition only adds non-segment columns)
    -> ``limited_backfill``: rewind the report start date by a bounded number
       of days so newly selected columns are populated without discarding the
       progress made before that window.

``SEGMENT_CHANGE`` / ``IDENTITY_CHANGE`` (segments, aliases, resource mapping,
    primary keys, or an identity field removed)
    -> ``new_generation``: start a new extraction generation.  The previous
       generation's bookmarks are archived under an identifiable key (never
       silently reused) and the query restarts from ``start_date``.

``COLUMNS_CHANGED`` (core-stream attributes were removed; the primary-key
    cursor is independent of the projection) -> ``continue``.  For reports,
    removing an attribute is an ``IDENTITY_CHANGE`` because it changes the
    hashed row shape.

``METRIC_ONLY``
    -> ``continue``: metrics are excluded from record identity and do not
       change the grain, so the existing bookmark remains valid.

``LEGACY`` (state written before fingerprints were stored)
    -> ``continue``: the fingerprint is adopted and progress is preserved, so
       upgrading the tap does not force a full re-extraction for every
       customer.

Every decision is persisted in state.  A transition is *latched* for the
whole customer loop so that it applies to customers reached later, and it is
cleared only when the stream completes.  Per-customer progress markers let an
interrupted sync tell customers that have already migrated (keep their
progress) from customers still on the old definition (rewind).  For
``new_generation`` the ``identity_scope`` is folded
into ``_sdc_record_hash`` so the new generation's records are identifiable
and cannot collide with the previous generation's.
"""

import hashlib
import json

import singer
from singer import utils

LOGGER = singer.get_logger()

# Place in singer state where per-stream collection definitions are stored.
STATE_KEY = "collection_definitions"

CATEGORY_UNCHANGED = "UNCHANGED"
CATEGORY_LEGACY = "LEGACY"
CATEGORY_METRIC_ONLY = "METRIC_ONLY"
CATEGORY_ADDITIVE = "ADDITIVE"
CATEGORY_COLUMNS_CHANGED = "COLUMNS_CHANGED"
CATEGORY_SEGMENT_CHANGE = "SEGMENT_CHANGE"
CATEGORY_IDENTITY_CHANGE = "IDENTITY_CHANGE"

STRATEGY_CONTINUE = "continue"
STRATEGY_LIMITED_BACKFILL = "limited_backfill"
STRATEGY_NEW_GENERATION = "new_generation"

REPORT_STREAM = "report"
CORE_STREAM = "core"

# Keep the archived state bounded while retaining enough history to migrate
# from.  These are capped because state is re-emitted on every flush.
MAX_ARCHIVED_GENERATIONS = 10
MAX_APPLIED_TRANSITIONS = 20

_REQUIRED_COMPONENT_KEYS = (
    "stream_kind",
    "resource_names",
    "primary_keys",
    "replication_method",
    "replication_keys",
    "segments",
    "attributes",
    "metrics",
    "aliases",
    "identity_fields",
)


def _canonical_json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def compute_fingerprint(components):
    """Return a stable digest for a normalized definition component mapping."""
    return hashlib.sha256(_canonical_json(components).encode("utf-8")).hexdigest()


def compute_identity_fingerprint(components):
    """Digest of the parts of a definition that define record identity.

    Metrics are intentionally excluded: changing metric selection does not
    change ``_sdc_record_hash`` and therefore must not invalidate bookmarks.
    """
    identity_components = {
        "stream_kind": components["stream_kind"],
        "resource_names": components["resource_names"],
        "primary_keys": components["primary_keys"],
        # Segment selection determines the grain, so a new generation must not
        # reuse identities from the old grain.
        "segments": components["segments"],
        "identity_fields": components["identity_fields"],
        "aliases": {
            name: aliases
            for name, aliases in components["aliases"].items()
            if name in components["identity_fields"]
        },
    }
    return hashlib.sha256(
        _canonical_json(identity_components).encode("utf-8")
    ).hexdigest()


def _selected_metadata_entries(stream_mdata):
    for mdata in stream_mdata or []:
        breadcrumb = mdata.get("breadcrumb") or []
        if len(breadcrumb) < 2:
            continue
        metadata = mdata.get("metadata") or {}
        if not utils.should_sync_field(
            metadata.get("inclusion"), metadata.get("selected")
        ):
            continue
        yield mdata


def build_collection_definition(stream, stream_obj):
    """Build a normalized, JSON-serializable definition for ``stream``.

    Only *selected* fields are considered.  Available-but-unselected fields
    are deliberately ignored so that adding fields to the tap/API does not
    look like a customer definition change.
    """
    stream_mdata = stream.get("metadata") or []
    selected_fields = set()
    aliases = {}
    behaviors = {}

    for mdata in _selected_metadata_entries(stream_mdata):
        metadata = mdata["metadata"]
        output_name = mdata["breadcrumb"][1]
        api_field_names = metadata.get("tap-google-ads.api-field-names") or []

        if output_name == "_sdc_record_hash":
            continue

        selected_fields.update(api_field_names)
        if api_field_names:
            aliases[output_name] = sorted(set(api_field_names))
        behaviors[output_name] = metadata.get("behavior")

    primary_keys = sorted(stream_obj.primary_keys)
    stream_kind = REPORT_STREAM if "_sdc_record_hash" in primary_keys else CORE_STREAM

    segments = sorted(field for field in selected_fields if field.startswith("segments."))
    metrics = sorted(field for field in selected_fields if field.startswith("metrics."))
    attributes = sorted(
        field for field in selected_fields
        if not field.startswith("segments.") and not field.startswith("metrics.")
    )

    if stream_kind == REPORT_STREAM:
        # Report identity is `_sdc_record_hash`, which hashes every non-metric
        # value in the record.  Keep the emitted field names here because the
        # hash is computed over those names.
        identity_fields = sorted(
            output_name
            for output_name in aliases
            if behaviors.get(output_name) != "METRIC"
        )
    else:
        # Core streams are keyed by their primary key; selected columns do not
        # participate in record identity.
        identity_fields = primary_keys

    root_metadata = {}
    for mdata in stream_mdata:
        if not mdata.get("breadcrumb"):
            root_metadata = mdata.get("metadata") or {}
            break

    return {
        "stream_kind": stream_kind,
        "resource_names": sorted(getattr(stream_obj, "google_ads_resource_names", [])),
        "primary_keys": primary_keys,
        "replication_method": root_metadata.get("forced-replication-method"),
        "replication_keys": sorted(root_metadata.get("valid-replication-keys") or []),
        "selected_fields": sorted(selected_fields),
        "segments": segments,
        "attributes": attributes,
        "metrics": metrics,
        "aliases": aliases,
        "identity_fields": identity_fields,
    }


def _identity_names(components):
    """Output names whose values participate in record identity."""
    identity_fields = set(components.get("identity_fields") or [])
    if components.get("stream_kind") == REPORT_STREAM:
        return identity_fields
    # Core record identity is the primary key, not the projection.
    return set(components.get("primary_keys") or [])


def _aliases_incompatible(previous, current):
    """Return True when a retained identity name now maps to different fields."""
    previous_aliases = previous.get("aliases") or {}
    current_aliases = current.get("aliases") or {}
    names = _identity_names(previous) | _identity_names(current)

    for output_name in names:
        previous_mapping = previous_aliases.get(output_name)
        current_mapping = current_aliases.get(output_name)
        if previous_mapping is not None and current_mapping is not None:
            if previous_mapping != current_mapping:
                return True
    return False


def classify_collection_definition(previous, current):
    """Classify a definition transition.

    Returns ``(category, reason)``.  ``previous`` is ``None`` when no
    fingerprint has been stored yet (legacy state).
    """
    if previous is None or not isinstance(previous, dict):
        return CATEGORY_LEGACY, "no collection-definition fingerprint stored"

    if any(key not in previous for key in _REQUIRED_COMPONENT_KEYS):
        return CATEGORY_LEGACY, "stored fingerprint is incomplete"

    if previous.get("stream_kind") != current["stream_kind"]:
        return CATEGORY_IDENTITY_CHANGE, "stream kind changed"

    if previous.get("resource_names") != current["resource_names"]:
        return CATEGORY_IDENTITY_CHANGE, "Google Ads resource mapping changed"

    if previous.get("primary_keys") != current["primary_keys"]:
        return CATEGORY_IDENTITY_CHANGE, "primary key changed"

    if previous.get("replication_method") != current["replication_method"]:
        return CATEGORY_IDENTITY_CHANGE, "replication method changed"

    if previous.get("replication_keys") != current["replication_keys"]:
        return CATEGORY_IDENTITY_CHANGE, "replication key changed"

    if _aliases_incompatible(previous, current):
        return CATEGORY_IDENTITY_CHANGE, "an identity field's alias mapping changed"

    previous_segments = set(previous.get("segments") or [])
    current_segments = set(current["segments"])
    if previous_segments != current_segments:
        return CATEGORY_SEGMENT_CHANGE, "selected segments changed"

    previous_identity = set(previous.get("identity_fields") or [])
    current_identity = set(current["identity_fields"])
    if not previous_identity <= current_identity:
        return CATEGORY_IDENTITY_CHANGE, "a record-identity field was removed or remapped"

    previous_attributes = set(previous.get("attributes") or [])
    current_attributes = set(current["attributes"])
    previous_metrics = set(previous.get("metrics") or [])
    current_metrics = set(current["metrics"])

    if previous_attributes == current_attributes:
        if previous_metrics == current_metrics:
            return CATEGORY_UNCHANGED, "definition unchanged"
        return CATEGORY_METRIC_ONLY, "only metric selection changed"

    if previous_attributes < current_attributes:
        return CATEGORY_ADDITIVE, "attribute columns were added"

    # Attribute removal is a projection change.  For reports, identity is the
    # hash over non-metric values, so previously emitted hashes no longer
    # describe the same row shape.
    return CATEGORY_COLUMNS_CHANGED, "attribute columns were removed"


def strategy_for(category, stream_kind):
    """Map a classification to one of the explicit state strategies."""
    if category in (CATEGORY_UNCHANGED, CATEGORY_LEGACY, CATEGORY_METRIC_ONLY):
        # Metrics never affect grain or record identity, so progress is kept;
        # newly selected metrics simply apply going forward.
        return STRATEGY_CONTINUE
    if category == CATEGORY_ADDITIVE:
        # Core streams are FULL_TABLE: their interrupted-sync bookmark is a
        # primary-key cursor that does not depend on the selected columns, and
        # a completed sync removes the bookmark anyway.  Re-reading a bounded
        # window would be meaningless, so additions simply continue.
        if stream_kind == CORE_STREAM:
            return STRATEGY_CONTINUE
        return STRATEGY_LIMITED_BACKFILL
    if category == CATEGORY_COLUMNS_CHANGED and stream_kind == CORE_STREAM:
        # Dropping a projected column from a FULL_TABLE core stream does not
        # affect its primary-key cursor.
        return STRATEGY_CONTINUE
    return STRATEGY_NEW_GENERATION


def _archive_bookmarks(state, stream_name, previous_record, reason):
    """Move bookmarks out of the active slot, preserving them for migration."""
    bookmarks = state.get("bookmarks") or {}
    archived = bookmarks.pop(stream_name, None)

    history = []
    if previous_record and previous_record.get("history"):
        history.extend(previous_record["history"])

    if archived is not None:
        history.append(
            {
                "generation": previous_record.get("generation") if previous_record else 1,
                "fingerprint": previous_record.get("fingerprint") if previous_record else None,
                "identity_fingerprint": (
                    previous_record.get("identity_fingerprint") if previous_record else None
                ),
                "components": previous_record.get("components") if previous_record else None,
                "bookmarks": archived,
                "reason": reason,
            }
        )
    if len(history) > MAX_ARCHIVED_GENERATIONS:
        # Drop the oldest generations first; they are the least likely to be
        # migrated from and each carries every customer's bookmark.
        history = history[-MAX_ARCHIVED_GENERATIONS:]
    return history


def _begin_pending_transition(record, category, reason):
    """Latch a transition whose effect spans every customer of the stream.

    The decision is computed once and must not be re-derived per customer.
    Without latching, the first customer's sync would advance the stored
    definition and every later customer would look "unchanged" and silently
    keep its stale bookmark.
    """
    record["pending_transition"] = {
        "from_fingerprint": record.get("previous", {}).get("fingerprint"),
        "to_fingerprint": record["fingerprint"],
        "category": category,
        "strategy": record["strategy"],
        "reason": reason,
    }


def resolve_collection_definition(state, stream, stream_obj, log=LOGGER):
    """Evaluate (and persist) the definition for one stream.

    Mutates ``state``: stores the current fingerprint under
    ``state["collection_definitions"]`` and, for a ``new_generation``,
    archives active bookmarks so the next query starts at ``start_date``.

    Returns the resolved record consumed by ``sync``.  The resolved
    ``strategy`` applies to *every customer* in this sync, even though this
    runs once per customer.
    """
    stream_name = stream.get("tap_stream_id") or stream.get("stream")
    current = build_collection_definition(stream, stream_obj)
    fingerprint = compute_fingerprint(current)
    identity_fingerprint = compute_identity_fingerprint(current)

    definitions = state.setdefault(STATE_KEY, {})
    previous_record = definitions.get(stream_name)
    previous = previous_record.get("components") if previous_record else None

    # A transition already latched for this fingerprint stays latched until
    # the stream finishes.  This is what keeps customers past the first one
    # from silently resuming on a bookmark written under the old definition.
    pending = (previous_record or {}).get("pending_transition")
    already_latched = bool(
        pending
        and pending.get("to_fingerprint") == fingerprint
        and previous_record.get("fingerprint") == fingerprint
    )
    if already_latched:
        category = pending["category"]
        reason = pending["reason"]
        strategy = pending["strategy"]
    else:
        category, reason = classify_collection_definition(previous, current)
        strategy = strategy_for(category, current["stream_kind"])

    generation = previous_record.get("generation", 1) if previous_record else 1
    identity_scope = previous_record.get("identity_scope") if previous_record else None

    record = {
        "fingerprint": fingerprint,
        "identity_fingerprint": identity_fingerprint,
        "components": current,
        "category": category,
        "strategy": strategy,
        "generation": generation,
        "identity_scope": identity_scope,
        "reason": reason,
        "migrated": category == CATEGORY_LEGACY,
    }

    if previous_record:
        record["previous"] = {
            "generation": previous_record.get("generation"),
            "fingerprint": previous_record.get("fingerprint"),
            "category": previous_record.get("category"),
        }
        if previous_record.get("history"):
            # Preserve archived generations across subsequent syncs; this is
            # what makes old state identifiable and migratable later.
            record["history"] = previous_record["history"]
        if previous_record.get("customer_definitions"):
            # Per-customer progress markers survive the per-customer resolve
            # cycle; they are keyed by [generation, fingerprint], so entries
            # from an older generation simply stop matching.
            record["customer_definitions"] = previous_record["customer_definitions"]
        if previous_record.get("applied_transitions"):
            record["applied_transitions"] = previous_record["applied_transitions"]

    if strategy == STRATEGY_NEW_GENERATION:
        if not already_latched:
            record["history"] = _archive_bookmarks(
                state, stream_name, previous_record, reason
            )
            generation += 1
            record["generation"] = generation
            # Make the new generation's record identity explicit and distinct
            # from the archived generation's rows.
            record["identity_scope"] = identity_fingerprint
            _begin_pending_transition(record, category, reason)
            log.warning(
                "Collection definition for stream %s is incompatible (%s: %s). "
                "Starting generation %s from start_date; previous bookmarks were archived "
                "under state['%s']['%s']['history'].",
                stream_name,
                category,
                reason,
                generation,
                STATE_KEY,
                stream_name,
            )
        else:
            record["identity_scope"] = previous_record.get("identity_scope")
            record["pending_transition"] = pending
    elif category == CATEGORY_LEGACY:
        if previous_record is None and not (state.get("bookmarks") or {}).get(stream_name):
            log.info(
                "Recorded initial collection definition for stream %s.", stream_name
            )
        else:
            log.info(
                "Adopting legacy state for stream %s (no fingerprint stored); "
                "existing bookmarks are preserved and the definition fingerprint was recorded.",
                stream_name,
            )
    elif strategy == STRATEGY_LIMITED_BACKFILL:
        if not already_latched:
            _begin_pending_transition(record, category, reason)
            log.warning(
                "Collection definition for stream %s changed (%s: %s); "
                "continuing from existing bookmarks with a limited backfill.",
                stream_name,
                category,
                reason,
            )
        else:
            record["pending_transition"] = pending
    elif category == CATEGORY_METRIC_ONLY:
        log.info(
            "Metric-only definition change for stream %s; record identity and "
            "bookmarks are unchanged, new metrics apply going forward.",
            stream_name,
        )

    definitions[stream_name] = record
    return record


def complete_stream_transition(state, stream_name):
    """Mark a latched transition as applied across every customer.

    Called only after a stream's customer loop finishes, so an interrupted
    sync keeps the strategy latched for the customers it has not reached yet.
    """
    record = (state.get(STATE_KEY) or {}).get(stream_name)
    if not record or "pending_transition" not in record:
        return None

    transition = record.pop("pending_transition")
    applied = record.setdefault("applied_transitions", [])
    applied.append(transition)
    if len(applied) > MAX_APPLIED_TRANSITIONS:
        del applied[:-MAX_APPLIED_TRANSITIONS]
    return transition


def _customer_marker(definition):
    return [definition["generation"], definition["fingerprint"]]


def customer_has_current_definition(state, stream_name, customer_id, definition):
    """True when this customer already progressed under the current definition.

    Tracked inside the definition record rather than on the bookmark so the
    bookmark payload keeps its existing shape (``{"date": ...}``).  A customer
    that restarted into the new generation must not be rewound again just
    because another customer of the same stream has not been reached yet.
    """
    record = (state.get(STATE_KEY) or {}).get(stream_name) or {}
    markers = record.get("customer_definitions") or {}
    return markers.get(str(customer_id)) == _customer_marker(definition)


def mark_customer_definition(state, stream_name, customer_id, definition):
    """Record that ``customer_id`` has produced rows under this definition."""
    record = (state.get(STATE_KEY) or {}).get(stream_name)
    if not record:
        return
    markers = record.setdefault("customer_definitions", {})
    markers[str(customer_id)] = _customer_marker(definition)

