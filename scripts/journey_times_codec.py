"""A smaller wire form for the journey-times documents, and its exact inverse.

Every call a published journey makes carries ten fields, and most of them say
again what the document already knows. Both UTC epochs are the service day's
origin plus the seconds beside them. The report interval starts at the
observed epoch. The quality flags and match nearly always equal the journey's
own. And the times climb a few seconds at a time. Written out in full, the
700's file for nine days was 6.8 MB, about 950 KB even compressed, and each
complete day adds more than a megabyte.

`compact` writes each call as steps from the call before it, rebuilds the
epochs from `day_origins`, keeps the interval as the seconds until the next
report, and names a call's flags only where they differ from its journey's.
Repeated strings (timetable builds, source-file hashes, route patterns) move
into `tables`. Anything that does not fit those rules is kept explicitly in
the journey's `call_exceptions`, so nothing is ever approximated.

**Nothing is dropped.** `expand(compact(doc)) == doc` for every document, and
the builder refuses to write one where that fails. The browser expands the
same way (`jtExpandDocument` in app.js) before it reads a single journey, so
everything it computes comes from exactly the calls it read before.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from observation_contract import service_origin                  # noqa: E402

ENCODING = "compact-calls-1"

# The call as published before this encoding, and as `expand` returns it.
CALL_FORMAT = ["stop_index", "observed_secs", "scheduled_secs", "flags", "call_sequence",
               "observed_epoch", "scheduled_epoch", "observed_interval_epoch",
               "quality_flags", "match"]

# The call on the wire. Trailing fields are left off when empty.
ROW_FORMAT = ["stop_index_step", "observed_secs_step", "scheduled_secs_step", "flags",
              "call_sequence_step", "interval_secs", "call_tag"]

CALL_ENCODING_NOTE = (
    "Each call is a row of steps from the call before it (the first from zero) in "
    "stop_index, observed_secs, scheduled_secs and call_sequence, with flags as in "
    "call_format. interval_secs is how long after the observed time the next report "
    "came: -1 if none came, absent or null if the call has no interval. call_tag "
    "indexes tables.call_tags ([quality_flags, match]) and defaults to the journey's "
    "call_tag. Epochs are day_origins[day] plus the seconds unless "
    "call_exceptions.epochs gives them, and call_exceptions.intervals gives any "
    "interval not described by interval_secs. A journey's data_version, "
    "source_files and route_pattern index tables. Expanded, every call is exactly "
    "call_format.")

_JOURNEY_KEYS = ("call_tag", "call_exceptions")
_DOCUMENT_KEYS = ("encoding", "day_origins", "tables", "call_encoding")


def is_compact(doc):
    return isinstance(doc, dict) and doc.get("encoding") == ENCODING


def _whole(value):
    return isinstance(value, int) and not isinstance(value, bool)


def compact(doc):
    """The wire form of one expanded document. Raises rather than guess."""
    if is_compact(doc):
        raise ValueError("document is already compact")
    if doc.get("call_format") != CALL_FORMAT:
        raise ValueError(f"unknown call_format: {doc.get('call_format')}")
    clash = [k for k in _DOCUMENT_KEYS if k in doc]
    if clash:
        raise ValueError(f"document already uses {clash}")

    tables = {"call_tags": [], "data_versions": [], "route_patterns": [], "source_files": []}
    seen = {name: {} for name in tables}

    def ref(name, value):
        key = repr(value)
        if key not in seen[name]:
            seen[name][key] = len(tables[name])
            tables[name].append(value)
        return seen[name][key]

    origins = {day: service_origin(day) for day in sorted({j["day"] for j in doc["journeys"]})}
    journeys = []
    for journey in doc["journeys"]:
        if any(k in journey for k in _JOURNEY_KEYS):
            raise ValueError(f"journey {journey.get('trip_id')} already uses {_JOURNEY_KEYS}")
        origin = origins[journey["day"]]
        calls = journey["calls"]
        tags = [ref("call_tags", [list(c[8]), c[9]]) if len(c) == 10 else None for c in calls]
        default = max(sorted(set(tags)), key=tags.count) if tags else None
        rows, epochs, intervals = [], {}, {}
        previous = (0, 0, 0, 0)
        for n, (call, tag) in enumerate(zip(calls, tags)):
            if len(call) != len(CALL_FORMAT):
                raise ValueError(f"{journey.get('trip_id')} call {n} has {len(call)} fields")
            stop, observed, scheduled, flags, sequence, observed_epoch, scheduled_epoch, \
                interval, _flags, _match = call
            if not all(_whole(v) for v in (stop, observed, scheduled, flags, sequence)):
                raise ValueError(f"{journey.get('trip_id')} call {n} is not whole seconds: {call}")
            if (observed_epoch, scheduled_epoch) != (origin + observed, origin + scheduled):
                epochs[str(n)] = [observed_epoch, scheduled_epoch]
            step = None
            if interval is not None:
                if (isinstance(interval, list) and len(interval) == 2
                        and interval[0] == observed_epoch
                        and (interval[1] is None
                             or (_whole(interval[1]) and interval[1] >= observed_epoch))):
                    step = -1 if interval[1] is None else interval[1] - observed_epoch
                else:
                    intervals[str(n)] = interval
            row = [stop - previous[0], observed - previous[1], scheduled - previous[2], flags,
                   sequence - previous[3], step, None if tag == default else tag]
            while len(row) > 5 and row[-1] is None:
                row.pop()
            rows.append(row)
            previous = (stop, observed, scheduled, sequence)
        out = {**journey, "calls": rows, "call_tag": default}
        exceptions = {name: found for name, found in (("epochs", epochs), ("intervals", intervals)) if found}
        if exceptions:
            out["call_exceptions"] = exceptions
        if "data_version" in journey:
            out["data_version"] = ref("data_versions", journey["data_version"])
        if "source_files" in journey:
            out["source_files"] = [ref("source_files", s) for s in journey["source_files"]]
        if journey.get("route_pattern") is not None:
            out["route_pattern"] = ref("route_patterns", journey["route_pattern"])
        journeys.append(out)

    return {**doc, "journeys": journeys, "encoding": ENCODING, "day_origins": origins,
            "tables": tables,
            "call_encoding": {"row_format": ROW_FORMAT, "note": CALL_ENCODING_NOTE}}


def expand(doc):
    """The document as it was before `compact`. Returns a new object.

    Anything not compact is returned as it is, so a reader can take either.
    """
    if not is_compact(doc):
        return doc
    tables, origins = doc["tables"], doc["day_origins"]
    journeys = []
    for journey in doc["journeys"]:
        exceptions = journey.get("call_exceptions") or {}
        epochs, intervals = exceptions.get("epochs", {}), exceptions.get("intervals", {})
        origin = origins.get(journey["day"])
        calls = []
        stop = observed = scheduled = sequence = 0
        for n, row in enumerate(journey["calls"]):
            stop += row[0]
            observed += row[1]
            scheduled += row[2]
            sequence += row[4]
            step = row[5] if len(row) > 5 else None
            tag = row[6] if len(row) > 6 and row[6] is not None else journey["call_tag"]
            key = str(n)
            if key in epochs:
                observed_epoch, scheduled_epoch = epochs[key]
            else:
                observed_epoch, scheduled_epoch = origin + observed, origin + scheduled
            if key in intervals:
                interval = intervals[key]
            elif step is None:
                interval = None
            else:
                interval = [observed_epoch, None if step == -1 else observed_epoch + step]
            quality_flags, match = tables["call_tags"][tag]
            calls.append([stop, observed, scheduled, row[3], sequence, observed_epoch,
                          scheduled_epoch, interval, list(quality_flags), match])
        out = {k: v for k, v in journey.items() if k not in _JOURNEY_KEYS}
        out["calls"] = calls
        if "data_version" in journey:
            out["data_version"] = tables["data_versions"][journey["data_version"]]
        if "source_files" in journey:
            out["source_files"] = [tables["source_files"][i] for i in journey["source_files"]]
        if journey.get("route_pattern") is not None:
            out["route_pattern"] = tables["route_patterns"][journey["route_pattern"]]
        journeys.append(out)
    return {**{k: v for k, v in doc.items() if k not in _DOCUMENT_KEYS}, "journeys": journeys}
