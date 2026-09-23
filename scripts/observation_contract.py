"""Stable time and source identities shared by observation consumers.

GTFS time is elapsed time from local noon minus twelve hours, including DST:
https://gtfs.org/documentation/schedule/reference/#field-types
"""
import hashlib
from datetime import date, datetime, time
from zoneinfo import ZoneInfo

LONDON = ZoneInfo("Europe/London")
TIME_BASIS = "gtfs_service_day_elapsed_seconds"


def service_origin(day):
    day = date.fromisoformat(day) if isinstance(day, str) else day
    return int(datetime.combine(day, time(12), LONDON).timestamp()) - 43200


def digest_file(path):
    with open(path, "rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def canonical_row(row, doc, source_hash):
    """Normalize old folder-relative seconds without certifying old matching.

    Legacy wall-clock values on a DST transition cannot recover the original
    instant unambiguously. Keep the evidence, but quarantine those rows.
    """
    row = dict(row)
    row.update(source_file_sha256=source_hash,
               data_version=doc.get("data_version", "unknown"),
               method_version=doc.get("method_version", "unknown"))
    if doc.get("time_basis") != TIME_BASIS:
        source_day, day = doc.get("day"), row.get("day")
        if source_day and day and source_day != "?":
            source_date, service_date = date.fromisoformat(source_day), date.fromisoformat(day)
            offset = (source_date - service_date).days * 86400
            for field in ("scheduled_secs", "observed_secs", "journey_start_secs"):
                if row.get(field) is not None:
                    row[field] += offset
            midnight = int(datetime.combine(source_date, time(), LONDON).timestamp())
            if midnight != service_origin(source_date):
                row["quality_flags"] = ["legacy_dst_time_ambiguous"]
        row.setdefault("quality_flags", []).append("legacy_matching_unverified")
    row["time_basis"] = TIME_BASIS
    if row.get("day") and "legacy_dst_time_ambiguous" not in row.get("quality_flags", []):
        origin = service_origin(row["day"])
        for field in ("scheduled", "observed"):
            if row.get(field + "_secs") is not None:
                row.setdefault(field + "_epoch", origin + row[field + "_secs"])
    return row


def row_identity(row):
    return (row.get("day"), row.get("operator"), row.get("trip_id"),
            row.get("data_version"), row.get("method_version"))
