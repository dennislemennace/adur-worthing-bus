"""Published disruptions, from BODS's SIRI-SX feed, for the routes and stops we show.

Operators and councils publish diversions, closures and stop suspensions to the
Bus Open Data Service in SIRI Situation Exchange. Each situation says what it
affects: whole operators, particular lines (by operator code and line name), or
particular stops (by ATCO code). This module reads that feed and keeps the
situations that touch this site's area, so a notice can sit beside the rows it
affects instead of on a page nobody visits.

It is a published claim by whoever wrote it, and is shown as one: with its own
wording, its validity dates and who published it, never restated as our own
finding.

The feed is national. It is streamed element by element and each situation is
discarded as soon as it is known not to touch us, so a large file never has to
sit in memory as a tree.
"""

import io
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional

# Situations that start within this long are shown as coming up, so a closure
# tomorrow morning is on the board tonight.
UPCOMING_WINDOW = timedelta(hours=36)

# Coaches and airport shuttles are not what anyone at a local stop is waiting
# for; an operator-wide notice from National Express would sit on every board
# a coach happens to serve. (The same list as trip_match.COACH_NOCS.)
COACH_NOCS = frozenset({"NATX", "FLIX", "OXBC", "GHOP", "BMCS", "UNTM"})


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _text(el, name: str) -> str:
    """First descendant called `name` (any namespace), as stripped text."""
    for child in el.iter():
        if _local(child.tag) == name and child.text and child.text.strip():
            return " ".join(child.text.split())
    return ""


def _children(el, name: str):
    return [c for c in el.iter() if _local(c.tag) == name]


def _parse_time(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def situation_from_element(el) -> Optional[dict]:
    """One PtSituationElement as a plain dict, or None if it has no number."""
    number = _text(el, "SituationNumber")
    if not number:
        return None
    periods = []
    for vp in _children(el, "ValidityPeriod"):
        periods.append((_parse_time(_text(vp, "StartTime")), _parse_time(_text(vp, "EndTime"))))
    pubs = [(_parse_time(_text(p, "StartTime")), _parse_time(_text(p, "EndTime")))
            for p in _children(el, "PublicationWindow")]

    lines, stops, operators = [], [], []
    for consequence in _children(el, "Consequence"):
        for line in _children(consequence, "AffectedLine"):
            lines.append({
                "operator": _text(line, "OperatorRef"),
                "line": _text(line, "PublishedLineName") or _text(line, "LineRef"),
                "line_ref": _text(line, "LineRef"),
            })
        for stop in _children(consequence, "AffectedStopPoint"):
            ref = _text(stop, "StopPointRef")
            if ref:
                stops.append(ref)
        # Operator-wide: an AffectedOperator that is not inside a line, with
        # AllLines on the network (or no lines named at all).
        line_ops = {id(o) for line in _children(consequence, "AffectedLine")
                    for o in _children(line, "AffectedOperator")}
        all_lines = bool(_children(consequence, "AllLines"))
        for op in _children(consequence, "AffectedOperator"):
            if id(op) in line_ops:
                continue
            ref = _text(op, "OperatorRef")
            if ref and (all_lines or not _children(consequence, "AffectedLine")):
                operators.append({"operator": ref, "name": _text(op, "OperatorName")})

    reason = ""
    for child in el:
        if _local(child.tag).endswith("Reason") and child.text and child.text.strip():
            reason = child.text.strip()
            break
    advice = " ".join(_text(a, "Details") for a in _children(el, "Advice") if _text(a, "Details"))
    severity = _text(el, "Severity")
    link = ""
    for info in _children(el, "InfoLink"):
        uri = _text(info, "Uri")
        if uri.startswith("https://") or uri.startswith("http://"):
            link = uri
            break
    return {
        "id": number,
        "progress": _text(el, "Progress") or "open",
        "periods": periods,
        "publication": pubs,
        "planned": _text(el, "Planned").lower() == "true",
        "reason": reason,
        "severity": severity,
        "summary": _text(el, "Summary"),
        "description": _text(el, "Description"),
        "advice": advice,
        "link": link,
        "publisher": _text(el, "ParticipantRef"),
        "lines": lines,
        "stops": stops,
        "operators": operators,
    }


def _xml_stream(payload: bytes):
    """The feed as a stream, whether BODS sent XML or a zip holding it."""
    if payload[:2] == b"PK":
        with zipfile.ZipFile(io.BytesIO(payload)) as zf:
            name = next(n for n in zf.namelist() if n.lower().endswith(".xml"))
            return io.BytesIO(zf.read(name))
    return io.BytesIO(payload)


def parse_feed(payload: bytes, keep=None) -> list:
    """Every situation in a SIRI-SX document that `keep` accepts (all if None)."""
    out = []
    for _event, el in ET.iterparse(_xml_stream(payload), events=("end",)):
        if _local(el.tag) != "PtSituationElement":
            continue
        sit = situation_from_element(el)
        el.clear()
        if sit and (keep is None or keep(sit)):
            out.append(sit)
    return out


def service_key(name: str) -> str:
    """Line names compared as a reader would: case, spaces and a night N aside."""
    k = "".join(str(name or "").split()).upper()
    return k[1:] if len(k) > 1 and k[0] == "N" and k[1:].isdigit() else k


def area_filter(stop_ids: Iterable[str], routes: Iterable[tuple]):
    """A `keep` for parse_feed: situations touching our stops, lines or operators.

    `routes` is `(operator NOC, line name)` for every route we hold. An
    operator-wide situation is kept only for an operator that runs one of them,
    and never for a coach operator.
    """
    stops = set(stop_ids)
    lines = {(noc, service_key(name)) for noc, name in routes}
    nocs = {noc for noc, _ in lines} - COACH_NOCS

    def keep(sit: dict) -> bool:
        if sit["progress"].lower() == "closed":
            return False
        if any(s in stops for s in sit["stops"]):
            return True
        if any((l["operator"], service_key(l["line"])) in lines
               or (l["operator"], service_key(l["line_ref"])) in lines
               for l in sit["lines"]):
            return True
        return any(o["operator"] in nocs for o in sit["operators"])
    return keep


def current(situations: list, now: datetime) -> list:
    """Situations in force now or starting soon, as the API publishes them.

    Trimmed to what a reader needs and made JSON-safe: times as ISO strings,
    `starts` set only when the situation has not begun yet.
    """
    out = []
    for sit in situations:
        if sit["publication"] and not any(
                (s is None or s <= now) and (e is None or e >= now) for s, e in sit["publication"]):
            continue
        live = [(s, e) for s, e in sit["periods"] if (s is None or s <= now) and (e is None or e >= now)]
        soon = sorted((s, e) for s, e in sit["periods"]
                      if s is not None and now < s <= now + UPCOMING_WINDOW)
        if sit["periods"] and not live and not soon:
            continue
        period = live[0] if live else (soon[0] if soon else (None, None))
        start, end = period
        out.append({
            "id": sit["id"],
            "summary": sit["summary"] or sit["description"][:120],
            "description": sit["description"],
            "advice": sit["advice"],
            "reason": sit["reason"],
            "severity": sit["severity"],
            "planned": sit["planned"],
            "starts": start.isoformat() if (start and not live) else None,
            "ends": end.isoformat() if end else None,
            "link": sit["link"],
            "publisher": sit["publisher"],
            "lines": [{"operator": l["operator"], "line": l["line"] or l["line_ref"]}
                      for l in sit["lines"]],
            "stops": sit["stops"],
            "operators": sit["operators"],
        })
    return out


def affecting(disruptions: list, *, stop_id: str = "", services: Iterable[tuple] = ()) -> list:
    """The disruptions that touch one stop, or any of `(operator, line)` services."""
    wanted = {(op, service_key(line)) for op, line in services}
    wanted_ops = {op for op, _ in wanted}
    out = []
    for d in disruptions:
        if stop_id and stop_id in d["stops"]:
            out.append(d)
        elif any((l["operator"], service_key(l["line"])) in wanted for l in d["lines"]):
            out.append(d)
        elif any(o["operator"] in wanted_ops for o in d["operators"]):
            out.append(d)
    return out


def row_matches(d: dict, operator: str, service: str) -> bool:
    """Whether a disruption names this row's line, or all of its operator's."""
    key = service_key(service)
    return (any(l["operator"] == operator and service_key(l["line"]) == key for l in d["lines"])
            or any(o["operator"] == operator for o in d["operators"]))
