"""A small BODS bundle and a small FlixBus timetable, for the FlixBus tests.

Real stop codes and coordinates, so the stop matching meets the distances it
meets in the weekly build: FlixBus's Brighton stop sits exactly on BODS's coach
stop, Preston Park 8 m off, and Gatwick North 132 m from the bay BODS's own
copy of FlixBus uses, with a local bus stop 20 m away that it must not take.

FlixBus writes its times in UTC (its agency_timezone, and it means it), so the
same 07:00 from Brighton is 07:00 in this file in winter and 06:00 in summer.
The clock changes on 28 March 2027 at 01:00 UTC.
"""

import csv
import io
import zipfile

# ── BODS ─────────────────────────────────────────────────────

BODS_STOPS = [
    ("4400SH01",     "Shoreham High Street", 50.8320, -0.2750),
    ("9000F22A3795", "Brighton Railway Station - Stroudley Road", 50.832152, -0.141385),
    ("900059056",    "Brighton Rail Station Stroudley Road opposite", 50.831907, -0.141065),
    ("9100PRSPBUS",  "Preston Park - London Road", 50.84585073, -0.15154891004),
    ("4400CY0375",   "North Terminal Bus Station", 51.160485, -0.176657),
    ("4400CY0388",   "North Terminal Bus Station", 51.159989, -0.1748178),
]

BODS_ROUTES = [
    ("R700",   "AG_SC",   "700",   "Coastliner"),
    ("RUK066", "AG_FLIX", "UK066", ""),
]

# BODS's copy of FlixBus: one coach, published twice, as BODS does.
BODS_TRIPS = [
    ("T_LOCAL", "R700", "Brighton Station",
     [(1, "4400SH01", "08:00:00"), (2, "900059056", "08:45:00")]),
    ("T_BODS_FLIX_A", "RUK066", "Brighton Railway Station - Stroudley Road - Parkside",
     [(1, "9000F22A3795", "07:00:00"), (2, "9100PRSPBUS", "07:05:00"),
      (3, "4400CY0375", "07:50:00")]),
    ("T_BODS_FLIX_B", "RUK066", "Brighton Railway Station - Stroudley Road - Parkside",
     [(1, "9000F22A3795", "07:00:00"), (2, "9100PRSPBUS", "07:05:00"),
      (3, "4400CY0375", "07:50:00")]),
]

# ── FlixBus ──────────────────────────────────────────────────

SRBG = "f22a3795-754c-4126-9e53-dcbe616152a2"
PRESTON = "515e527c-b799-4422-ba68-cd461eb3d59d"
GATWICK_N = "540d384c-569c-4c00-91c0-9e58ac4a3dc8"
VICTORIA = "dcc15c7e-0000-4000-8000-000000000001"
SHEFFIELD = "dcc15c7e-0000-4000-8000-000000000002"
FIELD = "dcc15c7e-0000-4000-8000-000000000003"     # in Sussex, near no stop

FLIX_STOPS = [
    (SRBG, "Brighton Railway Station - Stroudley Road", 50.832152, -0.141385),
    (PRESTON, "Brighton (Preston Park)", 50.845783, -0.151586),
    (GATWICK_N, "Gatwick Airport (North Terminal)", 51.15987, -0.175036),
    (VICTORIA, "London Victoria Coach Station", 51.4953, -0.1473),
    (SHEFFIELD, "Sheffield Interchange", 53.3800, -1.4650),
    (FIELD, "Somewhere in a field", 50.9500, -0.3500),
]

FLIX_ROUTES = [
    ("UK066", "FlixBus UK066", "London - Brighton"),
    ("UK940", "FlixBus UK940", "Gatwick - Sheffield"),
    ("UK999", "FlixBus UK999", "London - Sheffield"),
]

T_WINTER = "UK066-1-0700102026-SRBG#VCS-00"
T_SUMMER = "UK066-1-0600032027-SRBG#VCS-00"
T_SPANS = "UK066-2-0600032027-SRBG#VCS-00"
T_LATE = "UK066-3-2330032027-SRBG#VCS-00"
T_INBOUND = "UK066-4-0500102026-VCS#SRBG-00"
T_GATWICK = "UK940-1-0800102026-LGWN#SHF-00"
T_ELSEWHERE = "UK999-1-0900102026-VCS#SHF-00"

# (trip, route, service, shape, [(seq, stop, departure UTC), ...])
FLIX_TRIPS = [
    (T_WINTER, "UK066", "S_WINTER", "SH_BRIGHTON",
     [(1, SRBG, "07:00:00"), (2, PRESTON, "07:05:00"), (3, GATWICK_N, "07:50:00"),
      (4, VICTORIA, "09:00:00")]),
    (T_SUMMER, "UK066", "S_SUMMER", "SH_BRIGHTON",
     [(1, SRBG, "06:00:00"), (2, PRESTON, "06:05:00"), (3, VICTORIA, "08:00:00")]),
    # Runs either side of the clock change, which FlixBus itself avoids: its
    # 07:00 local has a different UTC time, so a different trip, each side.
    (T_SPANS, "UK066", "S_SPANS", "SH_BRIGHTON",
     [(1, SRBG, "06:00:00"), (2, PRESTON, "06:05:00"), (3, VICTORIA, "08:00:00")]),
    # 23:30 UTC in summer is 00:30 the next morning in Brighton.
    (T_LATE, "UK066", "S_SUMMER", "SH_BRIGHTON",
     [(1, SRBG, "23:30:00"), (2, PRESTON, "23:35:00"), (3, VICTORIA, "25:30:00")]),
    (T_INBOUND, "UK066", "S_WINTER", "SH_BRIGHTON",
     [(1, VICTORIA, "05:00:00"), (2, GATWICK_N, "06:10:00"), (3, PRESTON, "06:55:00"),
      (4, SRBG, "07:05:00")]),
    (T_GATWICK, "UK940", "S_WINTER", "SH_NORTH",
     [(1, GATWICK_N, "08:00:00"), (2, SHEFFIELD, "12:00:00")]),
    (T_ELSEWHERE, "UK999", "S_WINTER", "",
     [(1, VICTORIA, "09:00:00"), (2, SHEFFIELD, "13:00:00")]),
]

FLIX_CALENDAR = [
    ("S_WINTER", 1, 1, 1, 1, 1, 1, 1, "20261025", "20270327"),
    ("S_SUMMER", 1, 1, 1, 1, 1, 1, 1, "20270328", "20270425"),
    ("S_SPANS",  1, 1, 1, 1, 1, 1, 1, "20270320", "20270403"),
]
FLIX_CALENDAR_DATES = [("S_WINTER", "20261225", 2)]

FLIX_SHAPES = [
    ("SH_BRIGHTON", 50.832152, -0.141385, 0),
    ("SH_BRIGHTON", 50.9000, -0.1500, 1),
    ("SH_BRIGHTON", 51.4953, -0.1473, 2),
    # Gatwick to Sheffield never comes near the coast.
    ("SH_NORTH", 51.15987, -0.175036, 0),
    ("SH_NORTH", 53.3800, -1.4650, 1),
]


def csv_text(rows, header):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(header)
    w.writerows(rows)
    return buf.getvalue()


def write_bods_zip(path):
    with zipfile.ZipFile(path, "w") as z:
        z.writestr("agency.txt", csv_text(
            [("AG_SC", "Stagecoach South", "SCSO"), ("AG_FLIX", "FlixBus", "FLIX")],
            ["agency_id", "agency_name", "agency_noc"]))
        z.writestr("stops.txt", csv_text(
            BODS_STOPS, ["stop_id", "stop_name", "stop_lat", "stop_lon"]))
        z.writestr("routes.txt", csv_text(
            [(r, a, s, l, 3) for r, a, s, l in BODS_ROUTES],
            ["route_id", "agency_id", "route_short_name", "route_long_name", "route_type"]))
        z.writestr("trips.txt", csv_text(
            [(t, r, "ALL", h, "") for t, r, h, _ in BODS_TRIPS],
            ["trip_id", "route_id", "service_id", "trip_headsign", "shape_id"]))
        z.writestr("stop_times.txt", csv_text(
            [(t, seq, sid, hms, hms) for t, _, _, calls in BODS_TRIPS for seq, sid, hms in calls],
            ["trip_id", "stop_sequence", "stop_id", "arrival_time", "departure_time"]))
        z.writestr("calendar.txt", csv_text(
            [("ALL", 1, 1, 1, 1, 1, 1, 1, "20260101", "20271231")],
            ["service_id", "monday", "tuesday", "wednesday", "thursday",
             "friday", "saturday", "sunday", "start_date", "end_date"]))


def write_flixbus_zip(path, stops=FLIX_STOPS, trips=FLIX_TRIPS):
    """FlixBus's own layout: UTC agency, empty headsigns, a service per trip."""
    with zipfile.ZipFile(path, "w") as z:
        z.writestr("agency.txt", csv_text(
            [("FLIXBUS-eu", "FlixBus-eu", "https://global.flixbus.com", "UTC", "en")],
            ["agency_id", "agency_name", "agency_url", "agency_timezone", "agency_lang"]))
        z.writestr("stops.txt", csv_text(
            [(sid, name, lat, lon, "Europe/London") for sid, name, lat, lon in stops],
            ["stop_id", "stop_name", "stop_lat", "stop_lon", "stop_timezone"]))
        z.writestr("routes.txt", csv_text(
            [("FLIXBUS-eu", rid, short, long_, 3, "73D700") for rid, short, long_ in FLIX_ROUTES],
            ["agency_id", "route_id", "route_short_name", "route_long_name",
             "route_type", "route_color"]))
        z.writestr("trips.txt", csv_text(
            [(r, t, s, "", sh) for t, r, s, sh, _ in trips],
            ["route_id", "trip_id", "service_id", "trip_headsign", "shape_id"]))
        z.writestr("stop_times.txt", csv_text(
            [(t, sid, hms, hms, "", seq, 0, 0)
             for t, _, _, _, calls in trips for seq, sid, hms in calls],
            ["trip_id", "stop_id", "arrival_time", "departure_time", "timepoint",
             "stop_sequence", "pickup_type", "drop_off_type"]))
        z.writestr("calendar.txt", csv_text(
            FLIX_CALENDAR,
            ["service_id", "monday", "tuesday", "wednesday", "thursday",
             "friday", "saturday", "sunday", "start_date", "end_date"]))
        z.writestr("calendar_dates.txt", csv_text(
            FLIX_CALENDAR_DATES, ["service_id", "date", "exception_type"]))
        z.writestr("shapes.txt", csv_text(
            FLIX_SHAPES, ["shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"]))
