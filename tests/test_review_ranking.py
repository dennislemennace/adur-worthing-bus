import os,sys,csv,io,zipfile,tempfile
from pathlib import Path
from datetime import date
sys.path[:0]=['.','scripts']
import build_timetable as bt,json_to_sqlite as j2s
from api.timetable_db import Timetable

def table(header,rows):
 f=io.StringIO();w=csv.writer(f);w.writerow(header.split(','));w.writerows(rows);return f.getvalue()

def test_fastest_candidate_survives_presentation_limit(tmp_path, monkeypatch):
    monkeypatch.setenv("SKIP_OSRM", "1")
    p=tmp_path;feed=p/'feed.zip'
    routes=[('R0','SC','1','first',3)]+[(f'R{i}','BH' if i==1 else 'SC',str(i+1),'second',3) for i in range(1,6)]
    trips=[(f'T{i}',f'R{i}','ALL','End','') for i in range(6)]
    calls=[('T0',1,'4400A','12:00:00','12:00:00'),('T0',2,'4400X','12:10:00','12:10:00')]
    for i,arr in enumerate([50,55,56,57,58],1):calls +=[(f'T{i}',1,'4400X','12:15:00','12:15:00'),(f'T{i}',2,'4400B',f'12:{arr}:00',f'12:{arr}:00')]
    with zipfile.ZipFile(feed,'w') as z:
     for name,head,rows in [
     ('agency','agency_id,agency_name,agency_noc',[('SC','Stagecoach','SCSO'),('BH','Brighton','BHBC')]),
     ('stops','stop_id,stop_name,stop_lat,stop_lon',[('4400A','Start',50.83,-.36),('4400X','Change',50.83,-.32),('4400B','End',50.83,-.28)]),
     ('routes','route_id,agency_id,route_short_name,route_long_name,route_type',routes),
     ('trips','trip_id,route_id,service_id,trip_headsign,shape_id',trips),
     ('stop_times','trip_id,stop_sequence,stop_id,arrival_time,departure_time',calls),
     ('calendar','service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date',[('ALL',1,1,1,1,1,1,1,'20260101','20271231')])]:z.writestr(name+'.txt',table(head,rows))
    j2s.convert(bt.parse_gtfs(str(feed)),p/'timetable.sqlite');t=Timetable(p/'timetable.sqlite',allow_fetch=False)
    options = t.interchange_options("4400A", "4400B", date(2026,9,22), 43200, 4)
    assert min(x["total_minutes"] for x in options) == 50


def test_departure_identity_keeps_companies_with_the_same_service_number(monkeypatch):
    from types import SimpleNamespace
    from api import main
    from test_coach_services import _board
    from datetime import datetime
    from zoneinfo import ZoneInfo
    tt = SimpleNamespace(
        stops={"4400A": {"name": "A"}},
        trips={"SC": {"route_id": "SC", "service_id": "ALL", "headsign": "Town Centre"},
               "BH": {"route_id": "BH", "service_id": "ALL", "headsign": "Town Centre"}},
        routes={"SC": {"short_name": "1"}, "BH": {"short_name": "1"}},
        stop_times_for=lambda s: [(43800, "SC"), (43800, "BH")],
        trip_stops_for=lambda t: [(43800, "4400A"), (44400, "4400B")],
        runs_on=lambda s, d: True,
        noc_for_route=lambda r: {"SC": "SCSO", "BH": "BHBC"}[r],
    )
    when = datetime(2026, 9, 22, 12, 0, tzinfo=ZoneInfo("Europe/London"))
    board = _board(tt, "4400A", monkeypatch, when)
    assert len(board["departures"]) == 2
    assert {d["operator"] for d in board["departures"]} == {"SCSO", "BHBC"}
