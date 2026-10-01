"""Failure cases from the September 30 review; all upstreams are local fakes."""
import asyncio
from datetime import date, datetime
import json
from pathlib import Path
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / 'scripts')]
import api.main as main
from api import journey_planner as planner, live_eta
from api.trip_match import LONDON
from test_live_eta import FakeTimetable, declared
from test_build_fares import PER_PAIR, _zip
import build_fares
from process_snapshots import _resolve_vehicle_journeys


def test_replay_snapshot_is_self_contained(tmp_path):
    from replay_inputs import snapshot, restore
    data = snapshot()
    restore(data, tmp_path)
    assert (tmp_path / 'data/analysis_exclusions.json').is_file()
    for builder in ('build_journey_times.py', 'build_delay_hotspots.py'):
        result = subprocess.run([sys.executable, str(tmp_path / 'scripts' / builder), '--help'],
                                cwd=tmp_path, capture_output=True, text=True)
        assert result.returncode == 0, result.stderr
    data['files']['data/analysis_exclusions.json']['text'] += ' '
    with pytest.raises(ValueError, match='hash'):
        restore(data, tmp_path / 'bad')


def test_declared_trips_with_disjoint_actual_tracks_both_survive():
    a, b = ('late', date(2026, 9, 24)), ('on-time', date(2026, 9, 24))
    tracks = {a: [(36000+i*60,50,-.2,'bus') for i in range(5)],
              b: [(33000+i*60,50,-.2,'bus') for i in range(10)]}
    assert _resolve_vehicle_journeys(tracks, {a:(32400,33600),b:(33000,34200)}, {a,b}) == tracks


@pytest.mark.parametrize('day,start,hour,fold', [(25, 3000, 1, 0), (25, 3900, 1, 1)])
def test_lateness_is_elapsed_time_through_repeated_hour(day, start, hour, fold):
    tt = FakeTimetable(start=start, n=3)
    rows = live_eta.project_trip(tt, 'VJ_1400', declared(1200),
                                datetime(2026,10,day,hour,55,tzinfo=LONDON,fold=fold))
    row = next(r for r in rows if r['stop_id'] == 'STOP2')
    actual = datetime.fromisoformat(row['expected']).timestamp() - datetime.fromisoformat(row['scheduled']).timestamp()
    assert actual == 1200


@pytest.mark.parametrize('xml', [PER_PAIR.replace('2026-09-04', '2999-01-01'),
    PER_PAIR.replace('<FromDate>2026-09-04T00:00:00</FromDate>',
       '<ValidBetween><FromDate>2000-01-01T00:00:00Z</FromDate><ToDate>2000-01-31T23:59:59Z</ToDate></ValidBetween>')], ids=['future', 'expired'])
def test_inactive_fares_are_not_published(xml):
    result = build_fares.build({'SCSO': (2, _zip({'17_AdultSingle.xml': xml}))}, {'149000007830'})
    assert result['tables'] == []


class FakeClient:
    calls = []
    status = 200
    def __init__(self, **kwargs): pass
    async def __aenter__(self): return self
    async def __aexit__(self, *args): pass
    async def get(self, *args, **kwargs):
        self.calls.append(kwargs.get('params'))
        await asyncio.sleep(.01)
        class R:
            status_code = FakeClient.status
            def raise_for_status(self):
                if self.status_code >= 400: raise RuntimeError('HTTP error')
            def json(self): return {'options': []}
        return R()


@pytest.fixture
def planning(monkeypatch, tmp_path):
    main._cache.clear()
    FakeClient.calls, FakeClient.status = [], 200
    monkeypatch.setattr(main.httpx, 'AsyncClient', FakeClient)
    monkeypatch.setattr(planner, 'BAT_API_KEY', 'dummy')
    monkeypatch.setattr(main, '_plan_quota', planner.DailyQuota(tmp_path/'quota.json', 10))
    return dict(from_lat=50.83, from_lon=-.32, to_lat=50.822, to_lon=-.137,
                when='14:00', day='2026-09-30')


def test_simultaneous_identical_plans_share_one_request(planning):
    async def run():
        await asyncio.gather(*(main.get_plan(**planning) for _ in range(5)))
    asyncio.run(run())
    assert len(FakeClient.calls) == 1


def test_http_failure_does_not_refund_attempted_request(planning):
    FakeClient.status = 400
    asyncio.run(main.get_plan(**planning))
    assert main._plan_quota.remaining() == 9


def test_now_never_requests_an_earlier_five_minute_slot(planning, monkeypatch):
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None): return datetime(2026,9,30,14,4,59,tzinfo=LONDON)
    monkeypatch.setattr(main, 'datetime', Clock)
    asyncio.run(main.get_plan(**{**planning,'when':None,'day':None}))
    assert FakeClient.calls[0]['time'] == '14:05'


def test_scheduled_operator_cannot_vanish_behind_another_operators_data():
    import check_published
    coverage = {'cohorts': [
        {'operator':'SCSO','scheduled_in_recording_span':170,'journeys_with_measured_calls':0},
        {'operator':'BHBC','scheduled_in_recording_span':180,'journeys_with_measured_calls':160}]}
    assert check_published.coverage_gaps(coverage) == ['SCSO']


def test_transportapi_burst_and_failures_count_attempts(monkeypatch):
    main._cache.clear()
    monkeypatch.setattr(main, 'NEXTBUSES_APP_ID', 'dummy')
    monkeypatch.setattr(main, 'NEXTBUSES_APP_KEY', 'dummy')
    monkeypatch.setattr(main, '_nb_quota', {'date': date.today().isoformat(), 'count': 0})
    monkeypatch.setattr(main, '_nb_quota_save', lambda: None)
    calls = []
    async def fail(stop):
        calls.append(stop)
        await asyncio.sleep(.01)
        return None
    monkeypatch.setattr(main, '_fetch_nextbuses', fail)
    async def burst():
        await asyncio.gather(*(main._apply_live_overlay({'departures': []}, 'A') for _ in range(5)))
        await main._apply_live_overlay({'departures': []}, 'A')
    asyncio.run(burst())
    assert len(calls) == 1
    assert main._nb_quota['count'] == 1
