"""common.geo, common.tracker, common.terminals e common.timeutils (contrato 4.1)."""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone

import pytest

from common.geo import SpeedStatus, compute_speed_kmh, haversine_m, is_valid_coordinate
from common.terminals import TERMINALS, is_near_terminal, nearest_terminal
from common.timeutils import floor_to_window, parse_ts, to_iso, window_bounds
from common.tracker import SpeedTracker
from tests.conftest import BASE_TS, FREE_LAT, FREE_LON, LAPA_LAT, LAPA_LON, make_position

SE = (-23.5505, -46.6340)                  # Praca da Se
PAULISTA = (-23.5679, -46.6478)            # Av. Paulista x Brigadeiro Luis Antonio
T0 = BASE_TS
# 0.009 graus de latitude ~ 1000,75 m
ONE_KM_LAT = 1000.0 / (math.pi / 180 * 6_371_008.8)


# --------------------------------------------------------------------------- haversine


def test_haversine_se_to_paulista_is_about_2_4_km():
    assert 2200 < haversine_m(*SE, *PAULISTA) < 2600


def test_haversine_same_point_is_zero():
    assert haversine_m(*SE, *SE) == 0.0


def test_haversine_is_symmetric():
    assert haversine_m(*SE, *PAULISTA) == pytest.approx(haversine_m(*PAULISTA, *SE))


def test_haversine_one_degree_of_latitude():
    assert haversine_m(0, 0, 1, 0) == pytest.approx(111_195.08, abs=1)


def test_haversine_antipodal_points_do_not_fail():
    # min(1.0, sqrt(a)) protege contra erro de arredondamento > 1
    assert haversine_m(0, 0, 0, 180) == pytest.approx(math.pi * 6_371_008.8)


# --------------------------------------------------------------------------- coordenadas


@pytest.mark.parametrize(
    "lat, lon, expected",
    [
        (-23.55, -46.63, True),
        ("-23.55", "-46.63", True),     # string numerica e aceita
        (0.0, 0.0, False),               # coordenada zerada
        (0.0, -46.63, False),
        (-23.55, 0.0, False),
        (None, -46.63, False),
        (-23.55, None, False),
        ("abc", -46.63, False),
        (float("nan"), -46.63, False),
        (-22.90, -43.20, False),         # Rio de Janeiro: fora da Grande SP
        (-24.10, -47.10, True),          # canto da caixa (inclusivo)
        (-23.30, -46.20, True),
        (-24.11, -46.63, False),
        (-23.55, -46.19, False),
    ],
)
def test_is_valid_coordinate(lat, lon, expected):
    assert is_valid_coordinate(lat, lon) is expected


# --------------------------------------------------------------------------- velocidade


def _speed(dlat: float = ONE_KM_LAT, seconds: float = 60.0, lat1: float = FREE_LAT, lon1: float = FREE_LON, **kw):
    return compute_speed_kmh(lat1, lon1, T0, lat1 + dlat, lon1, T0 + timedelta(seconds=seconds), **kw)


def test_speed_ok_one_km_per_minute_is_60_kmh():
    result = _speed()
    assert result.status is SpeedStatus.OK and result.ok
    assert result.speed_kmh == pytest.approx(60.0, rel=1e-6)
    assert result.distance_m == pytest.approx(1000.0, rel=1e-6)
    assert result.interval_s == 60.0


def test_speed_stopped_is_zero():
    result = _speed(dlat=0.0, seconds=30)
    assert result.ok and result.speed_kmh == 0.0


@pytest.mark.parametrize("lat1, lon1", [(0.0, 0.0), (-22.90, -43.20)])
def test_speed_invalid_coordinate_first_point(lat1, lon1):
    result = compute_speed_kmh(lat1, lon1, T0, FREE_LAT, FREE_LON, T0 + timedelta(seconds=30))
    assert result.status is SpeedStatus.INVALID_COORDINATE
    assert result.speed_kmh is None and not result.ok


def test_speed_invalid_coordinate_second_point():
    result = compute_speed_kmh(FREE_LAT, FREE_LON, T0, 0, 0, T0 + timedelta(seconds=30))
    assert result.status is SpeedStatus.INVALID_COORDINATE


@pytest.mark.parametrize("seconds", [0.0, -30.0, 0.5])
def test_speed_interval_too_short_includes_zero_and_negative(seconds):
    result = _speed(seconds=seconds)
    assert result.status is SpeedStatus.INTERVAL_TOO_SHORT
    assert result.speed_kmh is None and result.interval_s == seconds


def test_speed_interval_equal_to_min_is_accepted():
    assert _speed(dlat=0.0001, seconds=1.0).ok


def test_speed_interval_too_long():
    result = _speed(seconds=601)
    assert result.status is SpeedStatus.INTERVAL_TOO_LONG and result.speed_kmh is None


def test_speed_interval_equal_to_max_is_accepted():
    assert _speed(seconds=600).ok


def test_speed_too_high_keeps_the_measured_value():
    result = _speed(dlat=ONE_KM_LAT * 2, seconds=60)  # 120 km/h
    assert result.status is SpeedStatus.SPEED_TOO_HIGH
    assert result.speed_kmh == pytest.approx(120.0, rel=1e-6)
    assert not result.ok


def test_speed_custom_limits():
    assert _speed(max_speed_kmh=50).status is SpeedStatus.SPEED_TOO_HIGH
    assert _speed(seconds=60, min_interval_s=61).status is SpeedStatus.INTERVAL_TOO_SHORT
    assert _speed(seconds=60, max_interval_s=59).status is SpeedStatus.INTERVAL_TOO_LONG


# --------------------------------------------------------------------------- SpeedTracker


def test_tracker_first_reading_has_no_speed_and_becomes_state():
    tracker = SpeedTracker()
    reading = tracker.update(make_position(t=0))
    assert reading.previous is None and reading.speed is None and reading.speed_kmh is None
    assert reading.valid_coordinate
    assert tracker.last("11433").event_ts == T0 and len(tracker) == 1


def test_tracker_computes_speed_from_previous_valid_reading():
    tracker = SpeedTracker()
    tracker.update(make_position(t=0))
    reading = tracker.update(make_position(t=60, lat=FREE_LAT + ONE_KM_LAT))
    assert reading.speed_kmh == pytest.approx(60.0, rel=1e-6)
    assert reading.previous.event_ts == T0


def test_tracker_invalid_coordinate_does_not_update_state():
    tracker = SpeedTracker()
    tracker.update(make_position(t=0))
    bad = tracker.update(make_position(t=30, lat=0.0, lon=0.0))
    assert bad.speed.status is SpeedStatus.INVALID_COORDINATE and not bad.valid_coordinate
    assert bad.speed_kmh is None
    assert tracker.last("11433").event_ts == T0
    # a proxima leitura valida usa a anterior valida (intervalo de 60 s)
    nxt = tracker.update(make_position(t=60))
    assert nxt.speed.interval_s == 60.0


def test_tracker_invalid_first_reading_is_not_stored():
    tracker = SpeedTracker()
    reading = tracker.update(make_position(t=0, lat=0.0, lon=0.0))
    assert reading.speed.status is SpeedStatus.INVALID_COORDINATE and len(tracker) == 0


@pytest.mark.parametrize("t", [0, -30])
def test_tracker_repeated_or_older_reading_does_not_update_state(t):
    tracker = SpeedTracker()
    tracker.update(make_position(t=0))
    reading = tracker.update(make_position(t=t, lat=FREE_LAT + 0.01))
    assert reading.speed.status is SpeedStatus.INTERVAL_TOO_SHORT
    assert tracker.last("11433").lat == FREE_LAT


def test_tracker_short_positive_interval_updates_state():
    tracker = SpeedTracker(min_interval_s=5)
    tracker.update(make_position(t=0))
    reading = tracker.update(make_position(t=2))
    assert reading.speed.status is SpeedStatus.INTERVAL_TOO_SHORT
    assert tracker.last("11433").event_ts == T0 + timedelta(seconds=2)


def test_tracker_long_interval_discards_speed_but_updates_state():
    tracker = SpeedTracker()
    tracker.update(make_position(t=0))
    reading = tracker.update(make_position(t=900))
    assert reading.speed.status is SpeedStatus.INTERVAL_TOO_LONG and reading.speed_kmh is None
    assert tracker.last("11433").event_ts == T0 + timedelta(seconds=900)


def test_tracker_gps_jump_discards_speed_but_updates_state():
    tracker = SpeedTracker()
    tracker.update(make_position(t=0))
    reading = tracker.update(make_position(t=30, lat=FREE_LAT + 0.05))
    assert reading.speed.status is SpeedStatus.SPEED_TOO_HIGH and reading.speed_kmh is None
    assert tracker.last("11433").lat == pytest.approx(FREE_LAT + 0.05)


def test_tracker_line_change_keeps_vehicle_state():
    tracker = SpeedTracker()
    tracker.update(make_position(t=0, line_code=1, direction=1))
    reading = tracker.update(make_position(t=60, line_code=2, direction=2, lat=FREE_LAT + ONE_KM_LAT))
    assert reading.previous is not None and reading.speed_kmh == pytest.approx(60.0, rel=1e-6)


def test_tracker_vehicles_are_independent():
    tracker = SpeedTracker()
    tracker.update(make_position("A", t=0))
    reading = tracker.update(make_position("B", t=30))
    assert reading.previous is None and len(tracker) == 2


# --------------------------------------------------------------------------- terminais


def test_terminal_center_is_near_terminal():
    assert is_near_terminal(LAPA_LAT, LAPA_LON)
    terminal, distance = nearest_terminal(LAPA_LAT, LAPA_LON)
    assert terminal.name == "Lapa" and distance == pytest.approx(0.0)


def test_point_one_km_from_terminal_is_outside_default_radius():
    lat = LAPA_LAT + ONE_KM_LAT
    assert not is_near_terminal(lat, LAPA_LON)
    assert is_near_terminal(lat, LAPA_LON, radius_m=1100)


def test_radius_boundary_is_inclusive():
    lat = LAPA_LAT + ONE_KM_LAT * 0.399
    assert is_near_terminal(lat, LAPA_LON, radius_m=400)
    assert not is_near_terminal(LAPA_LAT + ONE_KM_LAT * 0.401, LAPA_LON, radius_m=400)


def test_free_point_used_by_tests_is_far_from_all_terminals():
    _, distance = nearest_terminal(FREE_LAT, FREE_LON)
    assert distance > 2000


def test_terminals_are_inside_greater_sao_paulo():
    assert len(TERMINALS) >= 30
    assert all(is_valid_coordinate(t.lat, t.lon) for t in TERMINALS)


# --------------------------------------------------------------------------- tempo


@pytest.mark.parametrize(
    "value",
    ["2026-10-14T12:00:00Z", "2026-10-14T09:00:00-03:00", "2026-10-14T12:00:00", " 2026-10-14T12:00:00+00:00 ",
     datetime(2026, 10, 14, 12, 0, 0), datetime(2026, 10, 14, 9, 0, 0, tzinfo=timezone(timedelta(hours=-3)))],
)
def test_parse_ts_normalizes_to_aware_utc(value):
    parsed = parse_ts(value)
    assert parsed == T0 and parsed.tzinfo == timezone.utc


def test_parse_ts_invalid_string_raises_value_error():
    with pytest.raises(ValueError):
        parse_ts("ontem")


def test_to_iso_uses_z_suffix_and_seconds_or_micro():
    assert to_iso(T0) == "2026-10-14T12:00:00Z"
    assert to_iso(T0.replace(microsecond=1500)) == "2026-10-14T12:00:00.001500Z"
    assert to_iso(datetime(2026, 10, 14, 9, 0, tzinfo=timezone(timedelta(hours=-3)))) == "2026-10-14T12:00:00Z"


@pytest.mark.parametrize(
    "ts, minutes, start",
    [
        ("2026-10-14T12:03:10Z", 5, "2026-10-14T12:00:00Z"),
        ("2026-10-14T12:05:00Z", 5, "2026-10-14T12:05:00Z"),
        ("2026-10-14T12:04:59.999Z", 5, "2026-10-14T12:00:00Z"),
        ("2026-10-14T12:59:59Z", 15, "2026-10-14T12:45:00Z"),
        ("2026-10-14T09:07:00-03:00", 5, "2026-10-14T12:05:00Z"),
    ],
)
def test_floor_to_window(ts, minutes, start):
    assert floor_to_window(parse_ts(ts), minutes) == parse_ts(start)


def test_window_bounds():
    start, end = window_bounds(parse_ts("2026-10-14T12:07:00Z"), 5)
    assert (start, end) == (parse_ts("2026-10-14T12:05:00Z"), parse_ts("2026-10-14T12:10:00Z"))
