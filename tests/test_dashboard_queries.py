"""dashboard.queries: logica pura (referencia de tempo, destaque de alertas, filtro, rankings)."""

from __future__ import annotations

import pandas as pd
import pytest

from dashboard import queries as q


def _ts(value: str) -> pd.Timestamp:
    return pd.Timestamp(value, tz="UTC")


def _kpis(last_event_ts) -> pd.DataFrame:
    return pd.DataFrame([{"active_buses": 10, "operating_lines": 3, "avg_city_speed_kmh": 15.0,
                          "last_event_ts": last_event_ts, "latest_window_start": None}],
                        columns=q.CITY_KPIS_COLUMNS)


def _alerts(rows) -> pd.DataFrame:
    df = pd.DataFrame([{"vehicle_id": v, "detected_at": _ts(d) if d else pd.NaT} for v, d in rows])
    return df if not df.empty else pd.DataFrame(columns=["vehicle_id", "detected_at"])


def _metrics(rows) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=["line_sign", "avg_speed_kmh", "active_vehicles"])


def test_reference_time_prefers_last_event_ts():
    ref = q.reference_time(_kpis(_ts("2026-10-14T12:00:00")), _alerts([("1", "2026-10-14T13:00:00")]))
    assert ref == _ts("2026-10-14T12:00:00")


def test_reference_time_falls_back_to_last_alert_then_none():
    alerts = _alerts([("1", "2026-10-14T11:00:00"), ("2", "2026-10-14T11:30:00")])
    assert q.reference_time(_kpis(pd.NaT), alerts) == _ts("2026-10-14T11:30:00")
    assert q.reference_time(pd.DataFrame(columns=q.CITY_KPIS_COLUMNS), alerts) == _ts("2026-10-14T11:30:00")
    assert q.reference_time(_kpis(None), _alerts([])) is None


def test_recently_alerted_vehicles_uses_window_before_reference():
    alerts = _alerts([("1", "2026-10-14T11:44:59"), ("2", "2026-10-14T11:45:00"), ("3", "2026-10-14T11:59:00")])
    assert q.recently_alerted_vehicles(alerts, _ts("2026-10-14T12:00:00")) == {"2", "3"}
    assert q.recently_alerted_vehicles(alerts, _ts("2026-10-14T12:00:00"), minutes=1) == {"3"}
    assert q.recently_alerted_vehicles(alerts, None) == set()
    assert q.recently_alerted_vehicles(_alerts([]), _ts("2026-10-14T12:00:00")) == set()


def test_filter_by_lines():
    df = pd.DataFrame({"line_sign": ["8000-10", "875A-10", "8000-10"], "x": [1, 2, 3]})
    assert q.filter_by_lines(df, ["8000-10"])["x"].tolist() == [1, 3]
    assert q.filter_by_lines(df, []) is df
    assert q.filter_by_lines(df, ["nao-existe"]).empty
    empty = pd.DataFrame(columns=["line_sign"])
    assert q.filter_by_lines(empty, ["8000-10"]) is empty


def test_speed_rankings_ignores_null_speed_and_few_vehicles():
    metrics = _metrics([
        ("A", 10.0, 5), ("B", 30.0, 2), ("C", None, 9), ("D", 5.0, 1), ("E", 20.0, 3), ("F", 10.0, 4),
    ])
    slowest, fastest = q.speed_rankings(metrics, top_n=3)
    assert slowest["line_sign"].tolist() == ["A", "F", "E"]          # empate desfeito pelo letreiro
    assert fastest["line_sign"].tolist() == ["B", "E", "A"]


def test_speed_rankings_empty():
    empty = _metrics([])
    slowest, fastest = q.speed_rankings(empty)
    assert slowest.empty and fastest.empty


def test_to_frame_keeps_columns_and_converts_timestamps():
    df = q._to_frame([("1", "2026-10-14T12:00:00+00:00")], ["vehicle_id", "event_ts"])
    assert list(df.columns) == ["vehicle_id", "event_ts"]
    assert str(df["event_ts"].dt.tz) == "UTC"
    empty = q._to_frame([], q.POSITIONS_COLUMNS)
    assert empty.empty and list(empty.columns) == q.POSITIONS_COLUMNS


@pytest.mark.parametrize("sql, view", [(q.SQL_LATEST_POSITIONS, "vw_latest_positions"),
                                       (q.SQL_LATEST_LINE_METRICS, "vw_latest_line_metrics"),
                                       (q.SQL_CITY_KPIS, "vw_city_kpis"),
                                       (q.SQL_RECENT_ALERTS, "vw_recent_alerts")])
def test_queries_read_only_from_contract_views(sql, view):
    assert sql.startswith("SELECT ") and f"FROM {view}" in sql
    assert "%(limit)s" in sql or "%" not in sql
