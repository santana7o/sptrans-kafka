"""common.schemas: flatten de /Posicao e serializacao dos 3 eventos (contrato secoes 1 e 3)."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from common.schemas import (
    ALERT_TYPE_STOPPED,
    EVENT_FIELDS,
    SCHEMA_VERSION,
    SOURCE_LIVE,
    SOURCE_REPLAY,
    BusAlert,
    BusPosition,
    LineMetric,
    SchemaError,
    flatten_posicao,
)
from common.timeutils import parse_ts
from tests.conftest import make_position

COLLECTED = parse_ts("2017-05-31T20:29:02Z")


# --------------------------------------------------------------------------- flatten


def test_flatten_fields_and_types(posicao_payload):
    events, malformed = flatten_posicao(posicao_payload, COLLECTED, source=SOURCE_LIVE)
    assert malformed == 1                       # veiculo sem "py"
    assert [e.vehicle_id for e in events] == ["11433", "11434", "22001"]
    first = events[0]
    assert first == BusPosition(
        vehicle_id="11433", line_code=1273, line_sign="8000-10", direction=1,
        destination_sign="TERM. LAPA", origin_sign="PCA. RAMOS DE AZEVEDO", accessible=True,
        lat=-23.5402, lon=-46.6402, event_ts=parse_ts("2017-05-31T20:28:49Z"),
        collected_at=COLLECTED, reference_time="20:28", source="live", schema_version=SCHEMA_VERSION,
    )
    for e in events:
        assert isinstance(e.vehicle_id, str) and isinstance(e.line_code, int) and isinstance(e.direction, int)
        assert isinstance(e.lat, float) and isinstance(e.lon, float) and isinstance(e.accessible, bool)
        assert e.event_ts.tzinfo == timezone.utc and e.collected_at == COLLECTED
    assert events[2].direction == 2 and events[2].line_code == 33887


def test_flatten_keeps_zero_coordinates_as_raw_data(posicao_payload):
    events, _ = flatten_posicao(posicao_payload, COLLECTED)
    zero = [e for e in events if e.vehicle_id == "11434"][0]
    assert (zero.lat, zero.lon) == (0.0, 0.0) and zero.accessible is False


def test_flatten_source_and_collected_at_string(posicao_payload):
    events, _ = flatten_posicao(posicao_payload, "2017-05-31T20:29:02Z", source=SOURCE_REPLAY)
    assert {e.source for e in events} == {"replay"} and events[0].collected_at == COLLECTED


def test_flatten_extra_fields_are_ignored(posicao_payload):
    events, _ = flatten_posicao(posicao_payload, COLLECTED)
    assert set(events[0].to_dict()) == set(EVENT_FIELDS["BusPosition"])


def test_flatten_line_without_code_counts_its_vehicles_as_malformed():
    payload = {"hr": "10:00", "l": [
        {"c": "X", "sl": 1, "vs": [{"p": 1, "ta": "2026-10-14T10:00:00Z", "py": -23.5, "px": -46.6}] * 3},
        {"c": "Y", "cl": "abc", "sl": 1, "vs": [{"p": 2, "ta": "2026-10-14T10:00:00Z", "py": -23.5, "px": -46.6}]},
        "linha-que-nao-e-objeto",
    ]}
    events, malformed = flatten_posicao(payload, COLLECTED)
    assert events == [] and malformed == 3 + 1 + 1


@pytest.mark.parametrize(
    "vehicle",
    [
        {"a": True, "ta": "2026-10-14T10:00:00Z", "py": -23.5, "px": -46.6},          # sem p
        {"p": 1, "a": True, "py": -23.5, "px": -46.6},                                 # sem ta
        {"p": 1, "a": True, "ta": "ontem", "py": -23.5, "px": -46.6},                  # ta invalido
        {"p": 1, "a": True, "ta": "2026-10-14T10:00:00Z", "py": "x", "px": -46.6},    # py invalido
        {"p": 1, "a": True, "ta": "2026-10-14T10:00:00Z", "py": None, "px": -46.6},   # py nulo
        "nao-e-objeto",
    ],
)
def test_flatten_malformed_vehicle_is_counted_and_others_survive(vehicle):
    good = {"p": 9, "a": False, "ta": "2026-10-14T10:00:00Z", "py": -23.5, "px": -46.6}
    payload = {"hr": "10:00", "l": [{"c": "X", "cl": 1, "sl": 1, "vs": [vehicle, good]}]}
    events, malformed = flatten_posicao(payload, COLLECTED)
    assert malformed == 1 and [e.vehicle_id for e in events] == ["9"]


@pytest.mark.parametrize("payload", [{}, {"hr": "10:00", "l": None}, {"hr": "10:00", "l": []},
                                     {"l": [{"c": "X", "cl": 1, "sl": 1, "vs": None}]}])
def test_flatten_empty_payloads(payload):
    assert flatten_posicao(payload, COLLECTED) == ([], 0)


def test_flatten_missing_accessible_defaults_to_false():
    payload = {"l": [{"c": "X", "cl": 1, "sl": 1, "vs": [{"p": 1, "ta": "2026-10-14T10:00:00Z", "py": -23.5, "px": -46.6}]}]}
    events, _ = flatten_posicao(payload, COLLECTED)
    assert events[0].accessible is False and events[0].reference_time == ""


@pytest.mark.xfail(strict=True, reason="BUG: flatten_posicao nao captura AttributeError de parse_ts; "
                                       "um veiculo com 'ta': null derruba a coleta inteira")
def test_flatten_null_ta_is_counted_as_malformed():
    payload = {"hr": "10:00", "l": [{"c": "X", "cl": 1, "sl": 1, "vs": [
        {"p": 1, "a": True, "ta": None, "py": -23.5, "px": -46.6},
        {"p": 2, "a": True, "ta": "2026-10-14T10:00:00Z", "py": -23.5, "px": -46.6},
    ]}]}
    events, malformed = flatten_posicao(payload, COLLECTED)
    assert malformed == 1 and [e.vehicle_id for e in events] == ["2"]


@pytest.mark.xfail(strict=True, reason="BUG: flatten_posicao levanta TypeError quando 'vs' nao e lista "
                                       "(ex. numero), em vez de contar como malformado")
def test_flatten_non_list_vs_is_counted_as_malformed():
    payload = {"hr": "10:00", "l": [
        {"c": "X", "cl": 1, "sl": 1, "vs": 5},
        {"c": "Y", "cl": 2, "sl": 1, "vs": [{"p": 2, "a": True, "ta": "2026-10-14T10:00:00Z", "py": -23.5, "px": -46.6}]},
    ]}
    events, malformed = flatten_posicao(payload, COLLECTED)
    assert [e.vehicle_id for e in events] == ["2"] and malformed >= 1


# --------------------------------------------------------------------------- round trip


def _metric() -> LineMetric:
    return LineMetric(
        window_start=parse_ts("2026-10-14T12:00:00Z"), window_end=parse_ts("2026-10-14T12:05:00Z"),
        line_code=1273, line_sign="8000-10", direction=1, avg_speed_kmh=17.4, speed_samples=42,
        active_vehicles=9, accessible_vehicles=8, accessible_pct=88.89,
        computed_at=parse_ts("2026-10-14T12:06:01Z"),
    )


def _alert() -> BusAlert:
    since = parse_ts("2026-10-14T12:01:30Z")
    return BusAlert(
        alert_id=BusAlert.make_id("11433", since), alert_type=ALERT_TYPE_STOPPED, vehicle_id="11433",
        line_code=1273, line_sign="8000-10", direction=1, lat=-23.56, lon=-46.66, stopped_since=since,
        detected_at=parse_ts("2026-10-14T12:11:45Z"), stopped_minutes=10.25, last_speed_kmh=0.4,
    )


@pytest.mark.parametrize("event", [make_position(), _metric(), _alert()], ids=["position", "metric", "alert"])
def test_round_trip_json(event):
    raw = event.to_json_bytes()
    assert type(event).from_json_bytes(raw) == event
    assert type(event).from_json_bytes(raw.decode("utf-8")) == event
    assert b", " not in raw and b": " not in raw  # JSON compacto


def test_position_json_matches_contract_example():
    pos = BusPosition.from_dict({
        "vehicle_id": "11433", "line_code": 1273, "line_sign": "8000-10", "direction": 1,
        "destination_sign": "TERM. LAPA", "origin_sign": "PCA. RAMOS DE AZEVEDO", "accessible": True,
        "lat": -23.5402, "lon": -46.6402, "event_ts": "2017-05-31T20:28:49Z",
        "collected_at": "2017-05-31T20:29:02Z", "reference_time": "20:28", "source": "live", "schema_version": 1,
    })
    data = json.loads(pos.to_json_bytes())
    assert data["event_ts"] == "2017-05-31T20:28:49Z" and data["collected_at"] == "2017-05-31T20:29:02Z"
    assert list(data) == EVENT_FIELDS["BusPosition"]
    assert pos.key() == b"11433"


def test_metric_none_speed_round_trip_and_key():
    metric = LineMetric(**{**_metric().__dict__, "avg_speed_kmh": None, "speed_samples": 0})
    assert LineMetric.from_json_bytes(metric.to_json_bytes()).avg_speed_kmh is None
    assert metric.key() == b"1273-1"


def test_alert_make_id_is_deterministic_and_utc():
    assert BusAlert.make_id("11433", parse_ts("2026-10-14T12:01:30Z")) == "11433-20261014T120130Z"
    assert BusAlert.make_id("11433", "2026-10-14T09:01:30-03:00") == "11433-20261014T120130Z"
    assert _alert().key() == b"11433"


def test_utf8_text_round_trip():
    pos = make_position(line_sign="JD. SÃO LUÍS")
    raw = pos.to_json_bytes()
    assert "SÃO".encode("utf-8") in raw and BusPosition.from_json_bytes(raw).line_sign == "JD. SÃO LUÍS"


def test_optional_fields_get_defaults():
    pos = BusPosition.from_dict({"vehicle_id": 5, "line_code": "7", "direction": "2", "lat": "-23.5",
                                 "lon": -46.6, "event_ts": "2026-10-14T12:00:00Z",
                                 "collected_at": datetime(2026, 10, 14, 12, 0, 5)})
    assert pos.vehicle_id == "5" and pos.line_code == 7 and pos.direction == 2 and pos.lat == -23.5
    assert pos.source == "live" and pos.accessible is False and pos.schema_version == SCHEMA_VERSION
    assert pos.collected_at.tzinfo == timezone.utc


# --------------------------------------------------------------------------- erros


@pytest.mark.parametrize("raw", [b"{not json", b"", None, b"[1, 2]", b'"texto"', b"\xff\xfe", b"null"])
def test_bad_json_raises_schema_error(raw):
    with pytest.raises(SchemaError):
        BusPosition.from_json_bytes(raw)


@pytest.mark.parametrize("missing", ["vehicle_id", "line_code", "direction", "lat", "lon", "event_ts", "collected_at"])
def test_missing_required_field_raises_schema_error(missing):
    data = json.loads(make_position().to_json_bytes())
    data.pop(missing)
    with pytest.raises(SchemaError):
        BusPosition.from_dict(data)


@pytest.mark.parametrize("field, value", [("line_code", "abc"), ("lat", "x"), ("event_ts", "ontem"),
                                          ("lat", None), ("direction", [1])])
def test_wrong_type_raises_schema_error(field, value):
    data = json.loads(make_position().to_json_bytes())
    data[field] = value
    with pytest.raises(SchemaError):
        BusPosition.from_dict(data)


@pytest.mark.xfail(strict=True, reason="BUG: event_ts nao-string (numero/objeto) gera AttributeError em "
                                       "parse_ts em vez de SchemaError; derruba os consumers (poison message)")
@pytest.mark.parametrize("value", [1760443200, {"a": 1}])
def test_non_string_timestamp_raises_schema_error(value):
    data = json.loads(make_position().to_json_bytes())
    data["event_ts"] = value
    with pytest.raises(SchemaError):
        BusPosition.from_dict(data)


@pytest.mark.parametrize("cls, missing", [(LineMetric, "window_start"), (LineMetric, "active_vehicles"),
                                          (BusAlert, "alert_id"), (BusAlert, "stopped_minutes")])
def test_metric_and_alert_required_fields(cls, missing):
    event = _metric() if cls is LineMetric else _alert()
    data = json.loads(event.to_json_bytes())
    data.pop(missing)
    with pytest.raises(SchemaError):
        cls.from_dict(data)
