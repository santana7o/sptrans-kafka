"""producer/: dedup, replay (rebase, agenda, leitura do sample), cliente Olho Vivo e publicacao."""

from __future__ import annotations

import gzip
import json
from datetime import timedelta
from pathlib import Path

import pytest
import requests

from common.runtime import ShutdownFlag
from common.schemas import BusPosition, flatten_posicao
from common.timeutils import parse_ts, utcnow
from producer.dedup import Deduplicator
from producer.live import next_wait_seconds
from producer.olhovivo import (
    AuthenticationError,
    OlhoVivoClient,
    OlhoVivoError,
    backoff_delay,
    is_retryable_status,
    is_session_expired,
    parse_auth_response,
    parse_positions_response,
    redact,
)
from producer.pipeline import process_collection
from producer.publisher import Publisher
from producer.replay import (
    ReplayFileError,
    iter_collections,
    lap_delta,
    rebase_payload,
    replay_wait_seconds,
    run_replay,
    scan_file,
    shift_hhmm,
    shift_iso,
)
from tests.conftest import make_position
from tests.fakes import FakeProducer

TOKEN = "segredo-de-teste-123"


# --------------------------------------------------------------------------- dedup


def test_dedup_drops_same_or_older_event_ts_per_vehicle():
    dedup = Deduplicator()
    kept, dups = dedup.filter([make_position("A", t=0), make_position("B", t=0), make_position("A", t=0)])
    assert [p.vehicle_id for p in kept] == ["A", "B"] and dups == 1
    kept, dups = dedup.filter([make_position("A", t=0), make_position("A", t=-30), make_position("A", t=30)])
    assert [p.event_ts for p in kept] == [make_position(t=30).event_ts] and dups == 2
    assert dedup.last_event_ts("A") == make_position(t=30).event_ts and len(dedup) == 2


def test_dedup_is_new_and_reset():
    dedup = Deduplicator()
    pos = make_position("A")
    assert dedup.is_new(pos)
    dedup.filter([pos])
    assert not dedup.is_new(pos)
    dedup.reset()
    assert dedup.is_new(pos) and len(dedup) == 0


def test_dedup_ignores_line_change():
    dedup = Deduplicator()
    kept, dups = dedup.filter([make_position("A", line_code=1), make_position("A", line_code=2)])
    assert len(kept) == 1 and dups == 1


# --------------------------------------------------------------------------- replay: logica pura


def test_rebase_payload_shifts_ta_and_hr_without_mutating(posicao_payload):
    original = json.loads(json.dumps(posicao_payload))
    out = rebase_payload(posicao_payload, timedelta(hours=1, minutes=5))
    assert posicao_payload == original                          # funcao pura
    assert out["hr"] == "21:33" and out["campo_novo"] == {"x": 1}
    v = out["l"][0]["vs"][0]
    assert v["ta"] == "2017-05-31T21:33:49Z" and v["is"] == "campo extra" and v["p"] == 11433
    assert out["l"][0]["extra_linha"] == "ignorar"
    # o vehicle sem py continua sem py; nada e inventado
    assert "py" not in out["l"][1]["vs"][1]


def test_rebase_payload_preserves_invalid_values_and_shapes():
    payload = {"l": [{"cl": 1, "vs": [{"p": 1, "ta": "lixo"}, "x", {"p": 2}]}, "linha-estranha", {"cl": 2, "vs": None}]}
    out = rebase_payload(payload, timedelta(minutes=1))
    assert out["l"][0]["vs"] == [{"p": 1, "ta": "lixo"}, "x", {"p": 2}]
    assert out["l"][1] == "linha-estranha" and out["l"][2] == {"cl": 2, "vs": None}
    assert "hr" not in out


def test_rebase_then_flatten_keeps_intervals():
    payload = {"hr": "10:00", "l": [{"c": "X", "cl": 1, "sl": 1, "vs": [
        {"p": 1, "ta": "2026-10-14T10:00:00Z", "py": -23.5, "px": -46.6},
        {"p": 2, "ta": "2026-10-14T10:00:30Z", "py": -23.5, "px": -46.6}]}]}
    delta = timedelta(days=3, seconds=17)
    a, b = flatten_posicao(rebase_payload(payload, delta), "2026-10-14T10:01:00Z")[0]
    assert a.event_ts == parse_ts("2026-10-14T10:00:00Z") + delta
    assert (b.event_ts - a.event_ts).total_seconds() == 30


@pytest.mark.parametrize("value, minutes, expected", [
    ("23:50", 20, "00:10"), ("00:05", -10, "23:55"), ("10:00", 24 * 60, "10:00"), ("08:30", 0, "08:30"),
])
def test_shift_hhmm_wraps_around_midnight(value, minutes, expected):
    assert shift_hhmm(value, timedelta(minutes=minutes)) == expected


@pytest.mark.parametrize("value", ["", "xx", None, "10:00:00"])
def test_shift_hhmm_invalid_is_kept(value):
    assert shift_hhmm(value, timedelta(minutes=5)) == value


def test_shift_iso():
    assert shift_iso("2026-10-14T23:59:30Z", timedelta(seconds=45)) == "2026-10-15T00:00:15Z"
    assert shift_iso(None, timedelta(seconds=1)) is None and shift_iso("x", timedelta(seconds=1)) == "x"


def test_lap_delta_adds_file_duration_and_gap_per_lap():
    base, duration, gap = timedelta(hours=1), timedelta(minutes=40), timedelta(seconds=30)
    assert lap_delta(base, 0, duration, gap) == base
    assert lap_delta(base, 2, duration, gap) == base + 2 * timedelta(minutes=40, seconds=30)


@pytest.mark.parametrize("offset, speed, elapsed, expected", [
    (0, 5, 0, 0.0), (300, 5, 0, 60.0), (300, 5, 59.5, 0.5), (300, 5, 90, 0.0), (30, 1, 10, 20.0),
])
def test_replay_wait_seconds_schedules_from_start(offset, speed, elapsed, expected):
    assert replay_wait_seconds(offset, speed, elapsed) == pytest.approx(expected)


# --------------------------------------------------------------------------- replay: arquivos


def test_scan_sample_file(sample_path):
    info = scan_file(sample_path)
    assert info.collections == 81
    assert info.gap == timedelta(seconds=30)
    assert info.duration == timedelta(minutes=40)
    assert info.first < info.last


def test_sample_format_matches_contract(sample_collections):
    stamps = [c for c, _ in sample_collections]
    assert stamps == sorted(stamps) and len(stamps) == 81
    first_payload = sample_collections[0][1]
    assert set(first_payload) >= {"hr", "l"}
    line = first_payload["l"][0]
    assert {"c", "cl", "sl", "lt0", "lt1", "qv", "vs"} <= set(line)
    assert {"p", "a", "ta", "py", "px"} <= set(line["vs"][0])


def test_iter_collections_skips_invalid_lines_in_plain_text(tmp_path: Path):
    path = tmp_path / "coletas.jsonl"
    path.write_text("\n".join([
        json.dumps({"collected_at": "2026-10-14T10:00:00Z", "payload": {"hr": "07:00", "l": []}}),
        "",
        "{nao e json",
        json.dumps({"payload": {"l": []}}),                                   # sem collected_at
        json.dumps({"collected_at": "2026-10-14T10:00:30Z", "payload": []}),  # payload nao e objeto
        json.dumps({"collected_at": "ontem", "payload": {}}),
        json.dumps({"collected_at": "2026-10-14T10:01:00Z", "payload": {"hr": "07:01", "l": []}}),
    ]), encoding="utf-8")
    rows = list(iter_collections(path))
    assert [to for to, _ in rows] == [parse_ts("2026-10-14T10:00:00Z"), parse_ts("2026-10-14T10:01:00Z")]
    info = scan_file(path)
    assert info.collections == 2 and info.gap == timedelta(seconds=60)


def test_iter_collections_reads_gzip(tmp_path: Path):
    path = tmp_path / "x.jsonl.gz"
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        fh.write(json.dumps({"collected_at": "2026-10-14T10:00:00Z", "payload": {"l": []}}) + "\n")
    info = scan_file(path)
    assert info.collections == 1 and info.gap == timedelta(seconds=30)   # padrao com uma coleta


def test_scan_file_errors(tmp_path: Path):
    with pytest.raises(ReplayFileError):
        scan_file(tmp_path / "nao-existe.jsonl.gz")
    empty = tmp_path / "vazio.jsonl"
    empty.write_text("\n\nlixo\n", encoding="utf-8")
    with pytest.raises(ReplayFileError):
        scan_file(empty)


def test_two_replay_laps_produce_no_cross_lap_duplicates(sample_collections, sample_path):
    info = scan_file(sample_path)
    dedup = Deduplicator()
    per_lap = []
    for lap in range(2):
        delta = lap_delta(timedelta(days=1), lap, info.duration, info.gap)
        published = dups = 0
        for collected_at, payload in sample_collections:
            events, _ = flatten_posicao(rebase_payload(payload, delta), collected_at + delta)
            kept, d = dedup.filter(events)
            published += len(kept)
            dups += d
        per_lap.append((published, dups))
    assert per_lap == [(40095 - 562, 562), (40095 - 562, 562)]


def test_run_replay_publishes_whole_sample_rebased_to_now(clean_env, sample_path):
    clean_env.setenv("REPLAY_FILE", str(sample_path))
    clean_env.setenv("REPLAY_SPEED", "1000000")
    clean_env.setenv("REPLAY_LOOP", "false")
    from common.config import load_settings

    settings = load_settings()
    producer = FakeProducer()
    started = utcnow()
    run_replay(settings, producer, ShutdownFlag(install_handlers=False))
    assert len(producer.produced) == 40095 - 562
    assert {topic for topic, _, _ in producer.produced} == {"bus-positions-raw"}
    first = BusPosition.from_json_bytes(producer.produced[0][2])
    assert first.source == "replay" and producer.produced[0][1] == first.vehicle_id.encode()
    # rebase D04: a primeira coleta corresponde ao inicio da reproducao
    assert abs((first.collected_at - started).total_seconds()) < 5
    assert first.collected_at.microsecond == 0


def test_run_replay_stops_immediately_on_shutdown(clean_env, sample_path):
    clean_env.setenv("REPLAY_FILE", str(sample_path))
    from common.config import load_settings

    shutdown = ShutdownFlag(install_handlers=False)
    shutdown.set()
    producer = FakeProducer()
    run_replay(load_settings(), producer, shutdown)
    assert producer.produced == []


# --------------------------------------------------------------------------- pipeline e publisher


def test_process_collection_stats(posicao_payload):
    producer = FakeProducer()
    publisher = Publisher(producer, "bus-positions-raw", poll_every=2)
    dedup = Deduplicator()
    stats = process_collection(posicao_payload, parse_ts("2017-05-31T20:29:02Z"), "live", dedup, publisher)
    assert (stats.lines, stats.vehicles, stats.published, stats.duplicates, stats.malformed) == (2, 3, 3, 0, 1)
    assert publisher.delivered == 3 and publisher.failed == 0 and producer.polls == [0]
    again = process_collection(posicao_payload, parse_ts("2017-05-31T20:29:32Z"), "live", dedup, publisher)
    assert again.published == 0 and again.duplicates == 3


def test_publisher_counts_delivery_failures_and_buffer_errors():
    producer = FakeProducer(buffer_errors=1, delivery_error="broker down")
    publisher = Publisher(producer, "t")
    assert publisher.publish([make_position("a"), make_position("b")]) == 2
    assert publisher.failed == 2 and publisher.delivered == 0 and 0.5 in producer.polls


def test_next_wait_seconds():
    assert next_wait_seconds(30, 5) == 25 and next_wait_seconds(30, 45) == 0.0


# --------------------------------------------------------------------------- olho vivo: logica pura


def test_backoff_delay_exponential_with_cap_and_jitter():
    assert backoff_delay(0, rand=lambda: 0.0) == 0.5
    assert backoff_delay(0, rand=lambda: 1.0) == 1.0
    assert backoff_delay(3, rand=lambda: 1.0) == 8.0
    assert backoff_delay(10, rand=lambda: 1.0) == 30.0
    assert backoff_delay(10, rand=lambda: 0.0) == 15.0
    assert backoff_delay(-1, rand=lambda: 1.0) == 1.0
    for attempt in range(8):
        assert 0 < backoff_delay(attempt) <= 30.0


@pytest.mark.parametrize("body, expected", [("true", True), ("True", True), ('"true"', True), (" true\n", True),
                                            ("false", False), ("", False), ("<html>", False)])
def test_parse_auth_response(body, expected):
    assert parse_auth_response(body) is expected


@pytest.mark.parametrize("status, body, expected", [
    (401, "", True), (403, "", True),
    (200, '{"Message":"Authorization has been denied for this request."}', True),
    (200, '{"hr":"10:00","l":[]}', False), (500, "", False),
    (200, "x" * 400 + "authorization has been denied", False),   # so olha o inicio do corpo
])
def test_is_session_expired(status, body, expected):
    assert is_session_expired(status, body) is expected


@pytest.mark.parametrize("status, expected", [(500, True), (503, True), (429, True), (200, False),
                                              (401, False), (404, False)])
def test_is_retryable_status(status, expected):
    assert is_retryable_status(status) is expected


def test_parse_positions_response():
    assert parse_positions_response({"hr": "10:00", "l": [], "extra": 1}) == {"hr": "10:00", "l": [], "extra": 1}
    assert parse_positions_response({"hr": None, "l": None}) == {"hr": "", "l": []}
    for bad in ([], "x", {"l": {}}, {"Message": "Authorization has been denied"}):
        with pytest.raises(OlhoVivoError):
            parse_positions_response(bad)


def test_redact():
    assert redact(f"https://x/Login?token={TOKEN}", TOKEN) == "https://x/Login?token=***"
    assert redact("sem segredo", "") == "sem segredo"


# --------------------------------------------------------------------------- olho vivo: cliente


class FakeResponse:
    def __init__(self, status_code: int = 200, text: str = "", payload=None) -> None:
        self.status_code = status_code
        self.text = text if payload is None else json.dumps(payload)
        self._payload = payload

    def json(self):
        if self._payload is None:
            return json.loads(self.text)
        return self._payload


class FakeCookies:
    def __init__(self) -> None:
        self.cleared = 0

    def clear(self) -> None:
        self.cleared += 1


class FakeSession:
    def __init__(self, responses) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, str, dict | None, float]] = []
        self.headers: dict[str, str] = {}
        self.cookies = FakeCookies()
        self.closed = False

    def request(self, method, url, params=None, timeout=None):
        self.calls.append((method, url, params, timeout))
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    def close(self) -> None:
        self.closed = True


POSITIONS = {"hr": "10:00", "l": [{"c": "X", "cl": 1, "sl": 1, "vs": []}]}


def _client(responses, max_retries: int = 3):
    session = FakeSession(responses)
    sleeps: list[float] = []
    client = OlhoVivoClient("https://api.test/v2.1/", TOKEN, timeout_s=7, max_retries=max_retries,
                            session=session, sleep=sleeps.append)
    return client, session, sleeps


def test_client_requires_token():
    with pytest.raises(AuthenticationError):
        OlhoVivoClient("https://api.test", "", session=FakeSession([]))


def test_client_authenticates_then_gets_positions():
    client, session, sleeps = _client([FakeResponse(200, "true"), FakeResponse(200, payload=POSITIONS)])
    assert client.get_positions() == POSITIONS
    (m1, url1, params1, timeout1), (m2, url2, _, _) = session.calls
    assert (m1, url1, params1, timeout1) == ("POST", "https://api.test/v2.1/Login/Autenticar", {"token": TOKEN}, 7)
    assert (m2, url2) == ("GET", "https://api.test/v2.1/Posicao")
    assert client.authenticated and session.cookies.cleared == 1 and sleeps == []
    assert session.headers["User-Agent"].startswith("sptrans-kafka")


def test_client_rejected_token_raises_without_leaking_it():
    client, _, _ = _client([FakeResponse(200, "false")])
    with pytest.raises(AuthenticationError) as exc:
        client.get_positions()
    assert TOKEN not in str(exc.value) and not client.authenticated
    assert TOKEN not in repr(client)


def test_client_retries_transient_errors_with_backoff():
    client, session, sleeps = _client([
        FakeResponse(200, "true"),
        FakeResponse(503, "busy"),
        requests.ConnectionError(f"falhou https://api.test/?token={TOKEN}"),
        FakeResponse(200, payload=POSITIONS),
    ], max_retries=3)
    assert client.get_positions() == POSITIONS
    assert len(session.calls) == 4 and len(sleeps) == 2
    assert 0.5 <= sleeps[0] <= 1.0 and 1.0 <= sleeps[1] <= 2.0


def test_client_gives_up_after_max_attempts_and_redacts_token():
    client, session, sleeps = _client([requests.Timeout(f"timeout token={TOKEN}")] * 3, max_retries=3)
    with pytest.raises(OlhoVivoError) as exc:
        client.authenticate()
    assert "3 tentativas" in str(exc.value) and TOKEN not in str(exc.value) and "***" in str(exc.value)
    assert len(session.calls) == 3 and len(sleeps) == 2


def test_client_reauthenticates_when_session_expires():
    client, session, _ = _client([
        FakeResponse(200, "true"),
        FakeResponse(401, '{"Message":"Authorization has been denied for this request."}'),
        FakeResponse(200, "true"),
        FakeResponse(200, payload=POSITIONS),
    ])
    assert client.get_positions() == POSITIONS
    assert [c[0] for c in session.calls] == ["POST", "GET", "POST", "GET"]
    assert session.cookies.cleared == 2


def test_client_session_denied_in_body_with_200():
    client, session, _ = _client([
        FakeResponse(200, "true"),
        FakeResponse(200, '{"Message":"Authorization has been denied for this request."}'),
        FakeResponse(200, "true"),
        FakeResponse(200, '{"Message":"Authorization has been denied for this request."}'),
    ])
    with pytest.raises(AuthenticationError):
        client.get_positions()
    assert not client.authenticated and len(session.calls) == 4


def test_client_unexpected_status_and_invalid_json():
    client, _, _ = _client([FakeResponse(200, "true"), FakeResponse(404, "nao achei")])
    with pytest.raises(OlhoVivoError, match="404"):
        client.get_positions()
    client, _, _ = _client([FakeResponse(200, "true"), FakeResponse(200, "<html>")])
    with pytest.raises(OlhoVivoError, match="JSON"):
        client.get_positions()


def test_client_does_not_reauthenticate_on_every_call():
    client, session, _ = _client([FakeResponse(200, "true"), FakeResponse(200, payload=POSITIONS),
                                  FakeResponse(200, payload=POSITIONS)])
    client.get_positions()
    client.get_positions()
    assert [c[0] for c in session.calls] == ["POST", "GET", "GET"]
    client.close()
    assert session.closed


def test_client_stops_retrying_on_shutdown():
    shutdown = ShutdownFlag(install_handlers=False)
    shutdown.set()
    session = FakeSession([FakeResponse(503), FakeResponse(503)])
    client = OlhoVivoClient("https://api.test", TOKEN, max_retries=5, session=session, shutdown=shutdown)
    with pytest.raises(OlhoVivoError, match="encerramento"):
        client.authenticate()
    assert len(session.calls) == 1


def test_client_from_settings(clean_env):
    clean_env.setenv("SPTRANS_TOKEN", TOKEN)
    clean_env.setenv("HTTP_MAX_RETRIES", "2")
    from common.config import load_settings

    client = OlhoVivoClient.from_settings(load_settings())
    try:
        assert client.max_attempts == 2 and client.base_url == "https://api.olhovivo.sptrans.com.br/v2.1"
    finally:
        client.close()
