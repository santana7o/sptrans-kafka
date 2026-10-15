"""Consumer de persistencia sem Kafka e sem Postgres (fakes injetados).

Base: testes do consumer de persistencia, adaptados as fixtures da suite.
"""

from __future__ import annotations

from contextlib import contextmanager

import psycopg
import pytest

from common.runtime import ShutdownFlag
from consumers.persistence import main as pm
from consumers.persistence import writer as pw
from tests.conftest import make_position
from tests.fakes import FakeConsumer, FakeKafkaError, FakeMsg, StepClock


def _msg(vehicle: str, offset: int, t: float = 0, partition: int = 0) -> FakeMsg:
    return FakeMsg(make_position(vehicle, t=t).to_json_bytes(), offset, partition)


class FakeWriter:
    """Writer idempotente em memoria (imita ON CONFLICT DO NOTHING)."""

    def __init__(self, fail_times: int = 0) -> None:
        self.fail_times = fail_times
        self.written: list[list] = []
        self.resets = 0
        self.seen: set = set()

    def write(self, positions):
        if self.fail_times > 0:
            self.fail_times -= 1
            raise psycopg.OperationalError("server closed the connection")
        new = 0
        for p in positions:
            key = (p.vehicle_id, p.event_ts)
            if key not in self.seen:
                self.seen.add(key)
                new += 1
        self.written.append(list(positions))
        return new

    def reset(self):
        self.resets += 1


def _persister(consumer, writer, shutdown=None, commits=None, batch_size=3, flush=10.0, commit_fn=None):
    commits = commits if commits is not None else []
    persister = pm.BatchPersister(
        consumer, writer, shutdown or ShutdownFlag(install_handlers=False), batch_size, flush,
        commit_fn=commit_fn or (lambda c, msgs: commits.append([m.offset() for m in msgs])),
        clock=StepClock(step=0.5), retry_initial_s=0.0, retry_max_s=0.0,
    )
    return persister, commits


def test_collect_until_batch_size():
    msgs = [_msg(str(i), i) for i in range(5)]
    consumer = FakeConsumer([msgs[:2], msgs[2:3], msgs[3:]])
    persister, _ = _persister(consumer, FakeWriter())
    batch = persister.collect_batch()
    assert [m.offset() for m in batch] == [0, 1, 2]
    assert consumer.calls[0][0] == 3 and consumer.calls[1][0] == 1


def test_collect_stops_on_flush_timeout():
    consumer = FakeConsumer([])
    persister, _ = _persister(consumer, FakeWriter(), flush=2.0)
    assert persister.collect_batch() == []
    assert 1 <= len(consumer.calls) <= 4
    assert all(timeout <= 1.0 for _, timeout in consumer.calls)


def test_collect_stops_on_shutdown():
    shutdown = ShutdownFlag(install_handlers=False)
    shutdown.set()
    consumer = FakeConsumer([[_msg("a", 0)]])
    persister, _ = _persister(consumer, FakeWriter(), shutdown=shutdown)
    assert persister.collect_batch() == [] and consumer.calls == []


def test_malformed_skipped_but_offset_committed():
    msgs = [_msg("1", 10), FakeMsg(b"{not json", 11), FakeMsg(b'{"vehicle_id":"2"}', 12), FakeMsg(None, 13)]
    writer = FakeWriter()
    persister, commits = _persister(FakeConsumer([]), writer)
    assert persister.process_batch(msgs) is True
    assert [x.vehicle_id for x in writer.written[0]] == ["1"]
    assert commits == [[10, 11, 12, 13]]
    assert persister.totals["malformed"] == 3


def test_empty_batch_is_noop():
    writer = FakeWriter()
    persister, commits = _persister(FakeConsumer([]), writer)
    assert persister.process_batch([]) is True and commits == [] and writer.written == []


def test_db_failure_retries_same_batch_and_commits_after():
    msgs = [_msg(str(i), i) for i in range(3)]
    order: list[str] = []
    writer = FakeWriter(fail_times=2)
    original = writer.write

    def write(positions):
        result = original(positions)
        order.append("db")
        return result

    writer.write = write
    persister, _ = _persister(FakeConsumer([]), writer, commit_fn=lambda c, m: order.append("kafka"))
    assert persister.process_batch(msgs)
    assert writer.resets == 2 and order == ["db", "kafka"]
    assert len(writer.written) == 1 and len(writer.written[0]) == 3


def test_shutdown_during_db_failure_does_not_commit():
    shutdown = ShutdownFlag(install_handlers=False)
    shutdown.set()
    writer = FakeWriter(fail_times=100)
    persister, commits = _persister(FakeConsumer([]), writer, shutdown=shutdown)
    assert persister.process_batch([_msg("a", 1)]) is False
    assert commits == [] and writer.written == []


def test_reprocessing_same_batch_counts_duplicates():
    msgs = [_msg(str(i), i) for i in range(3)]
    persister, commits = _persister(FakeConsumer([]), FakeWriter())
    persister.process_batch(msgs)
    persister.process_batch(msgs)
    assert persister.totals["inserted"] == 3 and persister.totals["duplicates"] == 3 and len(commits) == 2


def test_kafka_error_messages_not_decoded_and_commit_failure_tolerated():
    msgs = [FakeMsg(None, 0, err=FakeKafkaError()), _msg("a", 1)]
    writer = FakeWriter()

    def bad_commit(consumer, messages):
        raise RuntimeError("kafka down")

    persister, _ = _persister(FakeConsumer([]), writer, commit_fn=bad_commit)
    assert persister.process_batch(msgs) is True
    assert len(writer.written[0]) == 1 and persister.totals["malformed"] == 0


def test_run_flushes_partial_batch_on_shutdown_and_closes():
    shutdown = ShutdownFlag(install_handlers=False)

    class StoppingConsumer(FakeConsumer):
        def consume(self, num_messages=1, timeout=-1):
            shutdown.set()
            return [_msg("1", 0)]

    consumer = StoppingConsumer([])
    persister, commits = _persister(consumer, FakeWriter(), shutdown=shutdown)
    persister.run()
    assert commits == [[0]] and consumer.closed


def test_run_with_real_commit_offsets_and_fake_consumer():
    from common.kafka_utils import commit_offsets

    msgs = [_msg("1", 5, partition=0), _msg("2", 7, partition=0), FakeMsg(b"x", 3, 2)]
    consumer = FakeConsumer([msgs])
    persister = pm.BatchPersister(consumer, FakeWriter(), ShutdownFlag(install_handlers=False), 3, 1.0,
                                  commit_fn=commit_offsets, clock=StepClock(step=0.5))
    persister.process_batch(persister.collect_batch())
    assert sorted(consumer.committed[0]) == [("bus-positions-raw", 0, 8), ("bus-positions-raw", 2, 4)]


def test_decode_messages():
    positions, malformed = pm.decode_messages([_msg("a", 0), FakeMsg(b"[]", 1), FakeMsg(None, 2, err=FakeKafkaError())])
    assert [p.vehicle_id for p in positions] == ["a"] and malformed == 1


def test_poison_message_is_counted_as_malformed():
    poison = FakeMsg(b'{"vehicle_id":"1","line_code":1,"direction":1,"lat":-23.5,"lon":-46.6,'
                     b'"event_ts":1760443200,"collected_at":"2026-10-14T12:00:00Z"}', 1)
    positions, malformed = pm.decode_messages([_msg("a", 0), poison])
    assert len(positions) == 1 and malformed == 1


# --------------------------------------------------------------------------- writer


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params):
        if self.conn.data_error_on_batch and isinstance(params[0], list):
            raise psycopg.DataError("caractere NUL")
        if self.conn.bad_vehicle is not None and params[0] == self.conn.bad_vehicle:
            raise psycopg.DataError("linha ruim")
        self.conn.log.append((sql, params))
        self.rowcount = len(params[0]) if isinstance(params[0], list) else 1


class FakeConn:
    def __init__(self, data_error_on_batch: bool = False, bad_vehicle: str | None = None):
        self.log: list = []
        self.closed = False
        self.commits = self.rollbacks = 0
        self.data_error_on_batch = data_error_on_batch
        self.bad_vehicle = bad_vehicle

    def cursor(self):
        return FakeCursor(self)

    @contextmanager
    def transaction(self):
        yield

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


def test_insert_positions_builds_column_arrays_and_dumps():
    conn = FakeConn()
    positions = [make_position("1"), make_position("2", t=30, accessible=False)]
    assert pw.insert_positions(conn, positions) == 2
    sql, params = conn.log[0]
    assert "ON CONFLICT (vehicle_id, event_ts) DO NOTHING" in sql
    assert sql.count("%s") == len(params) == 12
    assert params[0] == ["1", "2"] and params[6] == [True, False]
    assert all(len(col) == 2 for col in params)
    # psycopg consegue adaptar todos os arrays (offline, sem servidor)
    from psycopg.adapt import PyFormat, Transformer

    tx = Transformer()
    dumped = [tx.get_dumper(col, PyFormat.TEXT).dump(col) for col in params]
    assert b"2026-10-14 12:00:30" in bytes(dumped[9])
    assert pw.insert_positions(conn, []) == 0 and len(conn.log) == 1


def test_writer_commits_and_returns_inserted(test_settings):
    writer = pw.PostgresPositionWriter(test_settings)
    conn = FakeConn()
    writer._conn = conn
    assert writer.write([make_position("1"), make_position("2")]) == 2
    assert conn.commits == 1 and conn.rollbacks == 0
    assert writer.write([]) == 0 and conn.commits == 1


def test_writer_falls_back_to_row_by_row_on_data_error(test_settings):
    writer = pw.PostgresPositionWriter(test_settings)
    conn = FakeConn(data_error_on_batch=True, bad_vehicle="bad")
    writer._conn = conn
    inserted = writer.write([make_position("1"), make_position("bad"), make_position("2")])
    assert inserted == 2 and conn.rollbacks == 1 and conn.commits == 1


def test_writer_rolls_back_and_reraises_on_other_errors(test_settings):
    class Broken(FakeConn):
        def cursor(self):
            raise psycopg.OperationalError("conexao caiu")

    writer = pw.PostgresPositionWriter(test_settings)
    conn = Broken()
    writer._conn = conn
    with pytest.raises(psycopg.OperationalError):
        writer.write([make_position("1")])
    assert conn.rollbacks == 1
    writer.reset()
    assert conn.closed and writer._conn is None
