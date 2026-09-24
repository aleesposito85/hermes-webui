"""Run-journal archival retention (#7613).

The feature ARCHIVES runs past the age / count / size caps: eligible
``{rid}.jsonl`` files are compressed into ``_run_journal_archive/<sid>/`` and
the live file is only dropped once the compressed copy is proven to decompress
back to the exact bytes. A wrong classification therefore costs one compressed
copy instead of the data, and every read path falls back to the archive.

What must hold:
  * non-terminal runs are never archived (a live or crashed run stays readable
    in place);
  * the compressed copy is byte-exact before the live file is dropped;
  * when anything fails mid-way the live file survives (a missed archive is
    safe, a wrong archive is recoverable);
  * reads (summaries, event reads, session replay, run lookup) transparently
    see archived runs;
  * the sweep is off the request path and gated: env kill switch, hourly
    schedule with a boot delay, one sweep at a time;
  * the only destructive step — archive pruning — is disabled by default.
"""
import gzip
import json
import os
import time
from pathlib import Path

import pytest

from api import run_journal as rj


# ── helpers ────────────────────────────────────────────────────────────────


def _write_run(root: Path, sid: str, rid: str, *, events: list[tuple[str, dict]] | None = None,
               terminal: bool = True, mtime_age_days: float = 0.0,
               terminal_state: str = "completed", truncate_terminal: bool = False) -> Path:
    """Create one run file shaped exactly like ``append_run_event`` writes it."""
    session_dir = root / rj.RUN_JOURNAL_DIR_NAME / sid
    session_dir.mkdir(parents=True, exist_ok=True)
    path = session_dir / f"{rid}.jsonl"
    rows = []
    seq = 1
    for name, payload in (events or [("token", {"text": "hello"}), ("token", {"text": " world"})]):
        rows.append({
            "version": 1,
            "event_id": f"{rid}:{seq}",
            "seq": seq,
            "run_id": rid,
            "session_id": sid,
            "event": name,
            "type": name,
            "created_at": time.time() - 3600,
            "terminal": False,
            "terminal_state": None,
            "payload": payload,
        })
        seq += 1
    if terminal:
        rows.append({
            "version": 1,
            "event_id": f"{rid}:{seq}",
            "seq": seq,
            "run_id": rid,
            "session_id": sid,
            "event": "done",
            "type": "done",
            "created_at": time.time() - 3600,
            "terminal": True,
            "terminal_state": terminal_state,
            "payload": {"terminal_state": terminal_state},
        })
    body = "".join(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n" for row in rows)
    if truncate_terminal:
        # Simulate a journal whose last row was cut mid-write: the bytes still
        # contain the ``"terminal":true`` marker but the row has no terminating
        # newline and does not parse as a complete JSON document.
        body = "".join(
            json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n" for row in rows[:-1]
        )
        last = json.dumps(rows[-1], ensure_ascii=False, separators=(",", ":"))
        body += last[: last.index('"terminal":true') + len('"terminal":true')]
    path.write_text(body, encoding="utf-8")
    if mtime_age_days:
        old = time.time() - mtime_age_days * 86400.0
        os.utime(path, (old, old))
    return path


def _archive_path(root: Path, sid: str, rid: str) -> Path:
    return root / rj.RUN_JOURNAL_ARCHIVE_DIR_NAME / sid / f"{rid}.jsonl.gz"


def _sweep(root: Path, **caps):
    return rj.sweep_run_journal(session_dir=root, **caps)


# ── classification: only provably-terminal runs are ever archived ──────────


def test_terminal_run_is_archived(tmp_path):
    _write_run(tmp_path, "s1", "r1", mtime_age_days=30)
    counters = _sweep(tmp_path, ttl_days=14, max_runs_per_session=0, max_bytes_per_session=0)
    assert counters["archived_files"] == 1
    assert counters["errors"] == 0
    assert not (tmp_path / rj.RUN_JOURNAL_DIR_NAME / "s1" / "r1.jsonl").exists()
    assert _archive_path(tmp_path, "s1", "r1").exists()


def test_non_terminal_run_is_never_archived(tmp_path):
    _write_run(tmp_path, "s1", "r1", terminal=False, mtime_age_days=365)
    counters = _sweep(tmp_path, ttl_days=14, max_runs_per_session=0, max_bytes_per_session=0)
    assert counters["archived_files"] == 0
    assert counters["retained_open"] == 1
    live = tmp_path / rj.RUN_JOURNAL_DIR_NAME / "s1" / "r1.jsonl"
    assert live.exists()


def test_truncated_terminal_row_is_not_terminal(tmp_path):
    """A journal cut mid-row (the only-copy shape) is NOT classified as terminal."""
    _write_run(tmp_path, "s1", "r1", mtime_age_days=365, truncate_terminal=True)
    counters = _sweep(tmp_path, ttl_days=14, max_runs_per_session=0, max_bytes_per_session=0)
    assert counters["archived_files"] == 0
    assert (tmp_path / rj.RUN_JOURNAL_DIR_NAME / "s1" / "r1.jsonl").exists()


def test_terminal_string_inside_payload_does_not_classify(tmp_path):
    """A non-terminal row whose PAYLOAD contains the terminal marker is not terminal."""
    _write_run(
        tmp_path,
        "s1",
        "r1",
        events=[("tool", {"blob": 'x "terminal":true y'})],
        terminal=False,
        mtime_age_days=365,
    )
    counters = _sweep(tmp_path, ttl_days=14, max_runs_per_session=0, max_bytes_per_session=0)
    assert counters["archived_files"] == 0
    assert (tmp_path / rj.RUN_JOURNAL_DIR_NAME / "s1" / "r1.jsonl").exists()


def test_terminal_row_for_another_run_does_not_classify(tmp_path):
    """Terminal row whose run_id/session_id do not match the file is foreign."""
    session_dir = tmp_path / rj.RUN_JOURNAL_DIR_NAME / "s1"
    session_dir.mkdir(parents=True)
    row = {
        "version": 1, "event_id": "other:3", "seq": 3, "run_id": "other",
        "session_id": "s1", "event": "done", "type": "done",
        "created_at": time.time() - 3600, "terminal": True,
        "terminal_state": "completed", "payload": {},
    }
    line = json.dumps(row, separators=(",", ":")) + "\n"
    (session_dir / "r1.jsonl").write_text(line, encoding="utf-8")
    old = time.time() - 365 * 86400
    os.utime(session_dir / "r1.jsonl", (old, old))
    counters = _sweep(tmp_path, ttl_days=14, max_runs_per_session=0, max_bytes_per_session=0)
    assert counters["archived_files"] == 0


def test_settlement_window_defers_recent_runs(tmp_path):
    """A just-settled run is never archived: the writer may still append."""
    _write_run(tmp_path, "s1", "r1", mtime_age_days=0.0)
    counters = _sweep(tmp_path, ttl_days=0.00001, max_runs_per_session=1, max_bytes_per_session=0)
    assert counters["archived_files"] == 0


# ── the archive is byte-exact before the live file is dropped ──────────────


def test_archived_copy_decompresses_to_exact_original_bytes(tmp_path):
    path = _write_run(tmp_path, "s1", "r1", mtime_age_days=30)
    original = path.read_bytes()
    _sweep(tmp_path, ttl_days=14, max_runs_per_session=0, max_bytes_per_session=0)
    with gzip.open(_archive_path(tmp_path, "s1", "r1"), "rb") as fh:
        assert fh.read() == original


def test_archive_written_through_verified_temp_before_live_unlink(tmp_path, monkeypatch):
    """The live file survives when the verify step fails."""
    path = _write_run(tmp_path, "s1", "r1", mtime_age_days=30)
    monkeypatch.setattr(rj, "_archive_reproduces_source", lambda *a, **k: False)
    counters = _sweep(tmp_path, ttl_days=14, max_runs_per_session=0, max_bytes_per_session=0)
    assert counters["archived_files"] == 0
    assert path.exists()
    assert not _archive_path(tmp_path, "s1", "r1").exists()


def test_append_during_classification_aborts_archive(tmp_path, monkeypatch):
    """A file that changes identity after classification is skipped, not archived."""
    path = _write_run(tmp_path, "s1", "r1", mtime_age_days=30)
    real = rj._journal_file_is_terminal

    def append_then_classify(*args, **kwargs):
        result = real(*args, **kwargs)
        with path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps({
                "version": 1, "event_id": "r1:99", "seq": 99, "run_id": "r1",
                "session_id": "s1", "event": "metering", "type": "metering",
                "created_at": time.time(), "terminal": False, "terminal_state": None,
                "payload": {},
            }, separators=(",", ":")) + "\n")
        return result

    monkeypatch.setattr(rj, "_journal_file_is_terminal", append_then_classify)
    counters = _sweep(tmp_path, ttl_days=14, max_runs_per_session=0, max_bytes_per_session=0)
    assert counters["archived_files"] == 0
    assert counters["skipped_files"] == 1
    assert path.exists()


def test_crash_window_archive_is_completed_not_clobbered(tmp_path):
    """Archive published but live unlink never ran (crash): next pass completes it.

    The live journal is append-only, so an existing archive is a PREFIX of the
    current live bytes. A complete-for-the-current-bytes archive is left as-is
    (and the live file dropped); one that no longer reproduces the live bytes
    (the run grew after the crash) is replaced with a freshly verified copy —
    never left as a truncated copy, which would silently lose the tail.
    """
    path = _write_run(tmp_path, "s1", "r1", mtime_age_days=30)
    original = path.read_bytes()
    existing = _archive_path(tmp_path, "s1", "r1")
    existing.parent.mkdir(parents=True)
    # Prefix-only archive: compresses just the first row, so it does NOT
    # reproduce the current live bytes and must be replaced.
    first_row = original.split(b"\n", 1)[0] + b"\n"
    with gzip.open(existing, "wb") as fh:
        fh.write(first_row)

    counters = _sweep(tmp_path, ttl_days=14, max_runs_per_session=0, max_bytes_per_session=0)

    assert counters["archived_files"] == 1
    assert counters["errors"] == 0
    assert not path.exists()  # live file dropped once the archive is complete
    with gzip.open(existing, "rb") as fh:
        assert fh.read() == original  # the FULL bytes, not the truncated prefix


def test_complete_existing_archive_is_kept_and_live_file_dropped(tmp_path):
    """A crash-window archive that already reproduces the live bytes is kept."""
    path = _write_run(tmp_path, "s1", "r1", mtime_age_days=30)
    existing = _archive_path(tmp_path, "s1", "r1")
    existing.parent.mkdir(parents=True)
    with gzip.open(existing, "wb") as fh:
        fh.write(path.read_bytes())
    before = existing.read_bytes()

    counters = _sweep(tmp_path, ttl_days=14, max_runs_per_session=0, max_bytes_per_session=0)

    assert counters["archived_files"] == 1
    assert not path.exists()
    assert existing.read_bytes() == before


# ── reads fall back to the archive transparently ───────────────────────────


def test_latest_run_summary_reads_archived_run(tmp_path):
    _write_run(tmp_path, "s1", "r1", mtime_age_days=30)
    _sweep(tmp_path, ttl_days=14, max_runs_per_session=0, max_bytes_per_session=0)
    summary = rj.latest_run_summary("s1", "r1", session_dir=tmp_path)
    assert summary["terminal"] is True
    assert summary["terminal_state"] == "completed"
    assert summary["event_count"] == 3


def test_read_run_events_reads_archived_run(tmp_path):
    _write_run(tmp_path, "s1", "r1", mtime_age_days=30)
    _sweep(tmp_path, ttl_days=14, max_runs_per_session=0, max_bytes_per_session=0)
    journal = rj.read_run_events("s1", "r1", session_dir=tmp_path)
    assert len(journal["events"]) == 3
    assert journal["events"][-1]["terminal"] is True


def test_find_run_summary_and_find_run_file_see_archived_run(tmp_path):
    _write_run(tmp_path, "s1", "r1", mtime_age_days=30)
    _sweep(tmp_path, ttl_days=14, max_runs_per_session=0, max_bytes_per_session=0)
    summary = rj.find_run_summary("r1", session_dir=tmp_path)
    assert summary is not None and summary["session_id"] == "s1"
    located = rj.find_run_file("r1", session_dir=tmp_path)
    assert located is not None and located[0] == "s1"


def test_session_journal_replay_includes_archived_runs(tmp_path):
    """:func:`read_session_run_events` replays archived + live runs together.

    The cursor points into the ARCHIVED run; its rows must replay alongside the
    live run's rows rather than the archived run silently vanishing from the
    replay window.
    """
    _write_run(tmp_path, "s1", "r1", mtime_age_days=30)
    _write_run(tmp_path, "s1", "r2", mtime_age_days=30 - 1)
    # Count cap only (ttl disabled): r1 is the older run, r2 stays live.
    _sweep(tmp_path, ttl_days=0, max_runs_per_session=1, max_bytes_per_session=0)
    archived = list((tmp_path / rj.RUN_JOURNAL_ARCHIVE_DIR_NAME / "s1").glob("*.jsonl.gz"))
    assert len(archived) == 1
    archived_rid = archived[0].name[: -len(".jsonl.gz")]
    result = rj.read_session_run_events("s1", after_event_id=f"{archived_rid}:1", session_dir=tmp_path)
    assert result["status"] == "ok"
    # The cursor's own run is resolvable even though its live file is gone, and
    # its post-cursor rows replay.
    assert any(
        event["run_id"] == archived_rid and event["seq"] > 1 for event in result["events"]
    )


def test_session_replay_cursor_resolves_via_archive_not_missing(tmp_path):
    """A cursor into an ARCHIVED run must not report ``cursor_run_missing``.

    This is the archive-visibility of the replay path: on the unarchived code
    the run id is unknown (its live file is gone), so the status degrades to
    ``cursor_run_missing``; with archive fallback the cursor resolves.
    """
    _write_run(tmp_path, "s1", "r1", mtime_age_days=30)
    _sweep(tmp_path, ttl_days=0, max_runs_per_session=1, max_bytes_per_session=0)
    _write_run(tmp_path, "s1", "r2", mtime_age_days=1)
    _write_run(tmp_path, "s1", "r3", mtime_age_days=1)
    _sweep(tmp_path, ttl_days=0, max_runs_per_session=1, max_bytes_per_session=0)
    result = rj.read_session_run_events("s1", after_event_id="r1:1", session_dir=tmp_path)
    assert result["status"] == "ok"
    assert result["cursor_run_id"] == "r1"


def test_session_replay_cursor_missing_when_run_fully_absent(tmp_path):
    _write_run(tmp_path, "s1", "r1", mtime_age_days=30)
    result = rj.read_session_run_events("s1", after_event_id="ghost:1", session_dir=tmp_path)
    assert result["status"] == "cursor_run_missing"


def test_live_and_archive_union_when_both_exist(tmp_path):
    """Both copies on disk (crash window / re-created run) read as the union."""
    path = _write_run(tmp_path, "s1", "r1", mtime_age_days=30)
    _sweep(tmp_path, ttl_days=14, max_runs_per_session=0, max_bytes_per_session=0)
    assert not path.exists()
    # Re-create the live path with strictly newer rows.
    with path.open("w", encoding="utf-8") as fh:
        fh.write(json.dumps({
            "version": 1, "event_id": "r1:4", "seq": 4, "run_id": "r1",
            "session_id": "s1", "event": "done", "type": "done",
            "created_at": time.time(), "terminal": True,
            "terminal_state": "completed", "payload": {},
        }, separators=(",", ":")) + "\n")
    journal = rj.read_run_events("s1", "r1", session_dir=tmp_path)
    seqs = [event["seq"] for event in journal["events"]]
    assert seqs == [1, 2, 3, 4]


# ── caps: age, count, and size ─────────────────────────────────────────────


def test_ttl_cap_only_archives_old_runs(tmp_path):
    _write_run(tmp_path, "s1", "old", mtime_age_days=30)
    _write_run(tmp_path, "s1", "recent", mtime_age_days=1)
    counters = _sweep(tmp_path, ttl_days=14, max_runs_per_session=0, max_bytes_per_session=0)
    assert counters["archived_files"] == 1
    assert (tmp_path / rj.RUN_JOURNAL_DIR_NAME / "s1" / "recent.jsonl").exists()
    assert _archive_path(tmp_path, "s1", "old").exists()


def test_count_cap_keeps_newest_runs(tmp_path):
    for index, rid in enumerate(["r1", "r2", "r3"]):
        _write_run(tmp_path, "s1", rid, mtime_age_days=30 - index)
    counters = _sweep(tmp_path, ttl_days=0, max_runs_per_session=1, max_bytes_per_session=0)
    assert counters["archived_files"] == 2
    assert (tmp_path / rj.RUN_JOURNAL_DIR_NAME / "s1" / "r3.jsonl").exists()


def test_size_cap_archives_oldest_first_and_keeps_newest(tmp_path):
    for index, rid in enumerate(["r1", "r2", "r3"]):
        _write_run(tmp_path, "s1", rid, mtime_age_days=30 - index)
    one_size = (tmp_path / rj.RUN_JOURNAL_DIR_NAME / "s1" / "r1.jsonl").stat().st_size
    counters = _sweep(
        tmp_path, ttl_days=0, max_runs_per_session=0, max_bytes_per_session=one_size + 10
    )
    assert counters["archived_files"] == 2
    assert (tmp_path / rj.RUN_JOURNAL_DIR_NAME / "s1" / "r3.jsonl").exists()


def test_zero_caps_disable_their_cap(tmp_path):
    _write_run(tmp_path, "s1", "r1", mtime_age_days=365)
    counters = _sweep(tmp_path, ttl_days=0, max_runs_per_session=0, max_bytes_per_session=0)
    assert counters["archived_files"] == 0


def test_caps_resolve_from_env_over_settings(tmp_path, monkeypatch):
    monkeypatch.setenv(rj._RETENTION_TTL_ENV, "3")
    _write_run(tmp_path, "s1", "r1", mtime_age_days=10)
    counters = _sweep(tmp_path, max_runs_per_session=0, max_bytes_per_session=0)
    assert counters["caps"]["ttl_days"] == 3.0
    assert counters["archived_files"] == 1


# ── gating: kill switch, schedule, single-flight ───────────────────────────


def test_sweep_disabled_by_env(monkeypatch):
    monkeypatch.setenv(rj.RUN_JOURNAL_SWEEP_ENV, "0")
    assert rj.run_journal_sweep_enabled() is False
    assert rj.maybe_sweep_run_journal() is None
    monkeypatch.setenv(rj.RUN_JOURNAL_SWEEP_ENV, "1")
    assert rj.run_journal_sweep_enabled() is True


def test_maybe_sweep_delays_first_pass_and_then_honours_interval(tmp_path, monkeypatch):
    rj._reset_run_journal_sweep_schedule()
    calls: list[float] = []
    monkeypatch.setattr(rj, "sweep_run_journal", lambda **kwargs: calls.append(1) or {"ok": True})
    base = 1_000_000.0
    assert rj.maybe_sweep_run_journal(now=base) is None  # arms the boot delay
    assert rj.maybe_sweep_run_journal(now=base + rj.RETENTION_FIRST_SWEEP_DELAY_SECS - 1) is None
    result = rj.maybe_sweep_run_journal(now=base + rj.RETENTION_FIRST_SWEEP_DELAY_SECS + 1)
    assert result == {"ok": True}
    assert rj.maybe_sweep_run_journal(now=base + rj.RETENTION_FIRST_SWEEP_DELAY_SECS + 2) is None
    result = rj.maybe_sweep_run_journal(
        now=base + rj.RETENTION_FIRST_SWEEP_DELAY_SECS + rj.RETENTION_SWEEP_INTERVAL_SECS + 2
    )
    assert result == {"ok": True}
    assert len(calls) == 2


def test_sweep_skipped_entirely_without_dir_fd_support(tmp_path, monkeypatch):
    _write_run(tmp_path, "s1", "r1", mtime_age_days=30)
    monkeypatch.setattr(rj, "_DIR_FD_OK", False)
    counters = _sweep(tmp_path, ttl_days=14, max_runs_per_session=0, max_bytes_per_session=0)
    assert counters["archived_files"] == 0
    assert (tmp_path / rj.RUN_JOURNAL_DIR_NAME / "s1" / "r1.jsonl").exists()


# ── hygiene: symlinks, ids, temps, archive pruning ─────────────────────────


def test_symlinked_session_dir_is_skipped(tmp_path):
    _write_run(tmp_path, "s1", "r1", mtime_age_days=30)
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "evil.jsonl").write_text(
        json.dumps({
            "version": 1, "event_id": "evil:1", "seq": 1, "run_id": "evil",
            "session_id": "s2", "event": "done", "type": "done",
            "created_at": time.time(), "terminal": True,
            "terminal_state": "completed", "payload": {},
        }, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    journal_root = tmp_path / rj.RUN_JOURNAL_DIR_NAME
    (journal_root / "s2").symlink_to(outside)
    counters = _sweep(tmp_path, ttl_days=14, max_runs_per_session=0, max_bytes_per_session=0)
    assert counters["archived_files"] == 1  # only the real s1 run
    assert (outside / "evil.jsonl").exists()
    assert not (outside / "evil.jsonl.gz").exists()


def test_weird_session_and_run_names_are_skipped(tmp_path):
    journal_root = tmp_path / rj.RUN_JOURNAL_DIR_NAME
    weird_dir = journal_root / "bad name"
    weird_dir.mkdir(parents=True)
    _write_run(tmp_path, "s1", "r1", mtime_age_days=30)
    (weird_dir / "x.jsonl").write_text("", encoding="utf-8")
    counters = _sweep(tmp_path, ttl_days=14, max_runs_per_session=0, max_bytes_per_session=0)
    assert counters["errors"] == 0
    assert counters["archived_files"] == 1


def test_stray_temp_files_are_cleaned(tmp_path):
    _write_run(tmp_path, "s1", "r1", mtime_age_days=30)
    archive_session = tmp_path / rj.RUN_JOURNAL_ARCHIVE_DIR_NAME / "s1"
    archive_session.mkdir(parents=True)
    stray = archive_session / ".r1.jsonl.gz.tmp.999999"
    stray.write_bytes(b"partial")
    _sweep(tmp_path, ttl_days=14, max_runs_per_session=0, max_bytes_per_session=0)
    assert not stray.exists()
    assert _archive_path(tmp_path, "s1", "r1").exists()


def test_archive_pruning_disabled_by_default(tmp_path):
    _write_run(tmp_path, "s1", "r1", mtime_age_days=800)
    _sweep(tmp_path, ttl_days=14, max_runs_per_session=0, max_bytes_per_session=0)
    assert _archive_path(tmp_path, "s1", "r1").exists()
    counters = _sweep(tmp_path, ttl_days=0, max_runs_per_session=0, max_bytes_per_session=0)
    assert counters["pruned_archives"] == 0
    assert _archive_path(tmp_path, "s1", "r1").exists()


def test_archive_pruning_deletes_only_past_archive_ttl(tmp_path, monkeypatch):
    _write_run(tmp_path, "s1", "r1", mtime_age_days=800)
    _sweep(tmp_path, ttl_days=14, max_runs_per_session=0, max_bytes_per_session=0)
    archive = _archive_path(tmp_path, "s1", "r1")
    old = time.time() - 400 * 86400
    os.utime(archive, (old, old))
    monkeypatch.setenv(rj._RETENTION_ARCHIVE_TTL_ENV, "90")
    _write_run(tmp_path, "s1", "r2", mtime_age_days=800)
    # r2's archive is fresh; r1's is 400 days old and must be pruned.
    rj.sweep_run_journal(session_dir=tmp_path, ttl_days=14, max_runs_per_session=0, max_bytes_per_session=0)
    assert not archive.exists()
    assert _archive_path(tmp_path, "s1", "r2").exists()


def test_delete_run_journal_removes_archives_too(tmp_path):
    """Deleting a session must not leave recoverable payloads in the archive."""
    _write_run(tmp_path, "s1", "r1", mtime_age_days=30)
    _write_run(tmp_path, "s1", "r2", mtime_age_days=30)
    _sweep(tmp_path, ttl_days=14, max_runs_per_session=0, max_bytes_per_session=0)
    assert _archive_path(tmp_path, "s1", "r1").exists()
    _write_run(tmp_path, "s2", "keep", mtime_age_days=30)
    _sweep(tmp_path, ttl_days=14, max_runs_per_session=0, max_bytes_per_session=0)
    assert _archive_path(tmp_path, "s2", "keep").exists()

    rj.delete_run_journal("s1", session_dir=tmp_path)

    assert not (tmp_path / rj.RUN_JOURNAL_DIR_NAME / "s1").exists()
    assert not (tmp_path / rj.RUN_JOURNAL_ARCHIVE_DIR_NAME / "s1").exists()
    # A sibling session is untouched.
    assert _archive_path(tmp_path, "s2", "keep").exists()


def test_sweep_on_missing_root_is_a_noop(tmp_path):
    counters = _sweep(tmp_path / "nope", ttl_days=14, max_runs_per_session=0, max_bytes_per_session=0)
    assert counters["archived_files"] == 0
    assert counters["errors"] == 0


# ── containment: archived reads must never escape the archive root ─────────


def _symlinked_archive_dir_with_external_run(root: Path, sid: str, rid: str) -> Path:
    """Point ``_run_journal_archive/<sid>`` at an EXTERNAL dir holding ``<rid>.jsonl.gz``.

    Mirrors the escape reported on the PR: archive discovery used to follow the
    symlinked session directory, so the external gzip was read as journal data.
    """
    outside = root.parent / f"{root.name}-outside-{sid}"
    outside.mkdir(parents=True, exist_ok=True)
    body = (
        json.dumps(
            {
                "version": 1,
                "event_id": f"{rid}:1",
                "seq": 1,
                "run_id": rid,
                "session_id": sid,
                "event": "token",
                "type": "token",
                "created_at": time.time() - 3600,
                "terminal": True,
                "terminal_state": "completed",
                "payload": {"text": "EXTERNAL"},
            },
            separators=(",", ":"),
        )
        + "\n"
    )
    with gzip.open(outside / f"{rid}.jsonl.gz", "wb") as fh:
        fh.write(body.encode("utf-8"))
    archive_root = root / rj.RUN_JOURNAL_ARCHIVE_DIR_NAME
    archive_root.mkdir(parents=True, exist_ok=True)
    (archive_root / sid).symlink_to(outside, target_is_directory=True)
    return outside


def test_symlinked_archive_session_dir_is_not_read(tmp_path):
    """A symlinked archive session dir must never serve journal data (fail closed)."""
    _symlinked_archive_dir_with_external_run(tmp_path, "s1", "evil")

    assert rj.find_run_summary("evil", session_dir=tmp_path) is None
    assert rj.find_run_file("evil", session_dir=tmp_path) is None
    # `read_run_events`/`latest_run_summary` return an empty shape (not the
    # external rows) when the only copy is behind an untrusted symlink.
    result = rj.read_run_events("s1", "evil", session_dir=tmp_path)
    assert result["events"] == []
    summary = rj.latest_run_summary("s1", "evil", session_dir=tmp_path)
    assert not (summary and summary.get("event_count"))
    assert rj._read_jsonl(tmp_path / rj.RUN_JOURNAL_DIR_NAME / "s1" / "evil.jsonl")[0] == []


def test_symlinked_archive_session_dir_not_in_replay(tmp_path):
    """Session replay must not include a run reachable only through a symlinked archive dir."""
    _symlinked_archive_dir_with_external_run(tmp_path, "s1", "evil")
    _write_run(tmp_path, "s1", "ok", mtime_age_days=30)

    replay = rj.read_session_run_events("s1", after_event_id="ok:1", session_dir=tmp_path)
    run_ids = {event.get("run_id") for event in replay.get("events", [])}
    assert "evil" not in run_ids


def test_archive_read_falls_back_when_live_path_is_symlinked(tmp_path):
    """A legitimate archive still reads when the ARCHIVE dir is a real directory."""
    _write_run(tmp_path, "s1", "r1", mtime_age_days=30)
    _sweep(tmp_path, ttl_days=14, max_runs_per_session=0, max_bytes_per_session=0)
    assert _archive_path(tmp_path, "s1", "r1").exists()

    summary = rj.latest_run_summary("s1", "r1", session_dir=tmp_path)
    assert summary is not None and summary.get("run_id") == "r1"


def test_archive_read_race_does_not_leak_external_file(tmp_path, monkeypatch):
    """A swap between the containment check and the open must not leak data.

    Reproduces the review finding: `_archive_read_allowed` validated the path,
    then `gzip.open()` re-resolved that mutable pathname — a symlink planted in
    between made the read follow it out of the archive tree. Reads now open
    through pinned directory handles (`O_NOFOLLOW` + `dir_fd`), so the swap
    cannot redirect the read.
    """
    _write_run(tmp_path, "s1", "r1", mtime_age_days=30)
    _sweep(tmp_path, ttl_days=14, max_runs_per_session=0, max_bytes_per_session=0)
    archive = _archive_path(tmp_path, "s1", "r1")
    assert archive.exists()

    # External gzip with a sentinel payload, then the attacker's swap.
    external = tmp_path.parent / "external-evil.jsonl.gz"
    with gzip.open(external, "wb") as fh:
        fh.write(_external_row_bytes("s1", "r1"))

    real_open = rj._open_archive_entry
    swap = {"done": False}

    def open_with_swap(path):
        if not swap["done"] and Path(path) == archive:
            swap["done"] = True
            archive.unlink()
            archive.symlink_to(external)
        return real_open(path)

    monkeypatch.setattr(rj, "_open_archive_entry", open_with_swap)
    text = rj._read_gz_text(archive)
    monkeypatch.undo()

    assert text is None or "EXTERNAL" not in text, "external file was served as archive data"


def test_archive_read_race_via_run_events_does_not_leak(tmp_path, monkeypatch):
    """The same swap must not leak through the read_run_events path either."""
    _write_run(tmp_path, "s1", "r1", mtime_age_days=30)
    _sweep(tmp_path, ttl_days=14, max_runs_per_session=0, max_bytes_per_session=0)
    archive = _archive_path(tmp_path, "s1", "r1")

    external = tmp_path.parent / "external-evil2.jsonl.gz"
    with gzip.open(external, "wb") as fh:
        fh.write(_external_row_bytes("s1", "r1"))

    real_open = rj._open_archive_entry
    swap = {"done": False}

    def open_with_swap(path):
        if not swap["done"] and Path(path) == archive:
            swap["done"] = True
            archive.unlink()
            archive.symlink_to(external)
        return real_open(path)

    monkeypatch.setattr(rj, "_open_archive_entry", open_with_swap)
    result = rj.read_run_events("s1", "r1", session_dir=tmp_path)
    monkeypatch.undo()

    payloads = [json.dumps(event.get("payload", {})) for event in result["events"]]
    assert not any("EXTERNAL" in p for p in payloads)


def _external_row_bytes(sid: str, rid: str) -> bytes:
    return (
        json.dumps(
            {
                "version": 1,
                "event_id": f"{rid}:99",
                "seq": 99,
                "run_id": rid,
                "session_id": sid,
                "event": "token",
                "type": "token",
                "created_at": time.time(),
                "terminal": True,
                "terminal_state": "completed",
                "payload": {"text": "EXTERNAL"},
            },
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


# ── archive root must never be followed (sweep / delete / prune) ────────────


def _symlink_archive_root(root: Path, external: Path) -> None:
    """Point ``_run_journal_archive`` at an EXTERNAL directory (symlink root)."""
    external.mkdir(parents=True, exist_ok=True)
    (root / rj.RUN_JOURNAL_ARCHIVE_DIR_NAME).symlink_to(external, target_is_directory=True)


def test_sweep_skips_archival_when_archive_root_is_symlinked(tmp_path):
    """A symlinked archive ROOT must not receive archives or lose the live file.

    Following it would move the run outside the journal tree — where the
    (correctly) containment-checked readers refuse it — so recovery would
    silently see zero events for a run whose live file was removed.
    """
    path = _write_run(tmp_path, "s1", "r1", mtime_age_days=30)
    external = tmp_path.parent / f"{tmp_path.name}-external-root"
    _symlink_archive_root(tmp_path, external)

    counters = _sweep(tmp_path, ttl_days=14, max_runs_per_session=0, max_bytes_per_session=0)

    assert counters["archived_files"] == 0, "archived into a symlinked root"
    assert path.exists(), "live file removed although the archive root was untrusted"
    assert list(external.rglob("*.jsonl.gz")) == [], "files written into the external dir"
    # Recovery still works: the run is live and fully readable.
    events = rj.read_run_events("s1", "r1", session_dir=tmp_path)
    assert len(events["events"]) > 0


def test_delete_run_journal_does_not_follow_symlinked_archive_root(tmp_path):
    """Session deletion must not remove a foreign directory via a symlinked root."""
    _write_run(tmp_path, "s1", "r1", mtime_age_days=30)
    external = tmp_path.parent / f"{tmp_path.name}-external-del"
    (external / "s1").mkdir(parents=True)
    precious = external / "s1" / "PRECIOUS.txt"
    precious.write_text("must survive", encoding="utf-8")
    _symlink_archive_root(tmp_path, external)

    rj.delete_run_journal("s1", session_dir=tmp_path)

    assert precious.exists(), "external file deleted through a symlinked archive root"
    assert (external / "s1").is_dir()


def test_prune_does_not_follow_symlinked_archive_root(tmp_path, monkeypatch):
    """Archive pruning must not delete files outside the journal tree."""
    _write_run(tmp_path, "s1", "keep", mtime_age_days=0)
    external = tmp_path.parent / f"{tmp_path.name}-external-prune"
    (external / "s1").mkdir(parents=True)
    victim = external / "s1" / "victim.jsonl.gz"
    with gzip.open(victim, "wb") as fh:
        fh.write(b'{"version":1}\n')
    old = time.time() - 400 * 86400
    os.utime(victim, (old, old))
    _symlink_archive_root(tmp_path, external)

    monkeypatch.setenv(rj._RETENTION_ARCHIVE_TTL_ENV, "90")
    counters = _sweep(tmp_path, ttl_days=0, max_runs_per_session=0, max_bytes_per_session=0)
    monkeypatch.undo()

    assert counters["pruned_archives"] == 0
    assert victim.exists(), "external archive pruned through a symlinked root"


def test_prune_skips_symlinked_session_dir_inside_real_root(tmp_path, monkeypatch):
    """A symlinked SESSION dir inside a real archive root is not pruned through."""
    _write_run(tmp_path, "s1", "keep", mtime_age_days=0)
    external = tmp_path.parent / f"{tmp_path.name}-external-session"
    external.mkdir(parents=True, exist_ok=True)
    victim = external / "victim.jsonl.gz"
    with gzip.open(victim, "wb") as fh:
        fh.write(b'{"version":1}\n')
    old = time.time() - 400 * 86400
    os.utime(victim, (old, old))
    archive_root = tmp_path / rj.RUN_JOURNAL_ARCHIVE_DIR_NAME
    archive_root.mkdir(parents=True, exist_ok=True)
    (archive_root / "s2").symlink_to(external, target_is_directory=True)

    monkeypatch.setenv(rj._RETENTION_ARCHIVE_TTL_ENV, "90")
    counters = _sweep(tmp_path, ttl_days=0, max_runs_per_session=0, max_bytes_per_session=0)
    monkeypatch.undo()

    assert counters["pruned_archives"] == 0
    assert victim.exists(), "pruned through a symlinked session dir"


# ── durability: a failed fsync must keep the live file ─────────────────────


def test_failed_archive_dir_fsync_keeps_live_file(tmp_path, monkeypatch):
    """The live file is the only copy until the archive is durably synced.

    Reproduces the gate finding: a failed archive-directory fsync was ignored
    and the live file was then removed, so a crash at that point could lose the
    run's only durable copy.
    """
    path = _write_run(tmp_path, "s1", "r1", mtime_age_days=30)
    # Pre-create the archive dir so the dir-chain sync is not the thing tested.
    (tmp_path / rj.RUN_JOURNAL_ARCHIVE_DIR_NAME / "s1").mkdir(parents=True)

    real_fsync = os.fsync

    def failing_dir_fsync(fd):
        import stat as _stat

        if _stat.S_ISDIR(os.fstat(fd).st_mode):
            raise OSError("injected dir fsync failure")
        return real_fsync(fd)

    monkeypatch.setattr(rj.os, "fsync", failing_dir_fsync)
    counters = _sweep(tmp_path, ttl_days=14, max_runs_per_session=0, max_bytes_per_session=0)
    monkeypatch.undo()

    assert path.exists(), "live file removed although the archive fsync failed"
    assert counters["archived_files"] == 0
    # The run is still fully readable from the live copy.
    events = rj.read_run_events("s1", "r1", session_dir=tmp_path)
    assert len(events["events"]) > 0


# ── ownership: the pinned raw handle must close on every exit path ──────────


def _archived_run_with_held_raw(tmp_path, monkeypatch):
    """Archive one run and return (live_style_path, raws_list, real_open).

    ``raws_list`` receives a STRONG reference to every raw handle opened by
    ``_open_archive_entry``. Keeping the reference alive is the point: it stops
    CPython refcount finalization from masking a missing explicit close.
    """
    _write_run(tmp_path, "s1", "r1", mtime_age_days=30,
               events=[("token", {"text": f"line-{i}"}) for i in range(12)])
    _sweep(tmp_path, ttl_days=14, max_runs_per_session=0, max_bytes_per_session=0)
    assert _archive_path(tmp_path, "s1", "r1").exists()

    raws: list = []
    real_open = rj._open_archive_entry

    def spy_open(path):
        fh = real_open(path)
        if fh is not None:
            raws.append(fh)
        return fh

    monkeypatch.setattr(rj, "_open_archive_entry", spy_open)
    return tmp_path / rj.RUN_JOURNAL_DIR_NAME / "s1" / "r1.jsonl", raws


def test_streaming_archive_read_closes_pinned_handle_on_completion(tmp_path, monkeypatch):
    """Full consumption of the bounded iterator must close the pinned raw handle.

    GzipFile.close() does not close a caller-supplied fileobj, so wrapping a
    pinned descriptor without owning it leaves closure to refcount finalization.
    The holder list keeps the raw handle strongly referenced, so only an
    explicit close can satisfy this.
    """
    path, raws = _archived_run_with_held_raw(tmp_path, monkeypatch)
    lines = list(rj._iter_bounded_raw_jsonl_lines(path, max_bytes=10_000_000))
    monkeypatch.undo()

    assert lines, "iterator yielded nothing"
    assert raws, "_open_archive_entry was not used"
    assert all(raw.closed for raw in raws), "pinned raw handle left open after full consumption"


def test_streaming_archive_read_closes_pinned_handle_on_limit_exception(tmp_path, monkeypatch):
    """A replay-limit ValueError mid-iteration must still close the raw handle."""
    path, raws = _archived_run_with_held_raw(tmp_path, monkeypatch)
    with pytest.raises(ValueError):
        list(rj._iter_bounded_raw_jsonl_lines(path, max_bytes=16))
    monkeypatch.undo()

    assert raws, "_open_archive_entry was not used"
    assert all(raw.closed for raw in raws), "pinned raw handle left open after replay-limit raise"


def test_streaming_archive_read_closes_pinned_handle_on_generator_close(tmp_path, monkeypatch):
    """Abandoning the generator (close() without exhaustion) must close the raw handle."""
    path, raws = _archived_run_with_held_raw(tmp_path, monkeypatch)
    iterator = rj._iter_bounded_raw_jsonl_lines(path, max_bytes=10_000_000)
    next(iterator)
    iterator.close()
    monkeypatch.undo()

    assert raws, "_open_archive_entry was not used"
    assert all(raw.closed for raw in raws), "pinned raw handle left open after generator close"


# ── pruning: a replacement archive must never be pruned ─────────────────────


def test_archive_pruning_does_not_delete_replacement_archive(tmp_path, monkeypatch):
    """Pruning claims the entry: a replacement published mid-prune survives.

    Reproduces the PR finding: the prune stat()ed an entry's age, then unlinked
    the mutable NAME later. A writer that republished that name between the two
    steps lost its newly written archive (the only retained copy). The fix
    claims the entry by rename, verifies the claim, and on a mismatch restores
    it without clobbering the canonical name.
    """
    _write_run(tmp_path, "s1", "r1", mtime_age_days=800)
    _sweep(tmp_path, ttl_days=14, max_runs_per_session=0, max_bytes_per_session=0)
    archive = _archive_path(tmp_path, "s1", "r1")
    assert archive.exists()
    old = time.time() - 400 * 86400
    os.utime(archive, (old, old))

    # Simulate the race in the widest window: replace the entry at the moment
    # the prune CLAIMS it (after the age check, before the identity verify).
    fresh_body = b"FRESH REPLACEMENT ARCHIVE"
    real_rename = os.rename
    swapped = {"done": False}

    def swap_then_claim(src, dst, **kwargs):
        if not swapped["done"] and isinstance(src, str) and src.endswith(".jsonl.gz"):
            swapped["done"] = True
            archive.unlink()  # the aged entry the checker saw
            archive.write_bytes(fresh_body)  # a NEW archive published at that name
        return real_rename(src, dst, **kwargs)

    monkeypatch.setattr(rj.os, "rename", swap_then_claim)
    monkeypatch.setenv(rj._RETENTION_ARCHIVE_TTL_ENV, "90")
    counters = rj.sweep_run_journal(
        session_dir=tmp_path, ttl_days=0, max_runs_per_session=0, max_bytes_per_session=0
    )
    monkeypatch.undo()

    assert archive.exists(), "replacement archive was deleted"
    assert archive.read_bytes() == fresh_body
    assert counters["pruned_archives"] == 0
    # No claim debris left behind.
    claims = list(archive.parent.glob("*.prune-claim.*"))
    assert claims == []