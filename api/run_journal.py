"""Append-only WebUI run event journal helpers.

This is the first #1925 journal/replay slice.  It mirrors SSE events emitted by
the existing in-process streaming path without changing execution ownership.
"""
from __future__ import annotations

import gzip
import json
import logging
import os
import re
import shutil
import stat
import threading
import time
from copy import deepcopy
from collections import OrderedDict
from pathlib import Path
from typing import Any, Iterable

logger = logging.getLogger(__name__)

RUN_JOURNAL_DIR_NAME = "_run_journal"
# Archived runs live in a sibling directory as ``<sid>/<run_id>.jsonl.gz``.
# Retention never unlinks a run: it compresses and MOVES the live file here, so
# a wrong retention decision costs one compressed copy instead of the data.
RUN_JOURNAL_ARCHIVE_DIR_NAME = "_run_journal_archive"
_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
_WRITER_LOCKS: dict[tuple[str, str, str], threading.Lock] = {}
_WRITER_LOCKS_GUARD = threading.Lock()
# Next-seq to assign per run-journal file path, kept in memory so repeat appends
# to the same run do not re-parse the whole file on every call. The per-path
# ``_lock_for(path)`` serializes same-path reserve→append so seqs stay monotonic
# and file order matches; ``_SEQ_CACHE_LOCK`` (below) additionally guards every
# *structural* access to the dict (reserve/note/evict) so ``delete_run_journal``
# can iterate + drop keys while a concurrent append on ANOTHER path inserts one,
# without a ``dictionary changed size during iteration`` crash. See
# ``_reserve_next_seq`` and ``delete_run_journal`` (which evicts stale entries).
_SEQ_CACHE: dict[str, int] = {}
_SEQ_CACHE_LOCK = threading.Lock()
# Summary callers only need terminal state and the latest cursor. Re-parsing a
# completed journal's full payload (which can include multi-megabyte tool or
# session results) on every status/reconnect probe is needless. This process
# cache is keyed by a complete stat identity, so it is never used after an
# atomic replacement, append, truncate, or same-path file recreation.
_SUMMARY_CACHE_MAX_ENTRIES = 128
_SUMMARY_CACHE: OrderedDict[str, tuple[tuple[int, int, int, int, int], dict]] = OrderedDict()
_SUMMARY_CACHE_LOCK = threading.Lock()
# Events that mark a run terminal in the journal / summary sense.
TERMINAL_SSE_EVENTS = frozenset({"done", "cancel", "apperror", "error", "stream_end"})
# Events that should close an SSE relay drain loop. `done` is intentionally
# excluded: background title generation and `stream_end` are emitted after
# `done`, and breaking early would drop them. `apperror` is included because
# it terminates with no trailing `stream_end`.
SSE_RELAY_CLOSE_EVENTS = frozenset({"stream_end", "cancel", "apperror", "error"})
# Back-compat alias used by older call sites / tests.
_TERMINAL_SSE_EVENTS = TERMINAL_SSE_EVENTS
# Events that are live-UI-only telemetry with no recovery value in the run
# journal. They are skipped at WRITE time (never durably journaled, so they
# cannot bloat the journal on marathon runs) and filtered at REPLAY time (so
# legacy journals that already contain a backlog never stream it to a
# reconnecting browser tab). Readers deliberately do NOT filter: cursor math
# (``cursor_event_missing`` bound) and the offline-gap coverage check count
# journal seqs and must keep seeing every row.
REPLAY_SKIPPED_SSE_EVENTS = frozenset({"metering"})
_FSYNC_MODE_ENV = "HERMES_WEBUI_RUN_JOURNAL_FSYNC"
_FSYNC_MODE_EAGER = "eager"
_FSYNC_MODE_TERMINAL_ONLY = "terminal-only"
_SESSION_REPLAY_MAX_BYTES = 4 * 1024 * 1024
_SESSION_REPLAY_MAX_ROWS = 4096
_SESSION_REPLAY_READ_CHUNK_BYTES = 64 * 1024
_SNAPSHOT_ARGS_MAX_ITEMS = 64
_SNAPSHOT_ARGS_MAX_DEPTH = 8
_SNAPSHOT_ARGS_MAX_STRING_CHARS = 8192
_SNAPSHOT_ARGS_MAX_TOTAL_CHARS = 64 * 1024
_SNAPSHOT_ARGS_TRUNCATED_SUFFIX = "...[truncated]"

# ── Archival retention (#7613) ──────────────────────────────────────────────
# `delete_run_journal` has exactly one call site — session deletion — so a
# long-lived or pinned session accumulates one `{run_id}.jsonl` per run forever
# (measured at 916 MB of completed-run logs on the reporter's install, 6.28 GB
# on another). Retention therefore needs a periodic pass.
#
# It ARCHIVES rather than unlinks. Deletion gated on inferred durability (is
# this run settled? was the session persisted? does the sidecar still reference
# it?) proved to have too many ways to be wrong, and every miss was data loss.
# Compressing and moving the file costs one compressed copy when the
# classification is wrong, and the read paths fall back to the archive, so even
# a false positive stays recoverable.
#
#   * `ttl_days`              — run file's own mtime older than the TTL
#   * `max_runs_per_session`  — beyond the newest N runs in the session
#   * `max_bytes_per_session` — retained live bytes beyond the session budget
#
# The caps only decide WHEN to archive. Nothing here is a correctness cliff.
RUN_JOURNAL_SWEEP_ENV = "HERMES_WEBUI_RUN_JOURNAL_SWEEP"
_RETENTION_TTL_ENV = "HERMES_WEBUI_RUN_JOURNAL_RETENTION_TTL_DAYS"
_RETENTION_MAX_RUNS_ENV = "HERMES_WEBUI_RUN_JOURNAL_RETENTION_MAX_RUNS_PER_SESSION"
_RETENTION_MAX_BYTES_ENV = "HERMES_WEBUI_RUN_JOURNAL_RETENTION_MAX_BYTES_PER_SESSION"
_RETENTION_TTL_SETTING = "run_journal_retention_ttl_days"
_RETENTION_MAX_RUNS_SETTING = "run_journal_retention_max_runs_per_session"
_RETENTION_MAX_BYTES_SETTING = "run_journal_retention_max_bytes_per_session"
# Archive pruning is the ONLY destructive step, and runs on its own, much
# longer clock. 0 = never prune (archives are kept forever).
_RETENTION_ARCHIVE_TTL_ENV = "HERMES_WEBUI_RUN_JOURNAL_ARCHIVE_TTL_DAYS"
_RETENTION_ARCHIVE_TTL_SETTING = "run_journal_archive_ttl_days"
DEFAULT_RUN_JOURNAL_RETENTION_TTL_DAYS = 14.0
DEFAULT_RUN_JOURNAL_RETENTION_MAX_RUNS_PER_SESSION = 40
DEFAULT_RUN_JOURNAL_RETENTION_MAX_BYTES_PER_SESSION = 256 * 1024 * 1024
DEFAULT_RUN_JOURNAL_ARCHIVE_TTL_DAYS = 0.0
_RETENTION_TTL_MAX_DAYS = 3650.0
_RETENTION_MAX_RUNS_LIMIT = 100_000
_RETENTION_MAX_BYTES_LIMIT = 100 * 1024 * 1024 * 1024  # 100 GiB
_SWEEP_DISABLED_VALUES = frozenset({"0", "false", "no", "off", "disabled", "none"})
# Minimum spacing between retention sweeps when driven by the shared
# maintenance tick (``api.background_process._reaper_loop`` via
# ``maybe_sweep_run_journal``). The tick itself fires far more often; this gate
# keeps the sweep hourly and, on a fresh process, delays the first pass so boot
# never competes with a large scan. State lives in the tick owner, not here.
RETENTION_SWEEP_INTERVAL_SECS = 3600.0
RETENTION_FIRST_SWEEP_DELAY_SECS = 60.0
# A run file must be untouched for this long before ANY cap can archive it.
# The run's writer can append post-terminal rows (metering / stream_end /
# title generation arrive around the terminal row), and a just-settled run may
# still be replayed to a reconnecting client; both want a settlement window.
_RETENTION_MIN_QUIESCENT_SECONDS = 3600.0
# Per-sweep cap on bytes handed to the archiver, so one tick never stalls on a
# large backlog (a 6 GB journal drains over several hourly passes instead of
# blocking the reaper). Only limits how fast we archive, never what we archive.
_RETENTION_ARCHIVE_BYTES_PER_SWEEP = 512 * 1024 * 1024
# gzip level: 6 keeps the compressor fast (~58 MB/s measured on real journals)
# for an 11% final size; the sweeper is off the request path, but it still must
# not monopolise a core in a small container.
_ARCHIVE_GZIP_LEVEL = 6
# Bounded row-window read used by terminal classification. Walks the file
# backwards and stops at the first VERIFIED terminal row: the common case
# (terminal row near EOF) reads one chunk; a multi-MB single row (giant
# `apperror` payload) needs the walk to cross its payload to reach its own
# prefix. Files whose terminal row sits further back than this budget are
# treated as non-terminal (fail closed — a missed archive is safe).
_RETENTION_VERIFY_MAX_BYTES = 64 * 1024 * 1024
_RETENTION_VERIFY_CHUNK_BYTES = 256 * 1024
_RETENTION_MARKER_BYTES = b'"terminal":true'
# A serialized run row always starts with this exact byte prefix (compact
# separators; key order fixed by `append_run_event`; `json.dumps` escapes
# quotes inside string values, and ids are restricted to [A-Za-z0-9_.-], so
# these bytes can only occur at a row start).
_RETENTION_ROW_START_BYTES = b'{"version":1,"event_id":"'
# Serialized run-row header, anchored at the row start (``re.match``).
# Groups: 1 = event_id, 2 = seq, 3 = run_id, 4 = session_id, 5 = terminal flag.
# Mirrors `append_run_event`'s compact serialization exactly; `created_at`
# permits the full float character set `json.dumps` can emit.
_RETENTION_HEADER_RE = re.compile(
    rb'\{"version":1,'
    rb'"event_id":"([^"\\]{1,300})",'
    rb'"seq":(\d{1,20}),'
    rb'"run_id":"([^"\\]{1,300})",'
    rb'"session_id":"([^"\\]{1,300})",'
    rb'"event":"[^"\\]{0,300}",'
    rb'"type":"[^"\\]{0,300}",'
    rb'"created_at":[-0-9.eE+]{1,64},'
    rb'"terminal":(true|false),'
)
# How far back a candidate marker may hunt for its row start. A genuine
# terminal row carries the marker in its first ~500 bytes; a nested
# `"terminal":true` inside a payload is rejected by the ownership-checked
# header parse (see `_verify_terminal_row_at`).
_RETENTION_ROW_START_HUNT_BYTES = 64 * 1024
_RETENTION_HEADER_MAX_BYTES = 8 * 1024
# dir_fd + O_NOFOLLOW primitives (Linux/macOS) — the same pattern
# `api/workspace.py` uses. Where they are unavailable (Windows) the sweep is
# disabled entirely rather than falling back to path-based moves that a
# directory swap can redirect.
_DIR_FD_OK = os.open in getattr(os, "supports_dir_fd", set())
_O_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)
_O_DIRECTORY = getattr(os, "O_DIRECTORY", 0)
# Sweep scheduling state, shared by the maintenance tick and manual callers.
# ``None`` = this process has not yet armed its first-pass delay;
# otherwise it is the wall-clock time the last sweep was started.
_SWEEP_LAST_STARTED: float | None = None
_SWEEP_THREAD_LOCK = threading.Lock()
# Serializes sweep bodies: the maintenance tick and any explicit caller never
# scan (and archive) concurrently.
_SWEEP_RUN_LOCK = threading.Lock()


def _default_session_dir() -> Path:
    from api.models import SESSION_DIR

    return Path(SESSION_DIR)


def _validate_id(value: str, field: str) -> str:
    cleaned = str(value or "").strip()
    if not cleaned or "/" in cleaned or "\\" in cleaned or not _SAFE_ID_RE.fullmatch(cleaned):
        raise ValueError(f"invalid {field}")
    return cleaned


def _run_path(session_id: str, run_id: str, session_dir: Path | None = None) -> Path:
    sid = _validate_id(session_id, "session_id")
    rid = _validate_id(run_id, "run_id")
    root = Path(session_dir) if session_dir is not None else _default_session_dir()
    return root / RUN_JOURNAL_DIR_NAME / sid / f"{rid}.jsonl"


def _lock_for(path: Path) -> threading.Lock:
    key = (str(path.parent), path.name, str(os.getpid()))
    with _WRITER_LOCKS_GUARD:
        lock = _WRITER_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _WRITER_LOCKS[key] = lock
        return lock


def _summary_cache_signature(path: Path) -> tuple[int, int, int, int, int] | None:
    """Return the complete filesystem identity used for summary-cache validity.

    Includes ``st_ctime_ns`` so a same-inode, same-size rewrite that restores the
    original ``mtime_ns`` (e.g. an atomic replace) still invalidates the cache —
    ctime advances on any metadata/content change and cannot be forged back.
    Falls back to the archived copy (#7613) so an archived run's summary can
    still be cached, and when BOTH copies exist (crash window / re-created run)
    folds both stats into the signature so a change to either invalidates the
    cached merged summary. The key stays the live path.
    """
    live_stat = None
    try:
        live_stat = path.stat()
    except OSError:
        pass
    archived = _archive_path_for(path)
    archive_stat = None
    if archived is not None:
        try:
            archive_stat = archived.stat()
        except OSError:
            archive_stat = None
    if live_stat is None and archive_stat is None:
        return None
    if live_stat is None:
        live_stat = archive_stat
    try:
        return (
            int(live_stat.st_dev),
            int(live_stat.st_ino),
            int(live_stat.st_size) + (int(archive_stat.st_size) if archive_stat else 0),
            max(int(live_stat.st_mtime_ns), int(archive_stat.st_mtime_ns) if archive_stat else 0),
            max(int(live_stat.st_ctime_ns), int(archive_stat.st_ctime_ns) if archive_stat else 0),
        )
    except OSError:
        return None


def _get_cached_summary(path: Path) -> dict | None:
    signature = _summary_cache_signature(path)
    if signature is None:
        return None
    key = str(path)
    with _SUMMARY_CACHE_LOCK:
        cached = _SUMMARY_CACHE.get(key)
        if cached is None:
            return None
        cached_signature, summary = cached
        if cached_signature != signature:
            _SUMMARY_CACHE.pop(key, None)
            return None
        _SUMMARY_CACHE.move_to_end(key)
        return deepcopy(summary)


def _cache_summary(
    path: Path,
    summary: dict,
    *,
    expected_signature: tuple[int, int, int, int, int] | None = None,
) -> None:
    signature = _summary_cache_signature(path)
    # The pre-read signature is an enforced TOCTOU precondition. In particular,
    # a journal created after a missing-file read has ``None -> signature`` and
    # must not cache the empty/unknown result under the new file's identity.
    if signature is None or signature != expected_signature:
        return
    key = str(path)
    with _SUMMARY_CACHE_LOCK:
        _SUMMARY_CACHE[key] = (signature, deepcopy(summary))
        _SUMMARY_CACHE.move_to_end(key)
        while len(_SUMMARY_CACHE) > _SUMMARY_CACHE_MAX_ENTRIES:
            _SUMMARY_CACHE.popitem(last=False)


def _discard_cached_summary(path: Path) -> None:
    with _SUMMARY_CACHE_LOCK:
        _SUMMARY_CACHE.pop(str(path), None)


def _archive_path_for(live_path: Path) -> Path | None:
    """Map a live run-file path to its archive sibling, or None when not run-shaped."""
    if live_path.suffix != ".jsonl" or live_path.parent.parent.name != RUN_JOURNAL_DIR_NAME:
        return None
    return (
        live_path.parent.parent.parent
        / RUN_JOURNAL_ARCHIVE_DIR_NAME
        / live_path.parent.name
        / f"{live_path.stem}.jsonl.gz"
    )


def _archive_read_allowed(archive_path: Path) -> bool:
    """True when ``archive_path`` may be opened as journal data (fail closed).

    Archive reads must stay inside the journal tree: the archive root and the
    per-session directory must be real directories (never symlinks) that resolve
    within the archive root, and the ``.jsonl.gz`` entry itself must be a real
    regular file. Without this a symlinked ``_run_journal_archive/<sid>`` (or a
    swapped entry) could make readers serve an arbitrary external file as
    journal rows.
    """
    archive_root = archive_path.parent.parent
    if not archive_path.name.endswith(".jsonl.gz"):
        return False
    try:
        if not _SAFE_ID_RE.fullmatch(archive_path.parent.name):
            return False
        if archive_root.is_symlink() or archive_path.parent.is_symlink():
            return False
        if not archive_root.is_dir() or not archive_path.parent.is_dir():
            return False
        root_real = os.path.realpath(archive_root)
        session_real = os.path.realpath(archive_path.parent)
        if session_real != root_real and not session_real.startswith(root_real + os.sep):
            return False
        if archive_path.is_symlink() or not archive_path.is_file():
            return False
    except OSError:
        return False
    return True


def _read_run_file_text(path: Path) -> str | None:
    """Read a run file as text, transparently falling back to its archived copy.

    Resolution order per run:
      * the live ``.jsonl`` file alone when only it exists (the normal case);
      * the archived ``.jsonl.gz`` alone when the run has been archived;
      * when BOTH exist — only possible across a crash window (archive
        published but the live unlink did not run) or when a writer re-created
        a run path after it was archived — the UNION in journal order: archive
        rows first, then any live rows whose seq continues past the archive's
        last seq. An append-only run can never legitimately have live rows at
        or below the archived max, so those are the archive's own copy of the
        same rows and re-reading them would only duplicate seqs.

    The archive never shadows newer live data: once a writer re-creates a run
    path, its appended rows are always part of what readers see. Returns None
    when neither copy exists or is readable.
    """
    if path.suffix == ".gz":
        return _read_gz_text(path)
    live_text = None
    try:
        live_text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        pass
    except OSError:
        return None
    archived = _archive_path_for(path)
    if archived is None:
        return live_text
    if live_text is None:
        return _read_gz_text(archived)
    archive_text = _read_gz_text(archived)
    if not archive_text:
        return live_text
    return _merge_archive_and_live_text(archive_text, live_text)


def _merge_archive_and_live_text(archive_text: str, live_text: str) -> str:
    """Union archived rows with the live rows that continue past them.

    Rows are matched by ``seq`` (leniently: an unparseable row is kept, the
    journal tolerates malformed rows and the readers surface them).
    """
    max_archived_seq = 0
    for raw in archive_text.splitlines():
        stripped = raw.strip()
        if not stripped:
            continue
        try:
            seq = int(json.loads(stripped).get("seq") or 0)
        except (json.JSONDecodeError, AttributeError, TypeError, ValueError):
            continue
        if seq > max_archived_seq:
            max_archived_seq = seq
    kept: list[str] = []
    for raw in live_text.splitlines():
        if not raw.strip():
            continue
        try:
            seq = int(json.loads(raw).get("seq") or 0)
        except (json.JSONDecodeError, AttributeError, TypeError, ValueError):
            kept.append(raw)
            continue
        if seq > max_archived_seq:
            kept.append(raw)
    if not kept:
        return archive_text
    joiner = "" if archive_text.endswith("\n") else "\n"
    return archive_text + joiner + "\n".join(kept) + "\n"


def _read_gz_text(archive_path: Path) -> str | None:
    if not _archive_read_allowed(archive_path):
        return None
    try:
        with gzip.open(archive_path, "rt", encoding="utf-8") as fh:
            return fh.read()
    except FileNotFoundError:
        return None
    except (OSError, EOFError, UnicodeDecodeError):
        # A corrupt archive must never be treated as "no events": the live file
        # is already gone in that case, so surface it loudly and let callers
        # fall back to their recovery paths on an empty read.
        logger.warning("Run-journal archive unreadable: %s", archive_path, exc_info=True)
        return None


def _read_jsonl(path: Path) -> tuple[list[dict], list[dict]]:
    events: list[dict] = []
    malformed: list[dict] = []
    text = _read_run_file_text(path)
    if text is None:
        return events, malformed
    lines = text.splitlines()
    for line_no, raw in enumerate(lines, start=1):
        if not raw.strip():
            continue
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            malformed.append({"line": line_no, "raw": raw})
            continue
        if isinstance(parsed, dict):
            events.append(parsed)
        else:
            malformed.append({"line": line_no, "raw": raw})
    return events, malformed


def _parse_run_journal_event_id(raw: str | None) -> tuple[str | None, int | None]:
    raw = str(raw or "").strip()
    if not raw:
        return None, None
    if ":" in raw:
        run_id, tail = raw.rsplit(":", 1)
    else:
        run_id, tail = None, raw
    try:
        seq = max(0, int(tail))
    except (TypeError, ValueError):
        return run_id or None, None
    return run_id or None, seq


def _snapshot_args_take_budget(budget: dict[str, int], amount: int) -> int:
    remaining = max(0, int(budget.get("remaining") or 0))
    take = min(remaining, max(0, amount))
    budget["remaining"] = remaining - take
    return take


def _bound_snapshot_args_string(value: str, budget: dict[str, int]) -> str:
    max_chars = min(len(value), _SNAPSHOT_ARGS_MAX_STRING_CHARS)
    take = _snapshot_args_take_budget(budget, max_chars)
    out = value[:take]
    if take < len(value):
        suffix_take = _snapshot_args_take_budget(budget, len(_SNAPSHOT_ARGS_TRUNCATED_SUFFIX))
        out += _SNAPSHOT_ARGS_TRUNCATED_SUFFIX[:suffix_take]
    return out


def _bound_run_journal_snapshot_value(value: Any, budget: dict[str, int], depth: int) -> Any:
    if budget.get("remaining", 0) <= 0:
        return None
    if isinstance(value, str):
        return _bound_snapshot_args_string(value, budget)
    if isinstance(value, dict):
        if depth >= _SNAPSHOT_ARGS_MAX_DEPTH:
            return {}
        out: dict[str, Any] = {}
        for index, (key, item) in enumerate(value.items()):
            if index >= _SNAPSHOT_ARGS_MAX_ITEMS or budget.get("remaining", 0) <= 0:
                break
            bounded_key = _bound_snapshot_args_string(str(key), budget)
            if not bounded_key:
                continue
            out[bounded_key] = _bound_run_journal_snapshot_value(item, budget, depth + 1)
        return out
    if isinstance(value, (list, tuple)):
        if depth >= _SNAPSHOT_ARGS_MAX_DEPTH:
            return []
        return [
            _bound_run_journal_snapshot_value(item, budget, depth + 1)
            for item in value[:_SNAPSHOT_ARGS_MAX_ITEMS]
            if budget.get("remaining", 0) > 0
        ]
    if isinstance(value, (bool, int, float)) or value is None:
        try:
            _snapshot_args_take_budget(budget, len(json.dumps(value)))
        except (TypeError, ValueError):
            return None
        return value
    return _bound_snapshot_args_string(str(value), budget)


def bound_run_journal_snapshot_args(args: Any) -> Any:
    """Return recovery tool args with realistic values intact and pathological payloads bounded."""
    if args is None:
        return {}
    budget = {"remaining": _SNAPSHOT_ARGS_MAX_TOTAL_CHARS}
    return _bound_run_journal_snapshot_value(args, budget, 0)


def _next_seq(path: Path) -> int:
    events, _malformed = _read_jsonl(path)
    seqs = [int(event.get("seq") or 0) for event in events if isinstance(event.get("seq"), int)]
    return (max(seqs) + 1) if seqs else 1


def _reserve_next_seq(path: Path) -> int:
    """Reserve and return the next seq for ``path``, advancing the in-memory cache.

    Callers MUST hold ``_lock_for(path)``. The first append per path in this
    process seeds the cache from ``_next_seq(path)`` (one file read); every later
    append is a pure in-memory increment, avoiding the O(n) re-parse that
    re-reading the whole journal on every append caused (O(n^2) over a run).
    Because ``RunJournalWriter`` and the free ``append_run_event`` share this one
    cache under the same per-path lock, their seqs stay monotonic and gapless
    even when both write the same path. ``_SEQ_CACHE_LOCK`` additionally makes the
    dict get+set atomic against a concurrent cross-path eviction.
    """
    key = str(path)
    with _SEQ_CACHE_LOCK:
        nxt = _SEQ_CACHE.get(key)
        if nxt is not None:
            _SEQ_CACHE[key] = nxt + 1
            return nxt
    # Cache miss: seed from disk WITHOUT holding the module-global lock, so a
    # slow first-access file read for one path can't block every other path's
    # cache ops. The caller holds the per-path lock, so only one thread per path
    # can reach this branch — no double-seed, and no same-path writer can race
    # the value in between.
    seeded = _next_seq(path)
    with _SEQ_CACHE_LOCK:
        _SEQ_CACHE[key] = seeded + 1
        return seeded


def _note_assigned_seq(path: Path, seq: int) -> None:
    """Keep the cache at least one past an explicitly-supplied ``seq``.

    Callers MUST hold ``_lock_for(path)``. When an append carries a caller-chosen
    ``seq`` rather than drawing from the cache, advance the cache so a later
    cache-based append on the same path cannot re-issue an already-used seq.
    """
    key = str(path)
    nxt = int(seq) + 1
    with _SEQ_CACHE_LOCK:
        if _SEQ_CACHE.get(key, 0) < nxt:
            _SEQ_CACHE[key] = nxt


def _terminal_state_for_event(event_name: str, payload) -> str | None:
    name = str(event_name or "")
    if name == "done" or name == "stream_end":
        if isinstance(payload, dict):
            explicit_state = str(payload.get("terminal_state") or "").strip().lower()
            if explicit_state in {"tool_limit_reached"}:
                return explicit_state
        return "completed"
    if name == "cancel":
        return "interrupted-by-user"
    if name in {"apperror", "error"}:
        err_type = str((payload or {}).get("type") or "").strip().lower() if isinstance(payload, dict) else ""
        if err_type == "tool_limit_reached":
            return "tool_limit_reached"
        if err_type in {"cancelled", "canceled"}:
            return "interrupted-by-user"
        if err_type == "interrupted":
            return "interrupted-by-crash"
        return "errored"
    return None


def _run_journal_fsync_mode() -> str:
    raw = os.environ.get(_FSYNC_MODE_ENV, _FSYNC_MODE_TERMINAL_ONLY)
    mode = str(raw or "").strip().lower()
    if mode in {_FSYNC_MODE_EAGER, _FSYNC_MODE_TERMINAL_ONLY}:
        return mode
    return _FSYNC_MODE_TERMINAL_ONLY


def _should_fsync_event(terminal_state: str | None) -> bool:
    if _run_journal_fsync_mode() == _FSYNC_MODE_EAGER:
        return True
    return bool(terminal_state)


def _fsync_parent_dir(path: Path) -> None:
    try:
        dir_fd = os.open(path.parent, getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except OSError:
        pass


def _event_created_at(event: dict, *, fallback: float = 0.0) -> float:
    try:
        return float(event.get("created_at") or fallback)
    except (TypeError, ValueError):
        return fallback


def _run_journal_file_bytes(path: Path):
    """Return a binary reader for a run file: live ``.jsonl`` or archived ``.jsonl.gz``.

    Raises FileNotFoundError when neither copy exists. The both-exist case is
    handled by the caller via :func:`_read_run_file_text` (it needs row-level
    merging); this opener is for streaming reads of a single copy. Archived
    copies must pass :func:`_archive_read_allowed` (containment, fail closed).
    """
    if path.suffix == ".gz":
        if not _archive_read_allowed(path):
            raise FileNotFoundError(str(path))
        return gzip.open(path, "rb")
    try:
        return path.open("rb")
    except FileNotFoundError:
        archived = _archive_path_for(path)
        if archived is None or not _archive_read_allowed(archived):
            raise
        return gzip.open(archived, "rb")


def _iter_bounded_raw_jsonl_lines(path: Path, *, max_bytes: int, retained_bytes: int = 0):
    line_no = 0
    buffered = bytearray()
    total_bytes = int(retained_bytes)
    live_exists = path.exists()
    archived = _archive_path_for(path)
    if live_exists and archived is not None and archived.exists():
        # Crash-window / re-created-run case (both copies on disk): fall back to
        # the merged full text so the bounded iteration sees the union rather
        # than one copy's rows. Rare by construction — see `_read_run_file_text`.
        merged = _read_run_file_text(path)
        if merged is None:
            return
        for raw in merged.splitlines(keepends=True):
            raw_bytes = raw.encode("utf-8")
            line_no += 1
            total_bytes += len(raw_bytes)
            if total_bytes > max_bytes:
                raise ValueError("replay_limit_bytes")
            yield line_no, raw_bytes, total_bytes
        return
    try:
        fh = _run_journal_file_bytes(path)
    except FileNotFoundError:
        return
    with fh:
        while True:
            chunk = fh.read(_SESSION_REPLAY_READ_CHUNK_BYTES)
            if not chunk:
                if buffered:
                    if total_bytes + len(buffered) > max_bytes:
                        raise ValueError("replay_limit_bytes")
                    line_no += 1
                    total_bytes += len(buffered)
                    yield line_no, bytes(buffered), total_bytes
                return
            start = 0
            while start < len(chunk):
                newline = chunk.find(b"\n", start)
                if newline == -1:
                    buffered.extend(chunk[start:])
                    if total_bytes + len(buffered) > max_bytes:
                        raise ValueError("replay_limit_bytes")
                    break
                buffered.extend(chunk[start : newline + 1])
                if total_bytes + len(buffered) > max_bytes:
                    raise ValueError("replay_limit_bytes")
                line_no += 1
                total_bytes += len(buffered)
                yield line_no, bytes(buffered), total_bytes
                buffered.clear()
                start = newline + 1


def append_run_event(
    session_id: str,
    run_id: str,
    event_name: str,
    payload=None,
    *,
    session_dir: Path | None = None,
    seq: int | None = None,
    created_at: float | None = None,
) -> dict:
    """Append one durable run event and fsync it according to the journal policy."""
    path = _run_path(session_id, run_id, session_dir=session_dir)
    payload = payload if payload is not None else {}
    event_name = str(event_name or "").strip()
    if not event_name:
        raise ValueError("event_name is required")
    with _lock_for(path):
        if seq is not None:
            assigned_seq = int(seq)
            _note_assigned_seq(path, assigned_seq)
        else:
            assigned_seq = _reserve_next_seq(path)
        terminal_state = _terminal_state_for_event(event_name, payload)
        event = {
            "version": 1,
            "event_id": f"{run_id}:{assigned_seq}",
            "seq": assigned_seq,
            "run_id": str(run_id),
            "session_id": str(session_id),
            "event": event_name,
            "type": event_name,
            "created_at": float(created_at if created_at is not None else time.time()),
            "terminal": bool(terminal_state),
            "terminal_state": terminal_state,
            "payload": payload,
        }
        path.parent.mkdir(parents=True, exist_ok=True)
        created_file = not path.exists()
        line = json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n"
        fd = os.open(path, os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o600)
        with os.fdopen(fd, "a", encoding="utf-8") as fh:
            fh.write(line)
            fh.flush()
            if _should_fsync_event(terminal_state):
                os.fsync(fh.fileno())
        _discard_cached_summary(path)
        if created_file:
            _fsync_parent_dir(path)
        return event


class RunJournalWriter:
    """Stateful writer for one WebUI stream/run."""

    def __init__(self, session_id: str, run_id: str, *, session_dir: Path | None = None):
        self.session_id = _validate_id(session_id, "session_id")
        self.run_id = _validate_id(run_id, "run_id")
        self.session_dir = Path(session_dir) if session_dir is not None else None

    def append_sse_event(self, event_name: str, payload=None) -> dict | None:
        # Live-UI-only telemetry (metering) has no recovery value in the journal:
        # nothing reads those rows back for recovery, and journaling them at ~10 Hz
        # on marathon runs balloons the durable file (12+ MB of a single 18 MB run
        # was metering). Skip the write entirely and return None so callers'
        # journal-id plumbing (``(journaled or {}).get("event_id")``) is untouched.
        # Not reserving a seq keeps the remaining journaled seqs contiguous, which
        # the offline-gap coverage and replay-cursor contiguity checks rely on.
        if str(event_name or "").strip() in REPLAY_SKIPPED_SSE_EVENTS:
            return None
        # Allocate the sequence inside the same per-path transaction that writes
        # the row. Reserving here, then releasing the lock before append, lets a
        # concurrent writer put a higher sequence on disk first.
        return append_run_event(
            self.session_id,
            self.run_id,
            event_name,
            payload or {},
            session_dir=self.session_dir,
        )


def journal_replay_visible(event) -> bool:
    """Return True when a journal row should be streamed to a reconnecting tab.

    Live-UI-only telemetry rows (see ``REPLAY_SKIPPED_SSE_EVENTS``) carry no
    recovery value — replaying a metering backlog only re-paints a stale TPS
    number while multiplying the reconnect burst size. Writers no longer journal
    them, but legacy journals may already contain them, so the replay emit sites
    filter through this predicate. Non-dict rows are passed through (visible) so
    an unexpected shape can never silently swallow user-visible output.
    """
    if not isinstance(event, dict):
        return True
    name = str(event.get("event") or event.get("type") or "")
    return name not in REPLAY_SKIPPED_SSE_EVENTS


def read_run_events(
    session_id: str,
    run_id: str,
    *,
    after_seq: int | None = None,
    max_seq: int | None = None,
    session_dir: Path | None = None,
) -> dict:
    path = _run_path(session_id, run_id, session_dir=session_dir)
    events, malformed = _read_jsonl(path)
    if after_seq is not None:
        events = [event for event in events if int(event.get("seq") or 0) > int(after_seq)]
    if max_seq is not None:
        events = [event for event in events if int(event.get("seq") or 0) <= int(max_seq)]
    return {
        "session_id": str(session_id),
        "run_id": str(run_id),
        "events": events,
        "malformed": malformed,
    }


def select_authoritative_terminal_event(events: Iterable[dict]) -> dict | None:
    """Return the terminal event that owns the run's settled outcome.

    ``stream_end`` is transport closure, so a preceding semantic terminal event
    (done, cancel, or error) remains authoritative. Among semantic terminal
    events, the latest journal row wins.
    """
    terminal_events = [
        event
        for event in events
        if isinstance(event, dict) and event.get("terminal")
    ]
    return next(
        (
            event
            for event in reversed(terminal_events)
            if event.get("event") != "stream_end"
        ),
        terminal_events[-1] if terminal_events else None,
    )


def runtime_model_from_events(session_id: str, stream_id: str, events: Iterable[dict]) -> dict | None:
    """Project only observed serving identity for this journal owner.

    A fallback warning or malformed later observation invalidates old evidence;
    the configured selection and free-text status never supply serving identity.
    """
    observed = None
    for event in events:
        if not isinstance(event, dict) or event.get("session_id") != session_id or event.get("run_id") != stream_id:
            continue
        payload = event.get("payload")
        if not isinstance(payload, dict):
            if event.get("event") == "runtime_model":
                observed = None
            continue
        if event.get("event") == "warning" and payload.get("type") == "fallback":
            observed = None
        elif event.get("event") == "runtime_model":
            observed = None
            if (payload.get("session_id") != session_id or payload.get("stream_id") != stream_id
                or not isinstance(payload.get("model"), str) or not payload["model"].strip()
                or not isinstance(payload.get("fallback_active"), bool)
                or payload.get("phase") not in ("observed_output", "route_observed")):
                continue
            observed = {key: payload[key] for key in (
                "session_id", "stream_id", "model", "fallback_active", "phase")}
            if isinstance(payload.get("provider"), str) and payload["provider"].strip():
                observed["provider"] = payload["provider"]
    return observed


def _summary_from_events(session_id: str, run_id: str, events: Iterable[dict]) -> dict:
    ordered = [event for event in events if isinstance(event, dict)]
    last = ordered[-1] if ordered else None
    terminal = select_authoritative_terminal_event(ordered)
    status = terminal.get("terminal_state") if terminal else ("running" if ordered else "unknown")
    return {
        "session_id": str(session_id),
        "run_id": str(run_id),
        "stream_id": str(run_id),
        "event_count": len(ordered),
        "last_seq": int((last or {}).get("seq") or 0),
        "last_event_id": (last or {}).get("event_id"),
        "terminal": bool(terminal),
        "terminal_state": status,
        "last_event": (last or {}).get("event"),
        "runtime_model": runtime_model_from_events(session_id, run_id, ordered),
    }


def latest_run_summary(session_id: str, run_id: str, *, session_dir: Path | None = None) -> dict:
    path = _run_path(session_id, run_id, session_dir=session_dir)
    cached = _get_cached_summary(path)
    if cached is not None:
        return cached
    pre_read_signature = _summary_cache_signature(path)
    events, _malformed = _read_jsonl(path)
    summary = _summary_from_events(session_id, run_id, events)
    _cache_summary(path, summary, expected_signature=pre_read_signature)
    return summary


def session_journal_fingerprint(session_id: str, *, session_dir: Path | None = None) -> tuple[int, float, int]:
    """Cheap, bounded fingerprint of a session's run journal: (file_count, max_mtime, total_size).

    Reads only directory + per-file stat metadata (never parses journal bodies), so it stays
    O(runs) and cannot be tipped over by a large ``done`` row. Used to detect that the journal
    advanced during an idle live-subscribe wait — a run that starts AND finishes inside a single
    keepalive tick leaves the journal changed but never materializes a live in-memory stream, so a
    no-cursor idle subscriber would otherwise miss it until a manual refresh. Returns (0, 0.0, 0)
    when the session has no journal yet. Invalid ids resolve to the empty fingerprint rather than
    raising so callers can probe unconditionally.
    """
    try:
        sid = _validate_id(session_id, "session_id")
    except ValueError:
        return (0, 0.0, 0)
    root = Path(session_dir) if session_dir is not None else _default_session_dir()
    session_root = root / RUN_JOURNAL_DIR_NAME / sid
    if not session_root.exists():
        return (0, 0.0, 0)
    count = 0
    max_mtime = 0.0
    total_size = 0
    for path in session_root.glob("*.jsonl"):
        try:
            st = path.stat()
        except OSError:
            continue
        count += 1
        total_size += st.st_size
        if st.st_mtime > max_mtime:
            max_mtime = st.st_mtime
    return (count, max_mtime, total_size)


def _archive_contained_session_dirs(archive_root: Path) -> list[Path]:
    """Archive session dirs that provably stay inside the archive root.

    Mirrors the live sweep's containment discipline: a session directory that is
    a symlink (or resolves outside the archive root) is never trusted, so a
    swapped pathname cannot make repository readers serve an external file as
    journal data.
    """
    try:
        if not archive_root.is_dir() or archive_root.is_symlink():
            return []
        root_real = os.path.realpath(archive_root)
        out: list[Path] = []
        for entry in sorted(archive_root.iterdir()):
            if not _SAFE_ID_RE.fullmatch(entry.name):
                continue
            if entry.is_symlink() or not entry.is_dir():
                continue
            try:
                resolved = os.path.realpath(entry)
            except OSError:
                continue
            if resolved != root_real and not resolved.startswith(root_real + os.sep):
                continue
            out.append(entry)
        return out
    except OSError:
        return []


def _glob_archived_run_paths(journal_root: Path, run_id: str) -> list[Path]:
    """Archived ``<rid>.jsonl.gz`` paths for a run id, as archive-dir Paths.

    Only session directories that pass :func:`_archive_contained_session_dirs`
    are searched, and the returned path is re-checked to be inside the archive
    root — archived reads fail closed rather than following a symlinked
    directory out of the journal tree.
    """
    archive_root = journal_root.parent / RUN_JOURNAL_ARCHIVE_DIR_NAME
    out: list[Path] = []
    for session_dir in _archive_contained_session_dirs(archive_root):
        candidate = session_dir / f"{run_id}.jsonl.gz"
        try:
            if candidate.is_symlink() or not candidate.is_file():
                continue
        except OSError:
            continue
        out.append(candidate)
    return sorted(out)


def find_run_summary(run_id: str, *, session_dir: Path | None = None) -> dict | None:
    rid = _validate_id(run_id, "run_id")
    root = Path(session_dir) if session_dir is not None else _default_session_dir()
    journal_root = root / RUN_JOURNAL_DIR_NAME
    candidates = list(journal_root.glob(f"*/{rid}.jsonl"))
    if not candidates:
        # Fall back to the archived copies (#7613): a long-idle run's rows are
        # compressed, and every reader must still find them.
        candidates = _glob_archived_run_paths(journal_root, rid)
    for path in candidates:
        session_id = path.parent.name
        summary = _get_cached_summary(path)
        if summary is None:
            pre_read_signature = _summary_cache_signature(path)
            events, _malformed = _read_jsonl(path)
            summary = _summary_from_events(session_id, rid, events)
            _cache_summary(path, summary, expected_signature=pre_read_signature)
        summary["path"] = str(path)
        return summary
    return None


def find_run_file(run_id: str, *, session_dir: Path | None = None) -> tuple[str, Path] | None:
    """Locate a run journal file by run id WITHOUT parsing its body.

    Hot callers that immediately read the full journal (the live-snapshot
    rebuild) must not pay :func:`find_run_summary`'s full-file parse first:
    on a long live run the file holds tens of thousands of rows and parsing
    it twice per rebuild dominated the snapshot cost. Returns
    ``(session_id, path)`` for the first match, or ``None`` when the run id
    is invalid or no journal exists. Archived copies (#7613) return the LIVE
    path they would occupy, so callers keep one addressing scheme and the
    read helpers resolve archive fallback themselves.
    """
    try:
        rid = _validate_id(run_id, "run_id")
    except ValueError:
        return None
    root = Path(session_dir) if session_dir is not None else _default_session_dir()
    journal_root = root / RUN_JOURNAL_DIR_NAME
    for path in journal_root.glob(f"*/{rid}.jsonl"):
        return path.parent.name, path
    for archived in _glob_archived_run_paths(journal_root, rid):
        live_path = journal_root / archived.parent.name / f"{rid}.jsonl"
        return archived.parent.name, live_path
    return None


def _session_run_paths(root: Path, sid: str, session_root: Path) -> list[Path]:
    """Every run file for a session (live + archived) as LIVE-style paths.

    Returns the same ``<sid>/<rid>.jsonl`` addressing the readers already use,
    one entry per run id (a run present in both places appears once — the read
    helpers merge those copies). Sorted by name so replay ordering matches
    upstream's ``sorted(glob)`` behavior.
    """
    paths: dict[str, Path] = {}
    try:
        for path in session_root.glob("*.jsonl"):
            paths[path.stem] = path
    except OSError:
        pass
    archive_session_root = root / RUN_JOURNAL_ARCHIVE_DIR_NAME / sid
    # Containment: only a real (non-symlinked) session directory inside the
    # archive root is searched, and each entry must pass `_archive_read_allowed`.
    try:
        if _SAFE_ID_RE.fullmatch(sid) and not archive_session_root.is_symlink():
            for archived in sorted(archive_session_root.glob("*.jsonl.gz")):
                if not _archive_read_allowed(archived):
                    continue
                stem = archived.name[: -len(".jsonl.gz")]
                if stem not in paths:
                    paths[stem] = session_root / f"{stem}.jsonl"
    except OSError:
        pass
    return [paths[key] for key in sorted(paths)]


def _all_session_foreign_run_paths(root: Path, sid: str, run_id: str) -> list[Path]:
    """Live + archived paths for ``run_id`` in sessions OTHER than ``sid``.

    Mirrors the live-only glob it replaces, extended so an archived run under a
    foreign session still reports ``cursor_session_mismatch`` instead of a bare
    ``cursor_run_missing``.
    """
    found: list[Path] = []
    journal_root = root / RUN_JOURNAL_DIR_NAME
    try:
        found.extend(p for p in journal_root.glob(f"*/{run_id}.jsonl") if p.parent.name != sid)
    except OSError:
        pass
    if not found:
        try:
            found.extend(
                p for p in _glob_archived_run_paths(journal_root, run_id) if p.parent.name != sid
            )
        except OSError:
            pass
    return found


def _run_file_mtime(path: Path) -> float:
    """mtime of a run file's live or archived copy; 0.0 when neither exists."""
    try:
        return path.stat().st_mtime
    except OSError:
        archived = _archive_path_for(path)
        if archived is None:
            return 0.0
        try:
            return archived.stat().st_mtime
        except OSError:
            return 0.0


def read_session_run_events(
    session_id: str,
    *,
    after_event_id: str | None = None,
    session_dir: Path | None = None,
    max_bytes: int = _SESSION_REPLAY_MAX_BYTES,
    max_rows: int = _SESSION_REPLAY_MAX_ROWS,
) -> dict:
    """Replay durable run-journal rows for one session after an opaque cursor."""
    sid = _validate_id(session_id, "session_id")
    cursor_run_id, cursor_seq = _parse_run_journal_event_id(after_event_id)
    raw_cursor = str(after_event_id or "").strip()
    if raw_cursor and cursor_run_id is not None:
        try:
            cursor_run_id = _validate_id(cursor_run_id, "run_id")
        except ValueError:
            cursor_seq = None
    if raw_cursor:
        try:
            if int(raw_cursor.rsplit(":", 1)[-1]) < 0:
                cursor_seq = None
        except (TypeError, ValueError):
            pass
    if raw_cursor and (cursor_run_id is None or cursor_seq is None or cursor_seq <= 0):
        return {
            "session_id": sid,
            "cursor_run_id": cursor_run_id,
            "cursor_seq": cursor_seq,
            "status": "cursor_invalid",
            "events": [],
        }
    if not raw_cursor:
        return {
            "session_id": sid,
            "cursor_run_id": None,
            "cursor_seq": None,
            "status": "ok",
            "events": [],
        }
    root = Path(session_dir) if session_dir is not None else _default_session_dir()
    session_root = root / RUN_JOURNAL_DIR_NAME / sid
    runs: list[tuple[float, str, list[dict]]] = []
    retained_rows = 0
    retained_bytes = 0
    for path in _session_run_paths(root, sid, session_root):
        run_id = path.stem
        try:
            run_id = _validate_id(run_id, "run_id")
        except ValueError:
            continue
        events: list[dict] = []
        expected_seq = 1
        try:
            for _line_no, raw, total_bytes in _iter_bounded_raw_jsonl_lines(
                path,
                max_bytes=max_bytes,
                retained_bytes=retained_bytes,
            ):
                retained_bytes = total_bytes
                if not raw.strip():
                    continue
                try:
                    event = json.loads(raw.decode("utf-8"))
                    seq = int(event.get("seq")) if isinstance(event, dict) else 0
                except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError):
                    return {"session_id": sid, "cursor_run_id": cursor_run_id, "cursor_seq": cursor_seq, "status": "replay_malformed", "events": []}
                if (
                    seq != expected_seq
                    or event.get("event_id") != f"{run_id}:{seq}"
                    or event.get("run_id") != run_id
                    or event.get("session_id") != sid
                ):
                    return {"session_id": sid, "cursor_run_id": cursor_run_id, "cursor_seq": cursor_seq, "status": "replay_noncontiguous", "events": []}
                expected_seq += 1
                retained_rows += 1
                if retained_rows > max_rows:
                    return {"session_id": sid, "cursor_run_id": cursor_run_id, "cursor_seq": cursor_seq, "status": "replay_limit_rows", "events": []}
                events.append(event)
        except FileNotFoundError:
            continue
        except ValueError as exc:
            if str(exc) == "replay_limit_bytes":
                return {"session_id": sid, "cursor_run_id": cursor_run_id, "cursor_seq": cursor_seq, "status": "replay_limit_bytes", "events": []}
            raise
        created_at = min((_event_created_at(event) for event in events), default=_run_file_mtime(path))
        runs.append((created_at, run_id, events))
    runs.sort(key=lambda run: (run[0], run[1]))
    cursor_index = next((index for index, (_created_at, run_id, _events) in enumerate(runs) if run_id == cursor_run_id), None)
    if cursor_index is None:
        # Look for the same run id under ANOTHER session (live or archived) to
        # distinguish "cursor belongs to a different session" from "gone".
        foreign_paths = _all_session_foreign_run_paths(root, sid, cursor_run_id) if cursor_run_id else []
        foreign_session_id = next((path.parent.name for path in foreign_paths), "")
        status = "cursor_run_missing"
        if foreign_session_id:
            status = "cursor_session_mismatch"
        return {
            "session_id": sid,
            "cursor_run_id": cursor_run_id,
            "cursor_seq": cursor_seq,
            "status": status,
            "events": [],
        }
    cursor_events = runs[cursor_index][2]
    if cursor_seq is None or cursor_seq > len(cursor_events):
        return {"session_id": sid, "cursor_run_id": cursor_run_id, "cursor_seq": cursor_seq, "status": "cursor_event_missing", "events": []}
    replay_events = [event for event in cursor_events if event["seq"] > cursor_seq]
    for _created_at, _run_id, events in runs[cursor_index + 1:]:
        replay_events.extend(events)
    return {
        "session_id": sid,
        "cursor_run_id": cursor_run_id,
        "cursor_seq": cursor_seq,
        "status": "ok",
        "events": replay_events,
    }


def delete_run_journal(session_id: str, *, session_dir: Path | None = None) -> bool:
    """Remove the entire per-session run-journal directory (``_run_journal/{sid}/``).

    The run journal stores one directory per session containing a ``{rid}.jsonl``
    file per run, so removing the session's directory clears every run's full
    request/response payloads. The session's archived runs (#7613) are removed
    too: an archived ``.jsonl.gz`` is a compressed copy of those same payloads,
    and leaving it behind would break this helper's deletion contract (a
    "deleted" session must not leave recoverable transcripts on disk).
    Invalid/empty ids and a missing directory are a no-op so callers can invoke
    this unconditionally on delete. Returns ``True`` if a directory was
    removed, ``False`` otherwise.
    """
    import shutil

    sid = str(session_id or "").strip()
    # Reject path-traversal ids: the regex below permits dots, so a bare "." or
    # ".." would resolve `root / RUN_JOURNAL_DIR_NAME / sid` to the journal ROOT
    # (or its parent) and rmtree the wrong directory. The route call site only
    # passes real sids, but this is a public helper — guard it directly.
    if sid in (".", "..") or not sid or "/" in sid or "\\" in sid or not _SAFE_ID_RE.fullmatch(sid):
        return False
    root = Path(session_dir) if session_dir is not None else _default_session_dir()
    session_journal_dir = root / RUN_JOURNAL_DIR_NAME / sid
    archive_session_dir = root / RUN_JOURNAL_ARCHIVE_DIR_NAME / sid
    shutil.rmtree(archive_session_dir, ignore_errors=True)
    if not session_journal_dir.exists():
        return False
    shutil.rmtree(session_journal_dir, ignore_errors=True)
    removed = not session_journal_dir.exists()
    # Evict any writer locks the removed runs left behind. `_lock_for` keys are
    # ``(str(path.parent), path.name, pid)`` and every run file for this session
    # lives directly under ``session_journal_dir``, so drop all keys whose parent
    # dir matches — pid-independent — to keep `_WRITER_LOCKS` from growing forever.
    # Guard on confirmed removal: `rmtree(ignore_errors=True)` can silently leave
    # the directory (locked files on Windows, permission transients). If the files
    # still exist their locks are still live — evicting them would hand a later
    # `_lock_for` caller a brand-new Lock, breaking mutual exclusion with a writer
    # still holding the old one.
    if removed:
        dir_key = str(session_journal_dir)
        with _WRITER_LOCKS_GUARD:
            for key in [k for k in _WRITER_LOCKS if k[0] == dir_key]:
                del _WRITER_LOCKS[key]
        # Drop cached next-seq entries for the removed runs too. Every run file
        # for this session lives directly under ``session_journal_dir``, so its
        # cache key's parent dir matches. Without this, a run re-created at the
        # same path would resume the stale cached seq instead of restarting at 1.
        # Hold ``_SEQ_CACHE_LOCK`` — the SAME mutex ``_reserve_next_seq``/
        # ``_note_assigned_seq`` take — so a concurrent append on another path
        # cannot mutate the dict mid-iteration (``dictionary changed size``).
        with _SEQ_CACHE_LOCK:
            for cache_key in [entry for entry in _SEQ_CACHE if str(Path(entry).parent) == dir_key]:
                del _SEQ_CACHE[cache_key]
        with _SUMMARY_CACHE_LOCK:
            for cache_key in [entry for entry in _SUMMARY_CACHE if str(Path(entry).parent) == dir_key]:
                del _SUMMARY_CACHE[cache_key]
    return removed


# ── Archival retention ------------------------------------------------------------------


def _archive_dir(root: Path) -> Path:
    """Archive root for a session-directories root (sibling of the journal root)."""
    return root / RUN_JOURNAL_ARCHIVE_DIR_NAME


def _archive_path(session_id: str, run_id: str, session_dir: Path | None = None) -> Path:
    """Path of an archived run: ``<root>/_run_journal_archive/<sid>/<run_id>.jsonl.gz``."""
    sid = _validate_id(session_id, "session_id")
    rid = _validate_id(run_id, "run_id")
    root = Path(session_dir) if session_dir is not None else _default_session_dir()
    return _archive_dir(root) / sid / f"{rid}.jsonl.gz"


def _open_dir_no_follow(path: Path) -> int | None:
    """Open a directory as a stable handle (``O_NOFOLLOW``); None when unavailable.

    The returned fd pins the *inode* of ``path``: every later operation done
    relative to it acts on that directory no matter what the pathname is swapped
    to. A symlinked final component is refused (``ELOOP``) — the journal only
    ever creates real directories.
    """
    try:
        return os.open(str(path), os.O_RDONLY | _O_DIRECTORY | _O_NOFOLLOW)
    except OSError:
        return None


def _archive_run_file(
    source_path: Path,
    archive_path: Path,
    source_fd: int,
    archive_fd: int,
    expected_signature: tuple[int, int, int, int, int],
) -> int:
    """Compress ``source_path`` into ``archive_path`` and drop the live file.

    Returns bytes reclaimed (0 = skipped). This is the ONLY step that removes a
    live run file, and it is crash-safe by construction:

      1. compress the live file to a temp entry INSIDE the pinned archive dir,
      2. fsync the temp,
      3. verify the compressed copy decompresses to the exact source bytes,
      4. atomically rename temp -> final (``os.replace``, same filesystem),
      5. fsync the archive directory,
      6. only then unlink the live file (fd-relative, signature re-checked).

    A crash before step 6 leaves the live file untouched and a stray temp that
    the next pass cleans up. If any step fails, the live file survives — a
    missed archive is always safe, and a wrong classification costs one
    compressed copy rather than the run.

    The live file's stat identity is re-checked under the caller's path lock so
    a trailing append aborts the archive instead of racing it. The archive is
    written through pinned directory handles on BOTH sides, so a swapped
    pathname cannot redirect either end.
    """
    name = source_path.name
    # 1. Re-verify identity under the lock before doing any work.
    try:
        st = os.stat(name, dir_fd=source_fd, follow_symlinks=False)
    except OSError:
        return 0
    if _stat_signature(st) != expected_signature:
        return 0

    tmp_name = f".{name}.gz.tmp.{os.getpid()}"
    try:
        # 2. Compress + fsync into the pinned archive directory.
        src_fd = os.open(name, os.O_RDONLY | _O_NOFOLLOW, dir_fd=source_fd)
        tmp_fd = None
        try:
            tmp_fd = os.open(
                tmp_name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | _O_NOFOLLOW,
                0o600,
                dir_fd=archive_fd,
            )
            with os.fdopen(src_fd, "rb", closefd=False) as src, os.fdopen(
                tmp_fd, "wb", closefd=False
            ) as dst:
                with gzip.GzipFile(fileobj=dst, mode="wb", compresslevel=_ARCHIVE_GZIP_LEVEL) as gz:
                    shutil.copyfileobj(src, gz, _RETENTION_VERIFY_CHUNK_BYTES)
            os.fsync(tmp_fd)
        finally:
            # closefd=False above keeps ownership here; close exactly once each.
            for fd in (tmp_fd, src_fd):
                if fd is None:
                    continue
                try:
                    os.close(fd)
                except OSError:
                    pass

        # 3. Verify the compressed copy reproduces the source exactly. The
        #    source bytes are read through the SAME pinned descriptor path, so
        #    nothing can substitute a different file between the two reads.
        if not _archive_reproduces_source(tmp_name, archive_fd, name, source_fd):
            _unlink_archive_entry(tmp_name, archive_fd)
            return 0

        # 4. Atomic publish (never clobber), then 5. fsync the archive dir.
        #    os.link fails with FileExistsError when an archive for this run id
        #    is already present. That happens after a crash window (archive
        #    published, live unlink did not run) or a racing attempt; the live
        #    journal is append-only, so the existing archive is a prefix of the
        #    current live bytes — if it no longer reproduces them (the live file
        #    grew since), publish the freshly-verified copy over it; if it does,
        #    it is already the complete copy.
        try:
            os.link(tmp_name, archive_path.name, src_dir_fd=archive_fd, dst_dir_fd=archive_fd)
        except FileExistsError:
            if _archive_reproduces_source(archive_path.name, archive_fd, name, source_fd):
                _unlink_archive_entry(tmp_name, archive_fd)
            else:
                os.replace(
                    tmp_name, archive_path.name, src_dir_fd=archive_fd, dst_dir_fd=archive_fd
                )
        else:
            _unlink_archive_entry(tmp_name, archive_fd)
        try:
            os.fsync(archive_fd)
        except OSError:
            pass

        # 6. Drop the live file (only now), re-checking identity one last time.
        try:
            st = os.stat(name, dir_fd=source_fd, follow_symlinks=False)
        except OSError:
            return 0
        if _stat_signature(st) != expected_signature:
            return 0
        try:
            os.unlink(name, dir_fd=source_fd)
        except OSError:
            return 0
    except OSError:
        _unlink_archive_entry(f".{name}.gz.tmp.{os.getpid()}", archive_fd)
        return 0

    # Confirmed archive: the caller counts these bytes and drops the cached
    # summary for the (now gone) live path. The seq cache entry is intentionally
    # kept — reads merge the archived rows back in, so a hypothetical append at
    # the same path continues seqs at N+1 rather than restarting at 1.
    return int(st.st_size)


def _stat_identity(st: os.stat_result) -> tuple[int, int, int, int]:
    """Identity that survives a rename: ``(dev, ino, size, mtime_ns)``.

    Used to verify a prune CLAIM. ``st_ctime_ns`` cannot be part of this check
    because the claim itself is made by renaming the entry, and a rename bumps
    ctime by definition — including it would reject every legitimate claim.
    Replacing an entry goes through unlink/rename-over, which always lands a
    different inode (and any rewrite moves size or mtime), so a replacement is
    still detected.
    """
    return (int(st.st_dev), int(st.st_ino), int(st.st_size), int(st.st_mtime_ns))


def _prune_archive_entry(name: str, checked_stat: os.stat_result, dir_fd: int) -> bool:
    """Delete an aged archive entry by CLAIMING it first, then verifying.

    The age check ``stat()``s a mutable NAME, so a concurrent sweep that
    completes/replaces the archive at that path between the check and the
    unlink would otherwise see its freshly published copy — the only retained
    copy after archival — deleted. A last-moment identity re-check only narrows
    that window; it does not close it.

    Instead the entry is atomically CLAIMED by renaming it to a private name,
    so from that instant nothing else can mutate what we hold. The claim is then
    verified against the signature the age check recorded:

      * same identity  -> it is provably the aged entry -> unlink the claim;
      * different      -> we claimed a REPLACEMENT someone published after the
                          check -> restore it without ever clobbering the
                          canonical name (``link`` fails closed on EEXIST; an
                          archive only ever grows, so if the name was taken
                          again the newer copy supersedes this one and the
                          claim is dropped).

    Returns True only when the aged entry was removed.
    """
    claim = f".{name}.prune-claim.{os.getpid()}"
    try:
        os.rename(name, claim, src_dir_fd=dir_fd, dst_dir_fd=dir_fd)
    except OSError:
        # Vanished (another sweep pruned/replaced it) — nothing to do.
        return False
    try:
        st = os.stat(claim, dir_fd=dir_fd, follow_symlinks=False)
    except OSError:
        return False
    if not stat.S_ISREG(st.st_mode) or _stat_identity(st) != _stat_identity(checked_stat):
        # Not the entry the age check saw: put it back. `link` restores the
        # exact inode and fails closed (EEXIST) rather than clobbering a newer
        # archive published at the canonical name.
        _restore_prune_claim(claim, name, dir_fd)
        return False
    return _unlink_archive_entry(claim, dir_fd)


def _restore_prune_claim(claim: str, name: str, dir_fd: int) -> None:
    """Undo a prune claim: restore ``claim`` to ``name``, else drop it safely.

    Restoring uses ``link`` (never clobbers): when the canonical name is free
    the claimed inode goes back — a resurrected archive only over-retains. When
    a newer archive already occupies the name, the claim is redundant (readers
    resolve the canonical entry) and is dropped. On any other failure the claim
    file is LEFT IN PLACE: a stray dot-file is recoverable, a deleted archive
    is not — and the next sweep's cleanup re-tries it.
    """
    try:
        os.link(claim, name, src_dir_fd=dir_fd, dst_dir_fd=dir_fd, follow_symlinks=False)
        os.unlink(claim, dir_fd=dir_fd)
    except FileExistsError:
        try:
            os.unlink(claim, dir_fd=dir_fd)
        except OSError:
            pass
    except OSError:
        logger.warning("Run-journal prune claim left in place (restore failed): %s", claim, exc_info=True)


def _unlink_archive_entry(name: str, dir_fd: int) -> bool:
    try:
        os.unlink(name, dir_fd=dir_fd)
        return True
    except OSError:
        return False


def _archive_reproduces_source(
    gz_name: str,
    gz_dir_fd: int,
    source_name: str,
    source_fd: int,
) -> bool:
    """True when ``gz_name`` (in ``gz_dir_fd``) decompresses to exactly the source bytes.

    Compares in fixed chunks (never buffering a multi-MB run in memory) and
    requires the decompressed stream to match length AND content. A mismatch
    means a truncated, corrupt, or superseded archive, so the archive is never
    preferred over the live file.
    """
    try:
        gz_fd = os.open(gz_name, os.O_RDONLY | _O_NOFOLLOW, dir_fd=gz_dir_fd)
    except OSError:
        return False
    try:
        src_fd = os.open(source_name, os.O_RDONLY | _O_NOFOLLOW, dir_fd=source_fd)
    except OSError:
        try:
            os.close(gz_fd)
        except OSError:
            pass
        return False
    try:
        with os.fdopen(gz_fd, "rb", closefd=False) as gz_raw, os.fdopen(
            src_fd, "rb", closefd=False
        ) as src:
            with gzip.GzipFile(fileobj=gz_raw, mode="rb") as gz:
                while True:
                    want = gz.read(_RETENTION_VERIFY_CHUNK_BYTES)
                    have = src.read(len(want)) if want else b""
                    if want != have:
                        return False
                    if not want:
                        break
            # Any extra source bytes mean the live file grew mid-compress.
            return src.read(1) == b""
    except (OSError, EOFError, gzip.BadGzipFile):
        return False
    finally:
        for fd in (gz_fd, src_fd):
            try:
                os.close(fd)
            except OSError:
                pass


def _stat_signature(st: os.stat_result) -> tuple[int, int, int, int, int]:
    """Complete filesystem identity used to prove a run file is unchanged.

    Any append, rewrite, or same-path recreation moves at least one component
    (``ctime`` advances on every metadata/content change and cannot be forged
    back), so a mismatch between classification and archive means the file was
    not quiescent and must be left alone.
    """
    return (
        int(st.st_dev),
        int(st.st_ino),
        int(st.st_size),
        int(st.st_mtime_ns),
        int(st.st_ctime_ns),
    )


def _read_run_span(path: Path, offset: int, length: int, file_fd: int | None) -> bytes | None:
    """Read ``length`` bytes at ``offset`` from a run file; None on failure.

    ``file_fd`` (fd mode) is the run file opened relative to the pinned session
    directory handle, so the read cannot be redirected by a path swap;
    otherwise the file is opened by path.
    """
    try:
        if file_fd is not None:
            return os.pread(file_fd, max(0, length), offset)
        with path.open("rb") as fh:
            fh.seek(offset)
            return fh.read(length)
    except OSError:
        return None


def _verify_terminal_row_at(
    path: Path, marker_pos: int, size: int, *, file_fd: int | None = None
) -> bool:
    """Prove a COMPLETE top-level terminal journal row owns the marker at ``marker_pos``.

    Hunts backwards for the row start, requires it to begin at a line boundary,
    schema-matches the row header, cross-checks its ids against the path, and —
    the part that matters for a destructive/archival decision — requires the row
    to be **complete**: the row must terminate with a newline at/inside the
    file, so a journal truncated mid-row (the shape most likely to be the only
    copy of something) is NOT classified as terminal. Returns False when it
    cannot prove terminality (fail closed: a missed archive is safe).
    """
    hunt_start = max(0, marker_pos - _RETENTION_ROW_START_HUNT_BYTES - 1)
    # Read from the hunt window to EOF: the completeness check below must see the
    # row's terminating newline, which can sit beyond the marker in a large row.
    tail = _read_run_span(path, hunt_start, size - hunt_start, file_fd)
    if tail is None:
        return False
    search_end = marker_pos - hunt_start
    for _attempt in range(8):  # bounded: real headers sit immediately before the marker
        start_idx = tail.rfind(_RETENTION_ROW_START_BYTES, 0, search_end)
        if start_idx == -1:
            return False
        at_line_start = (
            tail[start_idx - 1 : start_idx] == b"\n" if start_idx > 0 else hunt_start == 0
        )
        if at_line_start:
            header = tail[start_idx : start_idx + _RETENTION_HEADER_MAX_BYTES]
            match = _RETENTION_HEADER_RE.match(header)
            if match:
                event_id, seq_text, row_run_id, row_session_id, terminal_flag = match.group(
                    1, 2, 3, 4, 5
                )
                if terminal_flag == b"true":
                    stem = path.stem.encode()
                    parent = path.parent.name.encode()
                    if row_run_id == stem and row_session_id == parent:
                        try:
                            seq = int(seq_text)
                        except ValueError:
                            return False
                        if event_id == f"{path.stem}:{seq}".encode():
                            # COMPLETENESS: the row must end with a newline
                            # before EOF, and it must parse as a standalone
                            # JSON object. A row truncated mid-write fails this
                            # and is treated as non-terminal.
                            nl = tail.find(b"\n", start_idx)
                            if nl == -1:
                                return False
                            row_bytes = tail[start_idx:nl]
                            try:
                                parsed = json.loads(row_bytes.decode("utf-8"))
                            except (UnicodeDecodeError, ValueError):
                                return False
                            if not isinstance(parsed, dict):
                                return False
                            # A terminal row must also carry a terminal_state of
                            # the terminal shapes this journal produces.
                            if not str(parsed.get("terminal_state") or ""):
                                return False
                            return True
                # A verified non-terminal / foreign header is not this file's
                # terminal row; keep walking earlier candidates.
        search_end = start_idx
    return False


def _journal_file_is_terminal(path: Path, size: int, *, file_fd: int | None = None) -> bool:
    """Return True when the run file provably contains a COMPLETE terminal row.

    Walks the file backwards in bounded chunks, stopping at the first VERIFIED
    terminal row — the common case (terminal row near EOF) reads one chunk. The
    backward walk matters: a multi-megabyte single row (giant ``apperror``
    payload) carries its marker inside its header, so the scan must cross the
    row to reach it. Files whose terminal row sits deeper than the scan budget,
    or whose candidate row is truncated, return False and are left alone.
    """
    if size <= 0:
        return False
    remaining = min(size, _RETENTION_VERIFY_MAX_BYTES)
    overlap = len(_RETENTION_MARKER_BYTES) - 1
    pos = size
    carry = b""
    carry_abs_start: int | None = None
    while remaining > 0 and pos > 0:
        span = min(_RETENTION_VERIFY_CHUNK_BYTES, remaining)
        span_start = max(0, pos - span)
        data = _read_run_span(path, span_start, pos - span_start, file_fd)
        if data is None:
            return False
        buf = data + carry
        data_len = len(data)
        search_end = len(buf)
        while True:
            idx = buf.rfind(_RETENTION_MARKER_BYTES, 0, search_end)
            if idx == -1:
                break
            if idx < data_len:
                abs_pos = span_start + idx
            elif carry_abs_start is not None:
                abs_pos = carry_abs_start + (idx - data_len)
            else:
                abs_pos = -1
            if abs_pos >= 0 and _verify_terminal_row_at(path, abs_pos, size, file_fd=file_fd):
                return True
            search_end = idx
        remaining -= pos - span_start
        carry = data[:overlap]
        carry_abs_start = span_start
        pos = span_start
    return False


def _resolve_retention_cap(
    env_name: str,
    setting_name: str,
    settings: dict,
    *,
    kind: type,
    default,
    minimum,
    maximum,
):
    """Resolve one cap: env var > settings.json > default (mirrors ``_resolve_session_ttl``).

    Invalid or out-of-range values fall through to the next source; a value of
    0 is valid and disables that one cap.
    """
    candidates: list[Any] = [os.getenv(env_name), settings.get(setting_name)]
    for raw in candidates:
        if raw is None or isinstance(raw, bool):
            continue
        try:
            value = float(raw) if kind is float else int(str(raw).strip())
        except (TypeError, ValueError):
            continue
        if minimum <= value <= maximum:
            return value
    return default


def resolve_run_journal_retention_caps() -> dict:
    """Current retention caps as ``{"ttl_days", "max_runs_per_session", "max_bytes_per_session"}``."""
    settings: dict = {}
    try:
        from api.config import load_settings

        loaded = load_settings()
        if isinstance(loaded, dict):
            settings = loaded
    except Exception:
        # Retention must never depend on config import health; defaults stand.
        settings = {}
    return {
        "ttl_days": _resolve_retention_cap(
            _RETENTION_TTL_ENV,
            _RETENTION_TTL_SETTING,
            settings,
            kind=float,
            default=DEFAULT_RUN_JOURNAL_RETENTION_TTL_DAYS,
            minimum=0.0,
            maximum=_RETENTION_TTL_MAX_DAYS,
        ),
        "max_runs_per_session": _resolve_retention_cap(
            _RETENTION_MAX_RUNS_ENV,
            _RETENTION_MAX_RUNS_SETTING,
            settings,
            kind=int,
            default=DEFAULT_RUN_JOURNAL_RETENTION_MAX_RUNS_PER_SESSION,
            minimum=0,
            maximum=_RETENTION_MAX_RUNS_LIMIT,
        ),
        "max_bytes_per_session": _resolve_retention_cap(
            _RETENTION_MAX_BYTES_ENV,
            _RETENTION_MAX_BYTES_SETTING,
            settings,
            kind=int,
            default=DEFAULT_RUN_JOURNAL_RETENTION_MAX_BYTES_PER_SESSION,
            minimum=0,
            maximum=_RETENTION_MAX_BYTES_LIMIT,
        ),
        "archive_ttl_days": _resolve_retention_cap(
            _RETENTION_ARCHIVE_TTL_ENV,
            _RETENTION_ARCHIVE_TTL_SETTING,
            settings,
            kind=float,
            default=DEFAULT_RUN_JOURNAL_ARCHIVE_TTL_DAYS,
            minimum=0.0,
            maximum=_RETENTION_TTL_MAX_DAYS,
        ),
    }


def _cleanup_archive_temps(archive_fd: int) -> None:
    """Recover stray archive-directory debris left by an interrupted pass.

    Two shapes:

      * ``.gz.tmp.`` — an aborted archive attempt (the live file survived, so the
        temp is garbage);
      * ``.prune-claim.`` — a prune that was interrupted after claiming an entry
        (crash/power loss between ``rename`` and ``unlink``). The claimed entry
        may be the ONLY copy of that run, so it is RESTORED to its canonical
        name (never deleted); if the canonical name is occupied the claim is a
        redundant older copy and is dropped.
    """
    try:
        names = sorted(os.listdir(archive_fd))
    except OSError:
        return
    for name in names:
        if ".gz.tmp." in name:
            _unlink_archive_entry(name, archive_fd)
            continue
        marker = ".prune-claim."
        idx = name.find(marker)
        if idx < 0 or not name.startswith("."):
            continue
        original = name[1:idx]
        if not original.endswith(".jsonl.gz"):
            continue
        try:
            st = os.stat(name, dir_fd=archive_fd, follow_symlinks=False)
        except OSError:
            continue
        if not stat.S_ISREG(st.st_mode):
            continue
        # A claim is only restorable when it is OLDER than the prune window —
        # a live prune holds its claim only for microseconds, so anything past
        # the settlement window is debris from an interrupted pass.
        if time.time() - float(st.st_mtime) < _RETENTION_MIN_QUIESCENT_SECONDS:
            continue
        _restore_prune_claim(name, original, archive_fd)


def _sweep_session_entries(
    session_root: Path,
    session_journal_dir: Path,
    session_fd: int,
    archive_fd: int,
    caps: dict,
    now: float,
    counters: dict,
    budget: dict,
) -> None:
    """Body of one session sweep; every operation is fd-relative to ``session_fd``.

    Listing, stat, classification reads, and the final archive move all act on
    the pinned directory handle, so nothing the pathname is swapped to mid-pass
    can redirect any of them.
    """
    try:
        names = sorted(os.listdir(session_fd))
    except OSError:
        counters["errors"] += 1
        return
    entries: list[tuple[str, os.stat_result]] = []
    for name in names:
        if not name.endswith(".jsonl"):
            continue
        # The run id must be a plain filename segment (the same character class
        # as session ids). A filename can never contain "/", but this also
        # rejects the "." / ".." shapes before they can reach `_archive_path`.
        if not _SAFE_ID_RE.fullmatch(name[: -len(".jsonl")]):
            continue
        try:
            st = os.stat(name, dir_fd=session_fd, follow_symlinks=False)
        except OSError:
            continue
        if not stat.S_ISREG(st.st_mode):
            continue
        counters["files_scanned"] += 1
        entries.append((name, st))
    # Newest first: the caps keep the most recent runs live.
    entries.sort(key=lambda entry: (entry[1].st_mtime_ns, entry[0]), reverse=True)

    ttl_days = float(caps["ttl_days"])
    max_runs = int(caps["max_runs_per_session"])
    max_bytes = int(caps["max_bytes_per_session"])
    retained_bytes = 0
    size_cap_exceeded = False
    for rank, (name, st) in enumerate(entries):
        size = int(st.st_size)
        archived = False
        if budget["remaining"] > 0:
            age_seconds = now - float(st.st_mtime)
            # Settlement window: never archive a file a writer may still be
            # appending to (post-terminal `metering` / `stream_end` rows arrive
            # around the terminal row) or a client may still be reconnecting to.
            if age_seconds >= _RETENTION_MIN_QUIESCENT_SECONDS:
                over_ttl = ttl_days > 0 and age_seconds > ttl_days * 86400.0
                over_count = max_runs > 0 and rank >= max_runs
                # Size cap: archive from the newest-first prefix once the retained
                # budget would be exceeded, and keep archiving everything older
                # than the first overflow (sticky) so the live set is a contiguous
                # newest-first prefix. The newest run (rank 0) is exempt so a
                # session always keeps its most recent anchor; the TTL still
                # archives it once it is old enough.
                over_size = False
                if max_bytes > 0 and rank > 0:
                    if size_cap_exceeded or (retained_bytes + size) > max_bytes:
                        size_cap_exceeded = True
                        over_size = True
                if over_ttl or over_count or over_size:
                    archived = _try_archive_entry(
                        session_root,
                        session_journal_dir,
                        name,
                        st,
                        session_fd,
                        archive_fd,
                        counters,
                        size,
                        age_seconds,
                        budget,
                    )
        if not archived:
            # Everything that stays live — including a run that could not be
            # proven terminal — counts against the per-session budget.
            retained_bytes += size


def _try_archive_entry(
    session_root: Path,
    session_journal_dir: Path,
    name: str,
    st: os.stat_result,
    session_fd: int,
    archive_fd: int,
    counters: dict,
    size: int,
    age_seconds: float,
    budget: dict,
) -> bool:
    """Attempt to archive one eligible run; True when it was archived."""
    try:
        file_fd = os.open(name, os.O_RDONLY | _O_NOFOLLOW, dir_fd=session_fd)
    except OSError:
        return False
    try:
        if not _journal_file_is_terminal(
            session_journal_dir / name, int(st.st_size), file_fd=file_fd
        ):
            counters["retained_open"] += 1
            return False
    finally:
        try:
            os.close(file_fd)
        except OSError:
            pass
    # Archive under the same per-path lock appends use, so a trailing write
    # either lands before (identity changes -> skip) or waits until the move is
    # done. NOTE: the seq/summary caches are intentionally NOT evicted here —
    # `_read_jsonl` merges the archive back in, so a hypothetical post-archive
    # append still continues seqs correctly (N+1, not a restart at 1).
    path = session_journal_dir / name
    with _lock_for(path):
        archived_bytes = _archive_run_file(
            path,
            _archive_path(session_journal_dir.name, path.stem, session_root),
            session_fd,
            archive_fd,
            _stat_signature(st),
        )
        if archived_bytes > 0:
            # Cached summary keyed on the (now gone) live file is stale; drop it
            # so the next read re-derives from the archive.
            # `_summary_cache_signature` also falls back to the archive, so a
            # later read re-caches correctly.
            _discard_cached_summary(path)
    if archived_bytes > 0:
        counters["archived_files"] += 1
        counters["archived_bytes"] += archived_bytes
        budget["remaining"] -= archived_bytes
        logger.debug(
            "Run-journal retention archived %s (%s bytes, age %.1fd)",
            path,
            archived_bytes,
            age_seconds / 86400.0,
        )
        return True
    counters["skipped_files"] += 1
    return False


def _open_archive_dir(session_root: Path, session_id: str) -> int:
    """Open (creating if needed) the pinned archive directory for a session.

    Raises OSError when it cannot be created/opened; the caller treats that as
    "skip this run" (fail closed).
    """
    archive_dir = _archive_dir(session_root) / session_id
    try:
        archive_dir.mkdir(parents=True, exist_ok=True)
    except OSError:
        pass
    fd = _open_dir_no_follow(archive_dir)
    if fd is None:
        raise OSError(f"cannot open archive dir {archive_dir}")
    return fd


def _sweep_run_journal_session(
    session_root: Path,
    session_journal_dir: Path,
    caps: dict,
    now: float,
    counters: dict,
    budget: dict,
) -> None:
    """Archive eligible runs inside one session's journal dir.

    The session dir is PINNED as an open handle (``dir_fd``) for the whole pass,
    so every stat, classification read, and move acts on the directory's inode —
    a swap of the pathname to a symlink part-way through cannot redirect any of
    them. Sessions that cannot be pinned are skipped (fail closed).
    """
    session_fd = _open_dir_no_follow(session_journal_dir)
    if session_fd is None:
        return
    archive_fd = -1
    try:
        archive_fd = _open_archive_dir(session_root, session_journal_dir.name)
        _cleanup_archive_temps(archive_fd)
        _sweep_session_entries(
            session_root, session_journal_dir, session_fd, archive_fd, caps, now, counters, budget
        )
    except OSError:
        pass
    finally:
        for fd in (archive_fd, session_fd):
            if fd >= 0:
                try:
                    os.close(fd)
                except OSError:
                    pass


def _prune_run_journal_archive(session_root: Path, caps: dict, now: float, counters: dict) -> None:
    """Delete archived runs older than the archive TTL (the ONLY destructive step).

    Disabled by default (`archive_ttl_days = 0`), so a stock install never
    deletes anything the retention feature touched.
    """
    ttl_days = float(caps.get("archive_ttl_days") or 0.0)
    if ttl_days <= 0:
        return
    archive_root = _archive_dir(session_root)
    if not archive_root.exists():
        return
    cutoff = now - ttl_days * 86400.0
    try:
        session_dirs = sorted(archive_root.iterdir())
    except OSError:
        return
    for session_dir in session_dirs:
        if session_dir.is_symlink() or not session_dir.is_dir():
            continue
        if not _SAFE_ID_RE.fullmatch(session_dir.name):
            continue
        session_fd = _open_dir_no_follow(session_dir)
        if session_fd is None:
            continue
        try:
            for name in sorted(os.listdir(session_fd)):
                if not name.endswith(".jsonl.gz"):
                    continue
                try:
                    st = os.stat(name, dir_fd=session_fd, follow_symlinks=False)
                except OSError:
                    continue
                if float(st.st_mtime) >= cutoff:
                    continue
                # Identity is re-asserted inside the unlink (see
                # `_prune_archive_entry`): a name republished since the age
                # check must survive.
                if _prune_archive_entry(name, st, session_fd):
                    counters["pruned_archives"] += 1
        finally:
            try:
                os.close(session_fd)
            except OSError:
                pass


def sweep_run_journal(
    *,
    session_dir: Path | None = None,
    ttl_days: float | None = None,
    max_runs_per_session: int | None = None,
    max_bytes_per_session: int | None = None,
    now: float | None = None,
) -> dict:
    """Archive run journals past the age / count / size caps (#7613).

    An archive, not a delete: eligible runs are compressed into
    ``_run_journal_archive/`` and the live file is only dropped once the
    compressed copy is proven to decompress back to the exact bytes. Read paths
    fall back to the archive, so a wrong classification stays recoverable.
    Non-terminal runs are never touched. Caps may be passed explicitly (0
    disables one cap; tests rely on this) or left ``None`` to resolve env var >
    settings.json > default via ``resolve_run_journal_retention_caps``.

    Returns a counters dict (``archived_files``, ``archived_bytes``,
    ``files_scanned``, ``retained_open``, ``pruned_archives``,
    ``sessions_scanned``, ``skipped_files``, ``errors``, ``caps``).
    """
    root = Path(session_dir) if session_dir is not None else _default_session_dir()
    caps = resolve_run_journal_retention_caps()
    if ttl_days is not None:
        caps["ttl_days"] = max(0.0, float(ttl_days))
    if max_runs_per_session is not None:
        caps["max_runs_per_session"] = max(0, int(max_runs_per_session))
    if max_bytes_per_session is not None:
        caps["max_bytes_per_session"] = max(0, int(max_bytes_per_session))
    counters = {
        "archived_files": 0,
        "archived_bytes": 0,
        "files_scanned": 0,
        "retained_open": 0,
        "pruned_archives": 0,
        "sessions_scanned": 0,
        "skipped_files": 0,
        "errors": 0,
        "caps": dict(caps),
    }
    journal_root = root / RUN_JOURNAL_DIR_NAME
    if not _DIR_FD_OK:
        # Fail closed: pinned directory-relative operations are what make the
        # move race-safe. Where the platform cannot provide them (Windows),
        # retention is disabled entirely rather than falling back to path-based
        # moves a directory swap can redirect.
        logger.info(
            "Run-journal retention disabled: this platform has no dir_fd support "
            "(pinned-directory moves unavailable)"
        )
        return counters
    try:
        if not journal_root.exists():
            return counters
        journal_root_real = os.path.realpath(journal_root)
        session_dirs = [
            entry
            for entry in sorted(journal_root.iterdir())
            if not entry.is_symlink()
            and entry.is_dir()
            and _resolve_within(journal_root_real, entry)
        ]
    except OSError:
        counters["errors"] += 1
        return counters
    sweep_now = time.time() if now is None else float(now)
    budget = {"remaining": _RETENTION_ARCHIVE_BYTES_PER_SWEEP}
    with _SWEEP_RUN_LOCK:
        for session_journal_dir in session_dirs:
            if not _SAFE_ID_RE.fullmatch(session_journal_dir.name):
                continue
            counters["sessions_scanned"] += 1
            try:
                _sweep_run_journal_session(
                    root, session_journal_dir, caps, sweep_now, counters, budget
                )
            except Exception:
                counters["errors"] += 1
                logger.warning(
                    "Run-journal retention sweep failed for %s",
                    session_journal_dir,
                    exc_info=True,
                )
        try:
            _prune_run_journal_archive(root, caps, sweep_now, counters)
        except Exception:
            counters["errors"] += 1
            logger.warning("Run-journal archive prune failed", exc_info=True)
    if counters["archived_files"]:
        logger.info(
            "Run-journal retention archived %d file(s) / %d bytes across %d session(s)",
            counters["archived_files"],
            counters["archived_bytes"],
            counters["sessions_scanned"],
        )
    return counters


def _resolve_within(root_real: str, path: Path) -> bool:
    """True when ``path``'s fully-resolved location stays inside ``root_real``.

    Boundary-aware: a sibling directory whose name merely starts with the root's
    prefix is not "inside" it.
    """
    try:
        candidate = os.path.realpath(path)
    except OSError:
        return False
    if candidate == root_real:
        return True
    return candidate.startswith(root_real + os.sep)


def run_journal_sweep_enabled() -> bool:
    """False when ``HERMES_WEBUI_RUN_JOURNAL_SWEEP`` is set to a disabled value."""
    raw = os.getenv(RUN_JOURNAL_SWEEP_ENV, "").strip().lower()
    return raw not in _SWEEP_DISABLED_VALUES


def maybe_sweep_run_journal(now: float | None = None) -> dict | None:
    """Run one retention sweep if due; return its counters, or None when not due.

    Called from the shared maintenance tick in ``api.background_process``
    alongside the SessionChannel reaper — the sweep is hourly and never runs on
    any request path. Gating state lives here so any caller — the tick, a test,
    a manual invocation — shares it.
    """
    global _SWEEP_LAST_STARTED
    if not run_journal_sweep_enabled():
        return None
    tick_now = time.time() if now is None else float(now)
    with _SWEEP_THREAD_LOCK:
        if _SWEEP_LAST_STARTED is None:
            # Fresh process: arm so the first pass is due after
            # RETENTION_FIRST_SWEEP_DELAY_SECS (never during boot itself),
            # then one sweep per RETENTION_SWEEP_INTERVAL_SECS after that.
            _SWEEP_LAST_STARTED = tick_now - (
                RETENTION_SWEEP_INTERVAL_SECS - RETENTION_FIRST_SWEEP_DELAY_SECS
            )
            return None
        if tick_now - _SWEEP_LAST_STARTED < RETENTION_SWEEP_INTERVAL_SECS:
            return None
        _SWEEP_LAST_STARTED = tick_now
    try:
        return sweep_run_journal()
    except Exception:
        logger.warning("Run-journal retention sweep failed", exc_info=True)
        return None


def _reset_run_journal_sweep_schedule() -> None:
    """Forget the sweep schedule so the next due-check re-arms the boot delay.

    Used by tests to prove the first-pass delay independently of process age.
    """
    global _SWEEP_LAST_STARTED
    with _SWEEP_THREAD_LOCK:
        _SWEEP_LAST_STARTED = None


def stale_interrupted_event(session_id: str, run_id: str, *, after_seq: int | None = None) -> dict | None:
    summary = latest_run_summary(session_id, run_id)
    if summary.get("terminal") or not summary.get("event_count"):
        return None
    seq = int(summary.get("last_seq") or 0) + 1
    if after_seq is not None and seq <= int(after_seq):
        return None
    payload = {
        "type": "interrupted",
        "recovery_control": True,
        "message": "The live worker stopped before this run finished.",
        "hint": "The transcript was restored to the last journaled event. Start a new turn if you still need the task to continue.",
        "session_id": session_id,
        "stream_id": run_id,
        "journal_last_seq": summary.get("last_seq"),
    }
    return {
        "version": 1,
        "event_id": f"{run_id}:{seq}",
        "seq": seq,
        "run_id": run_id,
        "session_id": session_id,
        "event": "apperror",
        "type": "apperror",
        "created_at": time.time(),
        "terminal": True,
        "terminal_state": "lost-worker-bookkeeping",
        "payload": payload,
        "synthetic": True,
    }
