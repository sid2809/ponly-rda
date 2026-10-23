"""SQLite state. Every paid job ID is written here before we wait on it."""
import json
import os
import sqlite3
import time
from contextlib import contextmanager

from . import config

SCHEMA = """
CREATE TABLE IF NOT EXISTS articles (
  id TEXT PRIMARY KEY, row_num INTEGER, url TEXT, final_url TEXT, title TEXT,
  category TEXT, status TEXT DEFAULT 'queued', skip_reason TEXT, error TEXT,
  alcohol INTEGER, alcohol_reason TEXT, theme TEXT, wave INTEGER, pilot_side TEXT,
  image_model TEXT, copy_json TEXT, copy_attempts INTEGER DEFAULT 0,
  image_attempts INTEGER DEFAULT 0, check_failed_once INTEGER DEFAULT 0,
  landscape_path TEXT, square_path TEXT, regen_cost REAL DEFAULT 0,
  adgroup_key TEXT, landscape_asset TEXT, square_asset TEXT, ad_resource TEXT,
  ad_id TEXT, ad_attempts INTEGER DEFAULT 0, approval TEXT, policy_note TEXT, impressions INTEGER, clicks INTEGER,
  updated REAL
);
CREATE TABLE IF NOT EXISTS jobs (
  id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT, wave INTEGER, external_id TEXT,
  state TEXT, payload TEXT, created REAL, updated REAL
);
CREATE TABLE IF NOT EXISTS predictions (
  id TEXT PRIMARY KEY, article_id TEXT, model TEXT, state TEXT, created REAL
);
CREATE TABLE IF NOT EXISTS themes (category TEXT PRIMARY KEY, themes TEXT);
CREATE TABLE IF NOT EXISTS campaigns (
  key TEXT PRIMARY KEY, wave INTEGER, pool TEXT, image_model TEXT, nn INTEGER,
  theme TEXT, name TEXT, n INTEGER, budget_res TEXT, campaign_res TEXT, geo_res TEXT, lang_res TEXT,
  status TEXT DEFAULT 'planned', enabled_at REAL
);
CREATE TABLE IF NOT EXISTS adgroups (
  key TEXT PRIMARY KEY, campaign_key TEXT, nn INTEGER, theme TEXT, name TEXT,
  n INTEGER, alcohol INTEGER, adgroup_res TEXT, exclusion_res TEXT
);
CREATE TABLE IF NOT EXISTS quota_log (ts REAL, ops INTEGER, kind TEXT);
CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT);
"""


def connect(path=None):
    path = path or config.DB_PATH
    os.makedirs(os.path.dirname(path), exist_ok=True)
    con = sqlite3.connect(path, timeout=30, isolation_level=None)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.executescript(SCHEMA)
    for table, col, decl in (("campaigns", "error", "TEXT"), ("campaigns", "attempts", "INTEGER DEFAULT 0"),
                             ("adgroups", "error", "TEXT"), ("adgroups", "attempts", "INTEGER DEFAULT 0"),
                             ("articles", "check_api_errors", "INTEGER DEFAULT 0"),
                             ("campaigns", "labels_done", "TEXT"),
                             ("campaigns", "label_attempts", "INTEGER DEFAULT 0"),
                             ("articles", "api_errors", "INTEGER DEFAULT 0"),
                             ("articles", "big_tokens", "INTEGER DEFAULT 0"),
                             ("articles", "note", "TEXT"),
                             ("adgroups", "excl_attempts", "INTEGER DEFAULT 0"),
                             ("campaigns", "enable_attempts", "INTEGER DEFAULT 0"),
                             ("articles", "review_status", "TEXT"),
                             ("articles", "asset_policy", "TEXT"),
                             ("articles", "cost", "REAL")):
        have = {r[1] for r in con.execute(f"PRAGMA table_info({table})")}
        if col not in have:
            con.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl}")
    return con


_pending_logs = {}     # log lines written inside a transaction: printed only after COMMIT


def _log_after_commit(con, line):
    if con.in_transaction:
        _pending_logs.setdefault(id(con), []).append(line)
    else:
        from .log import log
        log(line)


@contextmanager
def tx(con):
    """BEGIN IMMEDIATE ... COMMIT; ROLLBACK on any exception (R6).
    Never make network calls inside this block."""
    con.execute("BEGIN IMMEDIATE")
    try:
        yield
    except BaseException:
        con.execute("ROLLBACK")
        _pending_logs.pop(id(con), None)
        raise
    con.execute("COMMIT")
    from .log import log
    for line in _pending_logs.pop(id(con), []):
        log(line)


def rollback_if_open(con):
    if con.in_transaction:
        con.execute("ROLLBACK")
    _pending_logs.pop(id(con), None)


def kv_get(con, k, default=None):
    r = con.execute("SELECT v FROM kv WHERE k=?", (k,)).fetchone()
    return json.loads(r["v"]) if r else default


def kv_set(con, k, v):
    con.execute("INSERT OR REPLACE INTO kv(k,v) VALUES(?,?)", (k, json.dumps(v)))


def update_article(con, aid, **fields):
    if fields.get("status") in ("skipped", "error"):
        from .log import short
        r = con.execute("SELECT title, wave FROM articles WHERE id=?", (aid,)).fetchone()
        why = fields.get("skip_reason") or fields.get("error") or ""
        _log_after_commit(con, f"article {fields['status'].upper()}: {aid} "
                               f"'{short(r['title'] if r else '', 60)}' "
                               f"(wave {r['wave'] if r else '?'}): {short(why, 300)}")
    fields["updated"] = time.time()
    cols = ", ".join(f"{k}=?" for k in fields)
    con.execute(f"UPDATE articles SET {cols} WHERE id=?", (*fields.values(), aid))


def log_ops(con, ops, kind):
    if ops:
        con.execute("INSERT INTO quota_log VALUES(?,?,?)", (time.time(), int(ops), kind))


def ops_last_24h(con):
    r = con.execute("SELECT COALESCE(SUM(ops),0) s FROM quota_log WHERE ts>?",
                    (time.time() - 86400,)).fetchone()
    return int(r["s"])


def new_job(con, kind, wave=None, payload=None, external_id=None, state="created"):
    now = time.time()
    cur = con.execute(
        "INSERT INTO jobs(kind,wave,external_id,state,payload,created,updated) VALUES(?,?,?,?,?,?,?)",
        (kind, wave, external_id, state, json.dumps(payload or {}), now, now))
    return cur.lastrowid


def set_job(con, jid, **fields):
    if "payload" in fields:
        fields["payload"] = json.dumps(fields["payload"])
    fields["updated"] = time.time()
    cols = ", ".join(f"{k}=?" for k in fields)
    con.execute(f"UPDATE jobs SET {cols} WHERE id=?", (*fields.values(), jid))


def open_jobs(con, kind):
    return con.execute("SELECT * FROM jobs WHERE kind=? AND state NOT IN ('done','abandoned','lost')"
                       " ORDER BY id", (kind,)).fetchall()
