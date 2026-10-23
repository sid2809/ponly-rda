"""SQLite state. Every paid job ID is written here before we wait on it."""
import json
import os
import sqlite3
import time

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
                             ("articles", "check_api_errors", "INTEGER DEFAULT 0")):
        have = {r[1] for r in con.execute(f"PRAGMA table_info({table})")}
        if col not in have:
            con.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl}")
    return con


def kv_get(con, k, default=None):
    r = con.execute("SELECT v FROM kv WHERE k=?", (k,)).fetchone()
    return json.loads(r["v"]) if r else default


def kv_set(con, k, v):
    con.execute("INSERT OR REPLACE INTO kv(k,v) VALUES(?,?)", (k, json.dumps(v)))


def update_article(con, aid, **fields):
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
    return con.execute("SELECT * FROM jobs WHERE kind=? AND state NOT IN ('done','abandoned')"
                       " ORDER BY id", (kind,)).fetchall()
