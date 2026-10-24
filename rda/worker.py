"""Main loop. Safe to stop and restart at any point: all state is in SQLite.

Safety rules (code review fix plan P1-P21, R1-R21):
- every step runs isolated; a failing step is reported in Summary and backs off
- every paid job row stores its article IDs before the paid call; restart recovery
  reverts only articles no open job covers
- API errors never count as content failures, but are capped so nothing is re-paid forever
- the Sheet reports are written every tick, whatever failed
"""
import hashlib
import json
import os
import random
import shutil
import signal
import threading
import time
import traceback
import uuid

from . import alcohol, config, db, images, lint, llm, split
from .gads import QuotaExhausted, is_quota_error
from .log import log as _log, short
from .sheet import OUTPUT_HEADER

TERMINAL = ("planned", "uploaded", "skipped", "error")
IN_PROGRESS = ("themed", "copy_pending", "copy_done", "image_pending", "image_done",
               "check_pending")
MAX_INFLIGHT_IMAGES = 40
CHECK_CHUNK = 100            # P7: image check requests built 100 at a time
COPY_CHUNK = 100
API_ERROR_CAP = 5            # R1
ALCOHOL_API_FALLBACK = 3     # R1: after 3 API errors a title is treated as alcohol
CHECK_API_CAP = 2            # Siddharth: image check gives up after 2 failed attempts
BACKOFF_S = (600, 1800, 3600)
BIG = {"copy": 1, "check": 2, "alcohol": 4}   # per-article doubled max_tokens flag (R19)
# kind -> (pending status, status to revert to)
PENDING_OF = {"alcohol": ("alcohol_pending", "url_ok"), "copy": ("copy_pending", "themed"),
              "check": ("check_pending", "image_done")}
CID_PREFIX = {"alcohol": "alc", "copy": "copy", "check": "chk"}


def article_id(url):
    return hashlib.sha1(url.strip().encode()).hexdigest()[:12]


def cid_article(cid):
    return cid.split("-")[1]


def run_now(claude, reqs, workers=6):
    """Regular (non-batch) API in parallel; same shape as batch_results:
    {custom_id: ("ok", data) | (kind, message)}. Used for the pilot."""
    from concurrent.futures import ThreadPoolExecutor

    def one(item):
        cid, params = item
        try:
            return cid, ("ok", claude.sync_json(params))
        except llm.LLMError as e:
            return cid, (e.kind, e.msg)
        except Exception as e:
            return cid, ("api", f"{type(e).__name__}: {str(e)[:200]}")
    with ThreadPoolExecutor(workers) as ex:
        return dict(ex.map(one, reqs))


def _chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


class Worker:
    def __init__(self, con, sheet, claude, rep, ads_factory):
        self.con, self.sheet, self.claude, self.rep = con, sheet, claude, rep
        self._ads, self._ads_factory = None, ads_factory
        self.settings = {}
        self.settings_problems = []
        self.stop = threading.Event()
        self.token = uuid.uuid4().hex     # R20: per-process lease token
        self._lease_warned = False

    @property
    def ads(self):
        if self._ads is None:
            self._ads = self._ads_factory(self.con)
            self._ads.progress = self.heartbeat      # keeps the lease fresh during long uploads
        return self._ads

    def log(self, msg):
        _log(msg)

    def _log_outcome(self, kind, wave, ids):
        """One summary line per finished Claude job: where its articles ended up."""
        if not ids:
            return
        q = ",".join("?" * len(ids))
        st = {}
        for r in self.con.execute(f"SELECT status, COUNT(*) n FROM articles WHERE id IN ({q}) "
                                  f"GROUP BY status", ids):
            st[r["status"]] = r["n"]
        extra = ""
        if kind == "alcohol":
            r = self.con.execute(f"SELECT SUM(alcohol=1) a, SUM(alcohol=0) f FROM articles WHERE "
                                 f"id IN ({q}) AND status='tagged'", ids).fetchone()
            extra = f"; alcohol {r['a'] or 0}, food {r['f'] or 0}"
        meaning = {"copy_done": "passed", "themed": "retry", "checked": "passed",
                   "image_done": "retry check", "copy_done@check": "new image",
                   "url_ok": "retry", "tagged": "tagged", "skipped": "skipped"}
        parts = []
        for s, n in sorted(st.items()):
            label = meaning.get(f"{s}@{kind}", meaning.get(s, s))
            parts.append(f"{label} {n}")
        self.log(f"{kind} results (wave {wave}): {len(ids)} articles -> {', '.join(parts)}{extra}")

    # ------------------------------------------------------------ settings
    def s_bool(self, key):
        v = (self.settings or {}).get(key, False)
        return v is True or str(v).strip().upper() == "TRUE"

    def paused(self):
        """R7: unreadable Settings or a missing Pause row count as paused. Fails closed: only
        an unticked box (FALSE) means running; anything else ('yes', '1', text) is paused."""
        if self.settings is None or "Pause" not in self.settings:
            return True
        v = self.settings.get("Pause")
        return not (v is False or str(v).strip().upper() == "FALSE")

    def num(self, key, default):
        """P16: accepts '14,000'; a bad value falls back to the default and is reported."""
        v = (self.settings or {}).get(key)
        if v in (None, ""):
            return default
        try:
            return int(float(str(v).replace(",", "").strip()))
        except (TypeError, ValueError):
            self.settings_problems.append(f"'{key}' = '{v}' is not a number; using {default}")
            return default

    def sizes(self):
        """(campaign min, max, ad group min, max) from Settings; None if invalid."""
        keys = ["Articles per campaign (min)", "Articles per campaign (max)",
                "Articles per ad group (min)", "Articles per ad group (max)"]
        defaults = [config.CAMPAIGN_MIN, config.CAMPAIGN_MAX, config.ADGROUP_MIN,
                    config.ADGROUP_MAX]
        v = [self.num(k, d) for k, d in zip(keys, defaults)]
        ok = 1 <= v[0] <= v[1] and 1 <= v[2] <= v[3] and v[3] <= v[1]
        db.kv_set(self.con, "sizes_error", None if ok else
                  f"Invalid size settings {v}: need min <= max and ad group max <= campaign max")
        return tuple(v) if ok else None

    def chosen_model(self):
        m = str((self.settings or {}).get("Image model", "")).strip().lower()
        m = m.replace(" ", "-").replace("_", "-")
        if m and m not in config.IMAGE_MODELS:
            self.settings_problems.append(f"Image model '{m}' unknown (use: "
                                          f"{', '.join(config.IMAGE_MODELS)})")
        return m if m in config.IMAGE_MODELS else None

    def quota_cap(self):
        cap = self.num("Daily API quota cap", config.DEFAULT_QUOTA_CAP)
        if str((self.settings or {}).get("Access level", "")).strip().lower() == "explorer":
            cap = min(cap, config.EXPLORER_QUOTA_CAP)       # P17
        return cap

    # --------------------------------------------------------------- lease
    def acquire_lease(self):
        """R11/R20: only one worker may use the database at a time."""
        now = time.time()
        with db.tx(self.con):
            lease = db.kv_get(self.con, "lease")
            if lease and lease["token"] != self.token and now - lease["ts"] < config.LEASE_STALE_S:
                return False
            db.kv_set(self.con, "lease", {"token": self.token, "ts": now})
        return True

    def heartbeat(self):
        lease = db.kv_get(self.con, "lease")
        if lease and lease["token"] == self.token and not self.con.in_transaction:
            db.kv_set(self.con, "lease", {"token": self.token, "ts": time.time()})

    def release_lease(self):
        try:
            db.rollback_if_open(self.con)
            lease = db.kv_get(self.con, "lease")
            if lease and lease["token"] == self.token:
                self.con.execute("DELETE FROM kv WHERE k='lease'")
        except Exception:
            pass

    # ------------------------------------------------------ step isolation
    def run_step(self, key, fn, *args, backoff=True):
        """P2/R3/R8: one step, isolated. Failures are recorded (Summary 'Last errors');
        after 3 in a row the step (for that pool/wave) waits 10 -> 30 -> 60 min."""
        if self.stop.is_set():
            return
        errs = db.kv_get(self.con, "step_errors", {})
        e0 = errs.get(key, {})
        if backoff and e0.get("until", 0) > time.time():
            return
        try:
            fn(*args)
            if e0.get("count"):
                e0.update(count=0, until=0, recovered=time.strftime("%Y-%m-%d %H:%M UTC",
                                                                   time.gmtime()))
                errs[key] = e0
                db.kv_set(self.con, "step_errors", errs)
        except QuotaExhausted as e:
            db.rollback_if_open(self.con)
            temporary = "TEMPORARILY" in str(e)
            wait = 300 if temporary else 3600
            db.kv_set(self.con, "quota_wait_until", time.time() + wait)
            db.kv_set(self.con, "quota_wait_reason", f"{key}: {short(e, 200)}")
            self.log(f"{key}: Google Ads {'rate limit' if temporary else 'quota'} hit, waiting "
                     f"{wait // 60} min ({e})")
        except Exception as e:
            db.rollback_if_open(self.con)
            if is_quota_error(e):                     # a Google Ads limit was hit
                temporary = "TEMPORARILY" in str(e) + str(getattr(e, "failure", ""))
                wait = 300 if temporary else 3600
                db.kv_set(self.con, "quota_wait_until", time.time() + wait)
                db.kv_set(self.con, "quota_wait_reason",
                          f"{key}: {'short-term rate limit' if temporary else 'daily quota'}: "
                          f"{short(e, 200)}")
                self.log(f"{key}: Google Ads {'rate limit' if temporary else 'quota'} hit, "
                         f"waiting {wait // 60} min ({short(e, 200)})")
                return
            n = e0.get("count", 0) + 1
            until = time.time() + BACKOFF_S[min(n - 3, 2)] if (backoff and n >= 3) else 0
            errs = db.kv_get(self.con, "step_errors", {})
            errs[key] = {"error": f"{type(e).__name__}: {e}"[:400], "count": n, "until": until,
                         "time": time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime())}
            db.kv_set(self.con, "step_errors", errs)
            self.log(f"step {key} failed ({n} in a row):\n" + traceback.format_exc())
        finally:
            self.heartbeat()

    # ------------------------------------------------------- job lifecycle
    def _mark_pending(self, kind, ids):
        pend, back = PENDING_OF[kind]
        extra = ", copy_attempts=copy_attempts+1" if kind == "copy" else ""
        for aid in ids:
            self.con.execute(f"UPDATE articles SET status=?, updated=?{extra} WHERE id=? AND "
                             f"status=?", (pend, time.time(), aid, back))

    def _submit(self, kind, wave, reqs):
        """P1/R4/R5: save the job row with its IDs, create the batch, then store the batch
        ID and set statuses together. A failed create changes nothing."""
        ids = [cid_article(c) for c, _ in reqs]
        jid = db.new_job(self.con, kind, wave, payload={"ids": ids, "cids": [c for c, _ in reqs]},
                         state="creating")
        try:
            bid = self.claude.create_batch(reqs)
        except Exception:
            # the batch may exist anyway (e.g. timeout after Anthropic accepted it): leave the
            # row 'creating' so recovery looks for it before anything is sent again
            raise
        self.log(f"{kind} batch {bid} created ({len(reqs)} requests, wave {wave})")
        with db.tx(self.con):
            db.set_job(self.con, jid, external_id=bid, state="running")
            self._mark_pending(kind, ids)
        return bid

    def _run_sync(self, kind, wave, reqs, apply_fn):
        """Pilot: regular API. The job row (state 'sync') lists the IDs, so a crash in the
        middle is reverted by recovery (R4)."""
        ids = [cid_article(c) for c, _ in reqs]
        jid = db.new_job(self.con, kind, wave, payload={"ids": ids, "cids": [c for c, _ in reqs]},
                         state="sync")
        with db.tx(self.con):
            self._mark_pending(kind, ids)
        res = run_now(self.claude, reqs)
        self.heartbeat()
        with db.tx(self.con):
            apply_fn(res)
            self._revert_unanswered(kind, ids, res)
            db.set_job(self.con, jid, state="done")
        self._log_outcome(kind, wave, ids)

    def _revert_unanswered(self, kind, ids, res):
        answered = {cid_article(c) for c in res}
        for aid in ids:
            if aid in answered:
                continue
            a = self.con.execute("SELECT * FROM articles WHERE id=?", (aid,)).fetchone()
            if a and a["status"] == PENDING_OF[kind][0]:
                self._api_error(kind, a, "no result returned")

    def _finish_job(self, kind, j, apply_fn):
        """Returns True if the job is finished (applied or abandoned)."""
        if not j["external_id"]:
            return False        # 'creating' rows are handled by recovery
        if not self.claude.batch_ended(j["external_id"]):
            return False
        res = self.claude.batch_results(j["external_id"])
        payload = json.loads(j["payload"] or "{}")
        cids = set(payload.get("cids") or [])
        if cids and (not res or not set(res) <= cids):     # R19: adopted the wrong batch?
            db.set_job(self.con, j["id"], state="abandoned")
            raise RuntimeError(f"{kind} batch {j['external_id']} results do not match the "
                               f"job's articles; not applied")
        with db.tx(self.con):
            apply_fn(res)
            if payload.get("ids"):
                self._revert_unanswered(kind, payload["ids"], res)
            db.set_job(self.con, j["id"], state="done")
        self._log_outcome(kind, j["wave"], payload.get("ids") or [cid_article(c) for c in res])
        errors = sum(1 for k, _ in res.values() if k != "ok")
        if errors:
            db.kv_set(self.con, "batch_errors_total",
                      db.kv_get(self.con, "batch_errors_total", 0) + errors)
        self.log(f"{kind} batch {j['external_id']}: {len(res)} results, {errors} errors")
        return True

    def recover(self):
        """R4/R5/R19/R21: runs every tick, before any step."""
        con = self.con
        for kind in PENDING_OF:
            for j in db.open_jobs(con, kind):
                if j["state"] == "sync":            # a pilot call never survives a tick
                    db.set_job(con, j["id"], state="abandoned")
                elif j["state"] == "creating" and not j["external_id"]:
                    self._adopt_or_abandon(kind, j)
        for kind, (pend, back) in PENDING_OF.items():
            covered, legacy = set(), False
            for j in db.open_jobs(con, kind):
                ids = json.loads(j["payload"] or "{}").get("ids")
                if ids is None:
                    legacy = True                    # old job without IDs: leave its articles
                else:
                    covered |= set(ids)
            if legacy:
                continue
            rows = con.execute("SELECT id, copy_attempts FROM articles WHERE status=?",
                               (pend,)).fetchall()
            orphans = [r for r in rows if r["id"] not in covered]
            if not orphans:
                continue
            with db.tx(con):
                for r in orphans:
                    fields = {"status": back}
                    if kind == "copy":
                        fields["copy_attempts"] = max(0, (r["copy_attempts"] or 0) - 1)
                    db.update_article(con, r["id"], **fields)
            self.log(f"recovery: {len(orphans)} {pend} articles had no open job; reverted to {back}")
        # an image whose prediction row is finished/missing must not wait forever
        stuck = [r["id"] for r in con.execute(
            "SELECT a.id FROM articles a WHERE a.status='image_pending' AND NOT EXISTS ("
            "SELECT 1 FROM predictions p WHERE p.article_id=a.id AND p.state NOT IN "
            "('done','failed'))")]
        if stuck:
            with db.tx(con):
                for aid in stuck:
                    db.update_article(con, aid, status="copy_done",
                                      error="image interrupted; will be made again")
            self.log(f"recovery: {len(stuck)} image_pending articles had no open prediction; "
                     f"back to copy_done")

    def _adopt_or_abandon(self, kind, j):
        """A 'creating' row with no batch ID: the process died during create_batch.
        Adopt a matching batch only if exactly one exists (R19/R21)."""
        payload = json.loads(j["payload"] or "{}")
        n = len(payload.get("cids") or [])
        found = []
        if n:
            try:
                # create may take until the client timeout (10 min) before it fails
                found = self.claude.find_batch(j["created"] - 10, j["created"] + 900, n)
            except Exception as e:
                self.log(f"find_batch failed: {e}")
                return                                   # try again next tick
            owned = {r[0] for r in self.con.execute(
                "SELECT external_id FROM jobs WHERE external_id IS NOT NULL")}
            found = [b for b in found if b not in owned]   # never take another job's batch
        if len(found) == 1:
            with db.tx(self.con):
                db.set_job(self.con, j["id"], external_id=found[0], state="running")
                self._mark_pending(kind, payload.get("ids") or [])
            self.log(f"recovery: adopted {kind} batch {found[0]} for job {j['id']}")
        else:
            db.set_job(self.con, j["id"], state="abandoned")
            if len(found) > 1:
                db.kv_set(self.con, f"ambiguous_batch_{j['id']}", found)
                self.log(f"recovery: {len(found)} batches match job {j['id']}; abandoned")

    # ----------------------------------------------------- error handling
    def _api_error(self, kind, a, msg):
        """R1: API/transport errors revert without a content attempt, but are capped."""
        con, aid = self.con, a["id"]
        self.log(f"API error ({kind}), not counted as a failed check: {aid}: {short(msg, 200)}")
        if kind == "check":
            n = (a["check_api_errors"] or 0) + 1
            if n >= CHECK_API_CAP:
                db.update_article(con, aid, status="skipped", check_api_errors=n,
                                  skip_reason=f"image check could not run twice: {msg}"[:300])
            else:
                db.update_article(con, aid, status="image_done", check_api_errors=n,
                                  note=f"last API error: {msg}"[:300])
            return
        n = (a["api_errors"] or 0) + 1
        if kind == "alcohol":
            if n >= ALCOHOL_API_FALLBACK:
                db.update_article(con, aid, alcohol=1, status="tagged", api_errors=0,
                                  alcohol_reason=f"check failed ({msg}); treated as alcohol"[:300])
            else:
                db.update_article(con, aid, status="url_ok", api_errors=n)
            return
        if n >= API_ERROR_CAP:
            db.update_article(con, aid, status="skipped", api_errors=n,
                              skip_reason=f"API errors {n} times: {msg}"[:300])
        else:
            db.update_article(con, aid, status="themed", api_errors=n,
                              copy_attempts=max(0, (a["copy_attempts"] or 0) - 1),
                              note=f"last API error: {msg}"[:300])

    def _cut_off(self, kind, a):
        """max_tokens: one retry with doubled max_tokens (flag kept per article)."""
        bit = BIG[kind]
        if (a["big_tokens"] or 0) & bit:
            return False
        self.log(f"answer cut off ({kind}): {a['id']}; retrying once with a larger limit")
        fields = {"big_tokens": (a["big_tokens"] or 0) | bit, "status": PENDING_OF[kind][1]}
        if kind == "copy":
            fields["copy_attempts"] = max(0, (a["copy_attempts"] or 0) - 1)
        db.update_article(self.con, a["id"], **fields)
        return True

    # ------------------------------------------------------------------ load
    def step_load(self):
        if db.kv_get(self.con, "waves_assigned"):
            return   # article list is fixed once waves are planned
        rows = self.sheet.read("Input")[1:]
        con = self.con
        seen_titles = {r["title"].lower() for r in con.execute(
            "SELECT title FROM articles WHERE status!='skipped' OR skip_reason!='duplicate title'")}
        have = {r["id"] for r in con.execute("SELECT id FROM articles")}
        with db.tx(con):
            for n, r in enumerate(rows, start=2):
                if len(r) < 3 or not str(r[0]).strip():
                    continue
                url, title, cat = str(r[0]).strip(), str(r[1]).strip(), str(r[2]).strip()
                aid = article_id(url)
                if aid in have:
                    continue
                have.add(aid)
                if title.lower() in seen_titles:
                    con.execute("INSERT INTO articles(id,row_num,url,title,category,status,"
                                "skip_reason) VALUES(?,?,?,?,?,'skipped','duplicate title')",
                                (aid, n, url, title, cat))
                    continue
                seen_titles.add(title.lower())
                con.execute("INSERT INTO articles(id,row_num,url,title,category) VALUES(?,?,?,?,?)",
                            (aid, n, url, title, cat))
            # No URL fetching (Siddharth, 23 Oct): only a text check that the URL is on ponly.com.
            for q in con.execute("SELECT id,url FROM articles WHERE status='queued'").fetchall():
                if q["url"].startswith(config.DOMAIN_PREFIX):
                    db.update_article(con, q["id"], status="url_ok", final_url=q["url"])
                else:
                    db.update_article(con, q["id"], status="skipped",
                                      skip_reason="URL is not on ponly.com")

    # --------------------------------------------------------------- alcohol
    def _apply_alcohol_results(self, res):
        con = self.con
        for cid, (kind, data) in res.items():
            a = con.execute("SELECT * FROM articles WHERE id=?", (cid_article(cid),)).fetchone()
            if not a or a["status"] != "alcohol_pending":
                continue
            if kind == "ok":
                db.update_article(con, a["id"], alcohol=int(bool(data["alcohol"])), status="tagged",
                                  alcohol_reason=f"Haiku: {data['reason']}"[:300], api_errors=0)
            elif kind == "refusal":
                db.update_article(con, a["id"], alcohol=1, status="tagged", api_errors=0,
                                  alcohol_reason="check failed (Haiku refused); treated as alcohol")
            elif kind == "max_tokens":
                if not self._cut_off("alcohol", a):
                    db.update_article(con, a["id"], alcohol=1, status="tagged", api_errors=0,
                                      alcohol_reason="check failed (answer cut off twice); "
                                                     "treated as alcohol")
            else:
                self._api_error("alcohol", a, data)

    def step_alcohol(self):
        con = self.con
        for j in db.open_jobs(con, "alcohol"):
            if not self._finish_job("alcohol", j, self._apply_alcohol_results):
                return
        if con.execute("SELECT 1 FROM articles WHERE status='queued'").fetchone():
            return
        todo = con.execute("SELECT * FROM articles WHERE status='url_ok'").fetchall()
        if not todo:
            return
        reqs = []
        with db.tx(con):
            for a in todo:
                hit, why = alcohol.keyword_check(a["title"])
                if hit is not None:
                    db.update_article(con, a["id"], alcohol=int(hit), alcohol_reason=why,
                                      status="tagged")
                else:
                    reqs.append((f"alc-{a['id']}", llm.alcohol_params(
                        a["title"], big=bool((a["big_tokens"] or 0) & BIG["alcohol"]))))
        if reqs:
            self._submit("alcohol", None, reqs)

    # ---------------------------------------------------------------- themes
    def theme_pools(self):
        con = self.con
        if con.execute("SELECT 1 FROM articles WHERE status IN ('queued','url_ok',"
                       "'alcohol_pending')").fetchone():
            return {}
        pools = {}
        for r in con.execute("SELECT id,title,category,alcohol FROM articles WHERE status='tagged'"
                             " ORDER BY row_num").fetchall():
            pools.setdefault(split.pool_of(r), []).append(r)
        return pools

    def _theme_state(self):
        lists = db.kv_get(self.con, "theme_lists", {})
        for k, v in list(lists.items()):
            if isinstance(v, list):                       # old format: {pool: [themes]}
                lists[k] = {"themes": v, "assigned": {}}
        return lists

    def step_theme_pool(self, pool, items):
        con = self.con
        lists = self._theme_state()
        sig = hashlib.sha1("|".join(r["id"] for r in items).encode()).hexdigest()
        st = lists.setdefault(pool, {"themes": [], "assigned": {}})
        if st.get("sig") != sig:              # article list changed: keep themes, redo assignment
            st["assigned"], st["sig"] = {}, sig

        def save():
            db.kv_set(con, "theme_lists", lists)
        save()
        self.log(f"themes: {pool} ({len(items)} articles)")
        titles = [r["title"] for r in items]
        try:
            themes, assigned, defaulted = llm.build_themes(self.claude, pool, titles, st, save)
        except llm.LLMError as e:
            save()
            n = db.kv_get(con, f"theme_fail_{pool}", 0) + 1
            db.kv_set(con, f"theme_fail_{pool}", n)
            if n < 3:
                raise
            self.log(f"themes: {pool} failed 3 times ({e}); defaulting the rest")
            themes = st.get("themes") or ["Recipes"]
            assigned = [st["assigned"].get(str(i), themes[0]) for i in range(len(items))]
            defaulted = [i for i in range(len(items)) if str(i) not in st["assigned"]]
        dset = set(defaulted)
        with db.tx(con):
            for i, (r, t) in enumerate(zip(items, assigned)):
                fields = {"theme": t, "status": "themed"}
                if i in dset:
                    fields["note"] = "theme defaulted (Haiku did not assign one)"
                db.update_article(con, r["id"], **fields)
            con.execute("INSERT OR REPLACE INTO themes VALUES(?,?)", (pool, json.dumps(themes)))
        self.log(f"themes: {pool} done: {len(items)} articles in {len(themes)} themes, "
                 f"{len(dset)} defaulted")

    # ----------------------------------------------------------------- waves
    def step_assign_waves(self):
        con = self.con
        if db.kv_get(con, "waves_assigned"):
            return
        if con.execute("SELECT 1 FROM articles WHERE status IN ('queued','url_ok',"
                       "'alcohol_pending','tagged')").fetchone():
            return
        arts = [dict(r) for r in con.execute(
            "SELECT id,category,alcohol,theme FROM articles WHERE status='themed'")]
        if not arts:
            return
        sizes = {}
        for a in arts:
            if not a["alcohol"]:
                sizes[a["category"]] = sizes.get(a["category"], 0) + 1
        chosen = [c.strip() for c in str(self.settings.get("Pilot categories", "")).split(";")
                  if c.strip() in sizes]
        if len(chosen) < config.PILOT_FOOD_CATEGORIES:
            chosen = sorted(sizes, key=lambda c: -sizes[c])[:config.PILOT_FOOD_CATEGORIES]
        sz = self.sizes()
        if not sz:
            return
        plan = split.assign_waves(arts, set(chosen), sz[0])
        with db.tx(con):
            for aid, (wave, side) in plan.items():
                model = {"A": "nano-banana", "B": "minimax"}.get(side)
                db.update_article(con, aid, wave=wave, pilot_side=side, image_model=model)
            db.kv_set(con, "pilot_categories", chosen)
            db.kv_set(con, "waves_assigned", True)
        self.log(f"waves assigned; pilot categories: {chosen}")

    # ------------------------------------------------------------ wave gates
    def wave_stats(self, w):
        con = self.con
        rows = con.execute("SELECT status, approval, policy_note, check_failed_once FROM articles "
                           "WHERE wave=?", (w,)).fetchall()
        up = [r for r in rows if r["status"] == "uploaded"]
        approved = sum(1 for r in up if r["approval"] in ("APPROVED", "APPROVED_LIMITED"))
        checked = [r for r in rows if r["status"] in ("planned", "uploaded") or
                   (r["status"] == "skipped" and r["check_failed_once"])]
        img_fail = sum(1 for r in checked if r["check_failed_once"])
        bad_policy = sum(1 for r in up if r["policy_note"] and any(
            k in r["policy_note"].upper() for k in ("CLICKBAIT", "MISREPRESENT", "MISLEADING")))
        return {"uploaded": len(up), "approved": approved,
                "approved_share": approved / len(up) if up else 0.0,
                "image_fail_share": img_fail / len(checked) if checked else 0.0,
                "bad_policy": bad_policy}

    def wave_passed(self, w):
        """P15: latched once true."""
        if db.kv_get(self.con, f"passed_{w}"):
            return True
        if not db.kv_get(self.con, f"enabled_{w}"):
            return False
        st = self.wave_stats(w)
        ok = (st["uploaded"] > 0 and st["approved_share"] >= config.PASS_APPROVED_MIN
              and st["bad_policy"] == 0 and st["image_fail_share"] <= config.PASS_IMAGE_FAIL_MAX)
        if ok:
            db.kv_set(self.con, f"passed_{w}", True)
        return ok

    def content_allowed(self, w):
        if not db.kv_get(self.con, "waves_assigned"):
            return False
        if w == 1:
            return bool(db.kv_get(self.con, "preflight_passed"))
        if w == 2:
            return self.wave_passed(1) and self.chosen_model() is not None
        return self.wave_passed(2) and self.chosen_model() is not None

    def wave_status(self, w):
        """P15: why is this wave waiting?"""
        con = self.con
        if db.kv_get(con, "waves_assigned") and not con.execute(
                "SELECT 1 FROM articles WHERE wave=?", (w,)).fetchone():
            return "no articles in this wave"
        if self.wave_passed(w):
            return "passed"
        if db.kv_get(con, f"enabled_{w}"):
            st = self.wave_stats(w)
            return (f"enabled; waiting for approvals / pass rules: approved "
                    f"{st['approved_share']:.0%} (need {config.PASS_APPROVED_MIN:.0%}), "
                    f"clickbait/misrepresentation flags {st['bad_policy']} (need 0), image "
                    f"check failures {st['image_fail_share']:.0%} (max "
                    f"{config.PASS_IMAGE_FAIL_MAX:.0%})")
        if db.kv_get(con, f"uploaded_{w}"):
            return f"uploaded (paused); tick 'W{w} enable' in Settings to enable"
        if not db.kv_get(con, "waves_assigned"):
            return "waiting: articles not yet tagged/themed/assigned to waves"
        if w == 1 and not db.kv_get(con, "preflight_passed"):
            return "waiting: preflight not passed"
        if w > 1 and not self.wave_passed(w - 1):
            return f"waiting: Wave {w - 1} has not passed yet"
        if w > 1 and self.chosen_model() is None:
            return "waiting: 'Image model' not set in Settings"
        return "in progress"

    # ------------------------------------------------------------------ copy
    def _apply_copy_results(self, res):
        con = self.con
        for cid, (kind, data) in res.items():
            a = con.execute("SELECT * FROM articles WHERE id=?", (cid_article(cid),)).fetchone()
            if not a or a["status"] != "copy_pending":
                continue
            if kind == "ok":
                problems = lint.check_copy(data, a["title"])
            elif kind == "refusal":
                problems = ["model refused to write this copy"]
            elif kind == "max_tokens":
                if self._cut_off("copy", a):
                    continue
                problems = ["answer cut off at max_tokens twice"]
            else:
                self._api_error("copy", a, data)   # never goes into the fix prompt (P3)
                continue
            if not problems:
                db.update_article(con, a["id"], copy_json=json.dumps(data), status="copy_done",
                                  error=None)
            elif a["copy_attempts"] >= 2:
                db.update_article(con, a["id"], status="skipped",
                                  skip_reason="copy failed checks twice: " + "; ".join(problems)[:300])
            else:
                db.update_article(con, a["id"], status="themed", error=json.dumps(problems))
                self.log(f"copy failed checks (will retry): {a['id']} '{short(a['title'], 60)}': "
                         f"{short('; '.join(problems), 300)}")

    def step_copy(self, w):
        con = self.con
        for j in db.open_jobs(con, "copy"):
            if j["wave"] == w and not self._finish_job("copy", j, self._apply_copy_results):
                return
        todo = con.execute("SELECT * FROM articles WHERE wave=? AND status='themed'", (w,)).fetchall()
        if not todo:
            return
        reqs = []
        for a in todo:
            problems = json.loads(a["error"]) if a["error"] and a["error"].startswith("[") else None
            reqs.append((f"copy-{a['id']}-{a['copy_attempts'] + 1}",
                         llm.copy_params(a["title"], a["category"], a["theme"], problems,
                                         big=bool((a["big_tokens"] or 0) & BIG["copy"]))))
        if w == 1 and not db.kv_get(con, "pilot_use_batch"):   # pilot: regular API
            self.log(f"wave 1: copy for {len(reqs)} articles (regular API)")
            for part in _chunks(reqs, COPY_CHUNK):
                if self.stop.is_set():
                    return
                self._run_sync("copy", w, part, self._apply_copy_results)
            return
        self._submit("copy", w, reqs)

    # ---------------------------------------------------------------- images
    def poll_images(self):
        """R3: own step, every tick, also while paused, never backed off."""
        images.poll_predictions(self.con, self.rep)

    def step_images(self, w):
        con = self.con
        inflight = con.execute("SELECT COUNT(*) n FROM predictions WHERE state NOT IN "
                               "('done','failed')").fetchone()["n"]
        room = MAX_INFLIGHT_IMAGES - inflight
        if room <= 0:
            return
        todo = con.execute("SELECT * FROM articles WHERE wave=? AND status='copy_done' "
                           "ORDER BY updated LIMIT ?", (w, room)).fetchall()
        counts = {"reused": 0, "started": 0}
        try:
            self._image_loop(w, todo, counts)
        finally:
            if counts["reused"] or counts["started"]:
                left = con.execute("SELECT COUNT(*) FROM articles WHERE wave=? AND "
                                   "status='copy_done'", (w,)).fetchone()[0]
                busy = con.execute("SELECT COUNT(*) FROM predictions WHERE state NOT IN "
                                   "('done','failed')").fetchone()[0]
                self.log(f"images (wave {w}): {counts['reused']} reused, {counts['started']} "
                         f"started this loop; {busy} being made; {left} waiting")

    def _image_loop(self, w, todo, counts):
        con = self.con
        began = time.time()
        for a in todo:
            model = a["image_model"] if w == 1 else self.chosen_model()
            if not model:
                return
            if a["image_attempts"] >= 3:
                db.update_article(con, a["id"], status="skipped",
                                  skip_reason="image could not be produced after 3 attempts")
                continue
            if (w == 1 and db.kv_get(con, "reuse_images_1") and not a["check_failed_once"]
                    and a["landscape_path"] and a["square_path"]
                    and os.path.exists(a["landscape_path"]) and os.path.exists(a["square_path"])):
                db.update_article(con, a["id"], status="image_done",
                                  note="image reused from the first pilot run (no new cost)")
                self.log(f"image reused (no new cost): {a['id']} '{short(a['title'], 60)}'")
                counts["reused"] += 1
                continue
            # 20-30 s between requests (Siddharth); the rest waits for the next loop
            wait = (db.kv_get(con, "last_image_request", 0)
                    + random.uniform(*config.IMAGE_GAP_S) - time.time())
            if wait > 0:
                if time.time() + wait - began > config.IMAGE_STEP_BUDGET_S:
                    return
                if self.stop.wait(wait):
                    return
                self.heartbeat()
            free = shutil.disk_usage(config.DATA_DIR).free
            if free < config.MIN_FREE_DISK_BYTES:      # never pay for an image we can't save
                raise RuntimeError(f"disk nearly full ({free // 2**20} MB free on "
                                   f"{config.DATA_DIR}); no new images started")
            extra = (" Absolutely no writing or symbols of any kind on any surface."
                     if a["check_failed_once"] else "")
            regen = a["image_attempts"] >= 1
            try:
                images.start_prediction(con, self.rep, a, model, extra)
            except images.StartRejected as e:      # this article only; the rest carry on
                counts["refused"] = counts.get("refused", 0) + 1
                if counts["refused"] >= 3 and not counts["started"]:
                    raise RuntimeError(f"3 image requests in a row refused ({e}); stopping "
                                       f"the step instead of using up articles' attempts")
                n = (a["image_attempts"] or 0) + 1
                if n >= 3:
                    db.update_article(con, a["id"], status="skipped", image_attempts=n,
                                      skip_reason=f"image request refused 3 times: {e}"[:300])
                else:
                    db.update_article(con, a["id"], image_attempts=n,
                                      error=f"image request refused, will retry: {e}"[:300])
                    self.log(f"image request refused (attempt {n}/3): {a['id']}: {short(e, 200)}")
                continue
            finally:
                db.kv_set(con, "last_image_request", time.time())
            counts["started"] += 1
            if regen:   # regeneration: record its cost (only once it was really started)
                db.update_article(con, a["id"], regen_cost=(a["regen_cost"] or 0) +
                                  config.IMAGE_MODELS[model]["est_usd"])

    # ----------------------------------------------------------- image check
    def _apply_check_results(self, res):
        con = self.con
        for cid, (kind, data) in res.items():
            a = con.execute("SELECT * FROM articles WHERE id=?", (cid_article(cid),)).fetchone()
            if not a or a["status"] != "check_pending":
                continue
            if kind == "ok":
                verdict, problems = data["pass"], data["problems"]
            elif kind == "refusal":
                verdict, problems = False, ["image checker refused"]
            elif kind == "max_tokens":
                if self._cut_off("check", a):
                    continue
                verdict, problems = False, ["image check answer cut off twice"]
            else:
                self._api_error("check", a, data)
                continue
            if verdict:
                db.update_article(con, a["id"], status="checked", error=None)
            elif not a["check_failed_once"]:
                self.log(f"image check FAILED (a new image will be made): {a['id']} "
                         f"'{short(a['title'], 60)}': {short('; '.join(problems), 300)}")
                self._delete_images(a)
                if (a["note"] or "").startswith("image reused"):
                    db.update_article(con, a["id"], note=None)   # a new image will be made
                db.update_article(con, a["id"], status="copy_done", check_failed_once=1,
                                  error="image check: " + "; ".join(problems)[:300])
            else:
                self._delete_images(a)
                db.update_article(con, a["id"], status="skipped", skip_reason=(
                    "image failed check twice: " + "; ".join(problems))[:300])

    def step_check(self, w):
        con = self.con
        for j in db.open_jobs(con, "check"):
            if j["wave"] == w and not self._finish_job("check", j, self._apply_check_results):
                return
        # wait until this wave has no images still being made, then check in one go
        if con.execute("SELECT 1 FROM articles WHERE wave=? AND status IN ('copy_done',"
                       "'image_pending','themed','copy_pending')", (w,)).fetchone():
            return
        todo = [dict(r) for r in con.execute(
            "SELECT id, title, image_attempts, landscape_path, square_path, big_tokens FROM "
            "articles WHERE wave=? AND status='image_done'", (w,))]
        for part in _chunks(todo, CHECK_CHUNK):          # P7: 100 images in memory at most
            if self.stop.is_set():
                return
            reqs = []
            for a in part:
                try:
                    with open(a["landscape_path"], "rb") as f1, open(a["square_path"], "rb") as f2:
                        l_bytes, s_bytes = f1.read(), f2.read()
                except (OSError, TypeError):
                    db.update_article(con, a["id"], status="copy_done",
                                      error="image file missing before check; making it again")
                    continue
                reqs.append((f"chk-{a['id']}-{a['image_attempts']}",
                             llm.check_params(a["title"], l_bytes, s_bytes,
                                              big=bool((a["big_tokens"] or 0) & BIG["check"]))))
            if not reqs:
                continue
            if w == 1 and not db.kv_get(con, "pilot_use_batch"):   # pilot: regular API
                self.log(f"wave 1: image check for {len(reqs)} articles (regular API)")
                self._run_sync("check", w, reqs, self._apply_check_results)
            else:
                self._submit("check", w, reqs)
            del reqs

    def _delete_images(self, a):
        for p in (a["landscape_path"], a["square_path"]):
            if p and os.path.exists(p):
                os.remove(p)

    # ------------------------------------------------------------------ plan
    def step_plan(self, w):
        con = self.con
        if db.kv_get(con, f"planned_{w}"):
            return
        if con.execute(f"SELECT 1 FROM articles WHERE wave=? AND status IN "
                       f"({','.join('?' * len(IN_PROGRESS))})", (w, *IN_PROGRESS)).fetchone():
            return
        arts = [dict(r) for r in con.execute(
            "SELECT id,category,alcohol,theme,image_model FROM articles WHERE wave=? AND "
            "status='checked'", (w,))]
        sz = self.sizes()
        if not sz:
            return   # invalid size settings: wait (reason shown in Summary)
        start_nn = {r["pool"]: r["m"] + 1 for r in con.execute(
            "SELECT pool, MAX(nn) m FROM campaigns GROUP BY pool")}
        self.log(f"wave {w}: planning with campaigns {sz[0]}-{sz[1]}, ad groups {sz[2]}-{sz[3]}")
        plan = split.plan_campaigns(arts, start_nn, sz)
        with db.tx(con):                                   # P13: all or nothing
            db.kv_set(con, f"sizes_used_{w}", sz)
            for c in plan:
                ckey = f"w{w}-{c['pool']}-{c['nn']:03d}"
                con.execute("INSERT INTO campaigns(key,wave,pool,image_model,nn,theme,name,n) "
                            "VALUES(?,?,?,?,?,?,?,?)", (ckey, w, c["pool"], c["image_model"],
                                                        c["nn"], c["theme"], c["name"],
                                                        len(c["articles"])))
                for g in c["adgroups"]:
                    gkey = f"{ckey}-ag{g['nn']:02d}"
                    con.execute("INSERT INTO adgroups(key,campaign_key,nn,theme,name,n,alcohol) "
                                "VALUES(?,?,?,?,?,?,?)", (gkey, ckey, g["nn"], g["theme"],
                                                          g["name"], len(g["articles"]),
                                                          int(c["pool"] == split.ALCOHOL_POOL)))
                    for a in g["articles"]:
                        db.update_article(con, a["id"], adgroup_key=gkey, status="planned")
            db.kv_set(con, f"planned_{w}", True)
        self.log(f"wave {w}: campaigns planned: {len(plan)} campaigns, "
                 f"{sum(len(c['adgroups']) for c in plan)} ad groups, "
                 f"{sum(len(c['articles']) for c in plan)} articles")

    # ---------------------------------------------------------------- upload
    def step_upload(self, w):
        con = self.con
        if not db.kv_get(con, f"planned_{w}") or db.kv_get(con, f"uploaded_{w}"):
            return
        if time.time() < db.kv_get(con, "quota_wait_until", 0):
            return
        left = self.quota_cap() - db.ops_last_24h(con) - 300   # reserve for sync / polls
        logos = self.ads.upload_logos()
        n = self.ads.run_upload(w, logos, max(0, left))
        if n == 0 and not db.open_jobs(con, "gads"):
            if self.ads.has_work(w):
                return  # work left but no quota now; wait for the 24 h window
            if not config.KEEP_IMAGES:
                for a in con.execute("SELECT * FROM articles WHERE wave=? AND status='uploaded'",
                                     (w,)).fetchall():
                    self._delete_images(a)
            db.kv_set(con, f"uploaded_{w}", True)
            self.log(f"wave {w}: upload complete")

    # ---------------------------------------------------------------- enable
    def step_enable(self, w):
        """First tick after 'W enable': enable everything that passes the pre-enable check.
        Afterwards, every 30 min, re-check campaigns still 'created' (not enabled because the
        check found a problem, or the enable failed: up to 3 tries). Campaigns 'held' (18-24
        exclusion could not be added after 5 tries) stay paused until fixed by hand."""
        con = self.con
        if not db.kv_get(con, f"uploaded_{w}") or not self.s_bool(f"W{w} enable"):
            return
        first = not db.kv_get(con, f"enabled_{w}")
        if not first and time.time() - db.kv_get(con, f"enable_try_{w}", 0) < 1800:
            return
        db.kv_set(con, f"enable_try_{w}", time.time())
        n = self.ads.enable_wave(w)
        if first:
            db.kv_set(con, f"enabled_{w}", True)
            db.kv_set(con, f"enabled_date_{w}", self.ads.account_today())
            db.kv_set(con, "sync_now", True)
        left = con.execute("SELECT COUNT(*) FROM campaigns WHERE wave=? AND status IN "
                           "('created','held')", (w,)).fetchone()[0]
        if first or n:
            self.log(f"wave {w}: enabled {n} campaigns; {left} still paused (see Campaign map > "
                     f"Error)")

    # ------------------------------------------------------------------ sync
    def step_sync(self):
        con = self.con
        if not con.execute("SELECT 1 FROM campaigns WHERE campaign_res IS NOT NULL AND "
                           "status!='removed'").fetchone():
            return
        if time.time() < db.kv_get(con, "quota_wait_until", 0):
            return
        last = db.kv_get(con, "last_sync", 0)
        # every 6 h while a wave is live but has not passed yet, else daily
        waiting = any(db.kv_get(con, f"enabled_{w}") and not self.wave_passed(w) for w in (1, 2, 3))
        interval = 6 * 3600 if waiting else 86400
        since = db.kv_get(con, "enabled_date_1")
        if time.time() - last < interval and not db.kv_get(con, "sync_now"):
            # spend: clicks and cost every hour while any wave is live (cheap query)
            if since and time.time() - db.kv_get(con, "last_cost_sync", 0) >= 3600:
                self.ads.sync(since, metrics_only=True)
                db.kv_set(con, "last_cost_sync", time.time())
            return
        on = self.ads.sync(since)
        db.kv_set(con, "last_sync", time.time())
        db.kv_set(con, "last_cost_sync", time.time())
        db.kv_set(con, "sync_now", False)
        if db.kv_get(con, "enabled_3"):
            db.kv_set(con, "final_sync_done", True)
        if on:
            self.log(f"WARNING: {on} ads show asset enhancements or auto video ON")

    # --------------------------------------------------------------- reports
    def write_reports(self, force=False):
        """Summary first (the health signal), then Campaign map, then Output. Each tab on its
        own, so one failing write never blocks the others. Output (8,000+ rows) is written
        only when something in it changed, and at most every 15 minutes."""
        errors = []
        for tab, fn in (("Summary", lambda: self.sheet.write("Summary", self.summary_rows(),
                                                              force)),
                        ("Campaign map", lambda: self._write_campaign_map(force)),
                        ("Output", lambda: self._write_output(force))):
            try:
                fn()
            except Exception as e:
                errors.append(f"{tab}: {type(e).__name__}: {short(e, 200)}")
                self.log(f"report error ({tab}): {type(e).__name__}: {short(e, 300)}")
        db.kv_set(self.con, "report_errors", errors)

    def _write_output(self, force=False):
        con = self.con
        camp = {r["key"]: dict(r) for r in con.execute("SELECT * FROM campaigns")}
        ag = {r["key"]: dict(r) for r in con.execute("SELECT * FROM adgroups")}
        out = [list(OUTPUT_HEADER)]
        for a in con.execute("SELECT * FROM articles ORDER BY row_num"):
            g = ag.get(a["adgroup_key"]) or {}
            c = camp.get(g.get("campaign_key")) or {}
            out.append([a["id"], a["category"], a["title"], a["final_url"] or a["url"],
                        "" if a["alcohol"] is None else ("Yes" if a["alcohol"] else "No"),
                        a["alcohol_reason"] or "", a["wave"] or "", a["image_model"] or "",
                        c.get("name", ""), g.get("name", ""), a["status"],
                        a["skip_reason"] or a["error"] or "", a["approval"] or "",
                        a["policy_note"] or "",
                        (c.get("campaign_res") or "").split("/")[-1], a["ad_id"] or "",
                        a["impressions"] or 0, a["clicks"] or 0, round(a["regen_cost"] or 0, 3),
                        a["note"] or "", a["review_status"] or "", a["asset_policy"] or "",
                        a["cost"] or 0])
        digest = hashlib.sha1(json.dumps(out, default=str).encode()).hexdigest()
        last = db.kv_get(con, "output_written", {})
        if not force and (last.get("digest") == digest or
                          time.time() - last.get("t", 0) < config.OUTPUT_MIN_WRITE_INTERVAL_S):
            return
        db.kv_set(con, "output_written", {"digest": last.get("digest"), "t": time.time()})
        self.sheet.write("Output", out, True)
        db.kv_set(con, "output_written", {"digest": digest, "t": time.time()})

    def _write_campaign_map(self, force=False):
        con = self.con
        camp = {r["key"]: dict(r) for r in con.execute("SELECT * FROM campaigns")}
        ag = {r["key"]: dict(r) for r in con.execute("SELECT * FROM adgroups")}
        cm = [["Campaign ID", "Campaign name", "Ad group ID", "Ad group name", "Wave",
               "Image model", "Campaign status", "Error"]]
        for g in ag.values():
            c = camp.get(g["campaign_key"])
            if not c:
                continue
            cm.append([(c["campaign_res"] or "").split("/")[-1], c["name"],
                       (g["adgroup_res"] or "").split("/")[-1], g["name"], c["wave"],
                       c["image_model"] or "", c["status"],
                       c.get("error") or g.get("error") or ""])
        self.sheet.write("Campaign map", cm, force)

    def _disk_free_mb(self):
        try:
            return shutil.disk_usage(config.DATA_DIR).free // 2**20
        except OSError:
            return "unknown"

    def _image_eta(self, w):
        con = self.con
        waiting = con.execute("SELECT COUNT(*) FROM articles WHERE wave=? AND status IN "
                              "('themed','copy_pending','copy_done','image_pending')",
                              (w,)).fetchone()[0]
        if not waiting:
            return "-"
        hour = con.execute("SELECT COUNT(*) FROM predictions WHERE created>?",
                           (time.time() - 3600,)).fetchone()[0]
        if not hour:
            return f"{waiting} articles still need images (none started in the last hour)"
        return f"{waiting} waiting; ~{hour}/hour now -> ~{waiting / hour:.1f} h (estimate)"

    def summary_rows(self):
        con = self.con

        def count(sql, *a):
            return con.execute(sql, a).fetchone()[0]
        counts = {r["status"]: r["n"] for r in con.execute(
            "SELECT status, COUNT(*) n FROM articles GROUP BY status")}
        preds = {r["model"]: r["n"] for r in con.execute(
            "SELECT model, COUNT(*) n FROM predictions WHERE state='done' GROUP BY model")}
        started = con.execute("SELECT COUNT(*) FROM predictions").fetchone()[0]
        extra_imgs = len(db.kv_get(con, "preflight_images", {})) + (
            1 if db.kv_get(con, "pilot_rerun", {}).get("cancel_test") else 0)
        img_usd = sum(config.IMAGE_MODELS.get(m, {}).get("est_usd", 0) * n
                      for m, n in preds.items()) + 0.039 * extra_imgs
        rows = [["Item", "Value"],
                ["Updated", time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime())],
                ["Mode", "DRY RUN (nothing real)" if config.DRY_RUN else "live"],
                ["Paused", "yes (Settings unreadable)" if self.settings is None else
                 ("yes" if self.paused() else "no")],
                ["Preflight", json.dumps(db.kv_get(con, "preflight", {}))[:45000]],
                ["Preflight passed", bool(db.kv_get(con, "preflight_passed"))],
                ["Connections (checked at startup)", json.dumps(db.kv_get(con, "connections", {}))],
                ["Live check (at startup)", json.dumps(db.kv_get(con, "live_check", {}))[:45000]],
                ["Pilot categories", "; ".join(db.kv_get(con, "pilot_categories", []))],
                ["Settings problems", "; ".join(db.kv_get(con, "settings_problems", [])) or "none"],
                ["Size settings problem", db.kv_get(con, "sizes_error") or "none"],
                ["Sizes used per wave (campaign min-max, ad group min-max)",
                 json.dumps({w: db.kv_get(con, f"sizes_used_{w}") for w in (1, 2, 3)})]]
        errs = db.kv_get(con, "step_errors", {})
        live = {k: v for k, v in errs.items() if v.get("count")}
        rows.append(["Last errors (step: error, failures in a row, time)",
                     "; ".join(f"{k}: {v['error']} (x{v['count']}, {v['time']}"
                               + (", paused until " + time.strftime(
                                   "%H:%M UTC", time.gmtime(v["until"])) if v.get("until", 0)
                                  > time.time() else "") + ")"
                               for k, v in sorted(live.items()))[:45000] or "none"])
        for k in sorted(counts):
            rows.append([f"Articles: {k}", counts[k]])
        failed = con.execute("SELECT name, error FROM campaigns WHERE status='failed'").fetchall()
        lost = [r["k"][9:] + ": " + json.loads(r["v"]) for r in con.execute(
            "SELECT k, v FROM kv WHERE k LIKE 'lost_job_%'")]
        rows += [["Campaigns created (not removed)", count("SELECT COUNT(*) FROM campaigns WHERE "
                                             "campaign_res IS NOT NULL AND status!='removed'")],
                 ["Pilot re-run", json.dumps(db.kv_get(con, "pilot_rerun", {}))[:45000]],
                 ["Campaigns FAILED (see Campaign map > Error)", len(failed)],
                 ["Failed campaigns", "; ".join(f"{r['name']}: {r['error']}" for r in failed)[:45000]],
                 ["Campaigns with a pending error",
                  count("SELECT COUNT(*) FROM campaigns WHERE error IS NOT NULL AND "
                        "status!='failed'")],
                 ["Label problems (labels not applied)",
                  json.dumps(db.kv_get(con, f"label_problems_{config.CUSTOMER_ID}", {}))],
                 ["Google Ads jobs lost and reconciled", "; ".join(lost) or "none"],
                 ["Alcohol checks that failed (treated as alcohol)",
                  count("SELECT COUNT(*) FROM articles WHERE alcohol_reason LIKE 'check failed%'")],
                 ["Themes defaulted (see Output > Note)",
                  count("SELECT COUNT(*) FROM articles WHERE note LIKE 'theme defaulted%'")],
                 ["Skipped after repeated API errors",
                  count("SELECT COUNT(*) FROM articles WHERE skip_reason LIKE 'API errors%' OR "
                        "skip_reason LIKE 'image check could not run%'")],
                 ["Batch API error results (total, retried)", db.kv_get(con, "batch_errors_total", 0)],
                 ["Images made (finished, by model)", json.dumps(preds)],
                 ["Image requests started (incl. failed/cancelled)", started],
                 ["Replicate spend (estimate incl. preflight + cancel test; confirm in billing)",
                  round(img_usd, 2)],
                 ["Disk free on the volume (MB)", self._disk_free_mb()],
                 ["Campaigns HELD paused (see Campaign map > Error)",
                  count("SELECT COUNT(*) FROM campaigns WHERE status='held' OR (status='created' "
                        "AND error LIKE 'not enabled%')")],
                 ["Ad review status (count)", json.dumps({r[0] or "-": r[1] for r in con.execute(
                     "SELECT review_status, COUNT(*) FROM articles WHERE status='uploaded' "
                     "GROUP BY review_status")})],
                 ["Ads still not reviewed 48 h after their campaign was enabled",
                  count("SELECT COUNT(*) FROM articles a JOIN adgroups g ON a.adgroup_key=g.key "
                        "JOIN campaigns c ON g.campaign_key=c.key WHERE a.status='uploaded' AND "
                        "c.status='enabled' AND c.enabled_at<? AND (a.approval IS NULL OR "
                        "a.approval IN ('UNKNOWN','UNSPECIFIED'))", time.time() - 172800)],
                 ["Ads with a disapproved image/text",
                  count("SELECT COUNT(*) FROM articles WHERE asset_policy LIKE '%DISAPPROVED%'")],
                 ["Ad spend so far (Rs, from the last sync)",
                  json.dumps({f"wave {r[0]}": round(r[1] or 0, 2) for r in con.execute(
                      "SELECT wave, SUM(cost) FROM articles WHERE status='uploaded' GROUP BY wave")})],
                 ["Report write problems", "; ".join(db.kv_get(con, "report_errors", [])) or "none"],
                 ["Google Ads API operations, last 24 h", db.ops_last_24h(con)],
                 ["Google Ads quota wait reason", db.kv_get(con, "quota_wait_reason") or "none"],
                 ["Google Ads quota wait until",
                  time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime(db.kv_get(con, "quota_wait_until")))
                  if db.kv_get(con, "quota_wait_until", 0) > time.time() else "none"],
                 ["Access level (Settings)", (self.settings or {}).get("Access level", "")],
                 ["Last sync", time.strftime("%Y-%m-%d %H:%M UTC",
                                             time.gmtime(db.kv_get(con, "last_sync", 0)))
                  if db.kv_get(con, "last_sync") else "never"],
                 ["Ads with enhancements/auto video ON", db.kv_get(con, "enhancements_on_count", 0)]]
        for w in (1, 2, 3):
            st = self.wave_stats(w)
            rows.append([f"Wave {w} status", self.wave_status(w)])
            rows.append([f"Wave {w} images ETA", self._image_eta(w)])
            rows.append([f"Wave {w}", json.dumps({**st,
                         "planned": bool(db.kv_get(con, f"planned_{w}")),
                         "uploaded_all": bool(db.kv_get(con, f"uploaded_{w}")),
                         "enabled": bool(db.kv_get(con, f"enabled_{w}")),
                         "passed": self.wave_passed(w)})])
        for m in config.IMAGE_MODELS:
            r = con.execute("SELECT COUNT(*) n, COALESCE(SUM(impressions),0) i, "
                            "COALESCE(SUM(clicks),0) c FROM articles WHERE image_model=? AND "
                            "status='uploaded'", (m,)).fetchone()
            ctr = (r["c"] / r["i"] * 100) if r["i"] else 0
            notes = count("SELECT COUNT(*) FROM articles WHERE image_model=? AND "
                          "policy_note IS NOT NULL", m)
            rows.append([f"{m}: ads / impressions / clicks / CTR % / ads with policy notes",
                         f"{r['n']} / {r['i']} / {r['c']} / {ctr:.2f} / {notes}"])
        return rows

    # ------------------------------------------------------------------ loop
    def read_settings(self):
        self.settings_problems = []
        try:
            if hasattr(self.sheet, "ensure_tabs"):
                self.sheet.ensure_tabs()   # recreates any deleted tab (Pause starts ticked)
            new = self.sheet.read_settings()
            old = getattr(self, "_last_settings", None)
            if old is not None:
                for k in sorted(set(old) | set(new)):
                    if str(old.get(k)) != str(new.get(k)):
                        self.log(f"Settings changed: '{k}': {old.get(k)!r} -> {new.get(k)!r}")
            self._last_settings = dict(new)
            self.settings = new
        except Exception as e:
            self.settings = None           # R7: nothing runs without readable Settings
            errs = db.kv_get(self.con, "step_errors", {})
            n = errs.get("settings", {}).get("count", 0) + 1
            errs["settings"] = {"error": f"{type(e).__name__}: {e}"[:400], "count": n, "until": 0,
                                "time": time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime())}
            db.kv_set(self.con, "step_errors", errs)
            self.log(f"Settings could not be read: {e}")

    def tick(self):
        if not self.acquire_lease():
            if not self._lease_warned:
                self.log("another worker holds the database lease; waiting")
                self._lease_warned = True
            return
        self._lease_warned = False
        try:
            self.run_step("recover", self.recover)
            self.run_step("poll_images", self.poll_images, backoff=False)
            self.read_settings()
            if self.paused():
                return
            if not db.kv_get(self.con, "pilot_rerun_done"):
                from . import rerun
                self.run_step("pilot_rerun", rerun.run, self)
                if not db.kv_get(self.con, "pilot_rerun_done"):
                    return                       # never build on a half-finished reset
            if not db.kv_get(self.con, "preflight_passed"):
                from . import preflight
                self.run_step("preflight", preflight.run, self)
            self.run_step("load", self.step_load)
            self.run_step("alcohol", self.step_alcohol)
            for pool, items in self.theme_pools().items():
                errs = db.kv_get(self.con, "step_errors", {})
                if errs.get(f"themes:{pool}", {}).get("until", 0) > time.time():
                    continue
                self.run_step(f"themes:{pool}", self.step_theme_pool, pool, items)
                break                    # one pool per loop keeps each loop short
            self.run_step("assign_waves", self.step_assign_waves)
            ads_ok = time.time() >= db.kv_get(self.con, "quota_wait_until", 0)
            for w in (1, 2, 3):
                if self.stop.is_set() or not self.content_allowed(w):
                    continue
                for name, fn in (("copy", self.step_copy), ("images", self.step_images),
                                 ("check", self.step_check), ("plan", self.step_plan),
                                 ("upload", self.step_upload), ("enable", self.step_enable)):
                    if name in ("upload", "enable") and not ads_ok:
                        continue
                    self.run_step(f"{name}:w{w}", fn, w)
            if ads_ok:
                self.run_step("labels", lambda: self.ads.add_missing_labels())
        finally:
            # read-only: keeps approvals, clicks and cost current even while paused
            self.run_step("sync", self.step_sync)
            try:
                db.rollback_if_open(self.con)
                db.kv_set(self.con, "settings_problems", sorted(set(self.settings_problems)))
                self.write_reports()          # P2: always, whatever failed
            except Exception:
                self.log("report error:\n" + traceback.format_exc())
            self.heartbeat()


def build():
    from . import fakes
    con = db.connect()
    if config.DRY_RUN:
        return Worker(con, fakes.FakeSheet(), fakes.FakeClaude(), fakes.FakeReplicate(),
                      fakes.fake_ads)
    from .gads import Ads
    from .sheet import Sheet
    sheet = Sheet()
    sheet.ensure_tabs()
    return Worker(con, sheet, llm.Claude(), images.ReplicateClient(), Ads)


def check_connections(w):
    """Harmless read from each service at startup: creates nothing, costs nothing."""
    res = {}

    def run(name, fn):
        try:
            res[name] = "OK: " + fn()
        except Exception as e:
            res[name] = f"FAIL: {type(e).__name__}: {str(e)[:300]}"
        w.log(f"connection {name}: {res[name]}")

    def ads():
        r = w.ads.search("SELECT customer.id, customer.descriptive_name, "
                         "customer.currency_code FROM customer LIMIT 1", "check")
        c = r[0].customer
        return f"account {c.id} '{c.descriptive_name}', currency {c.currency_code}"

    def claude():
        m = w.claude.c.models.retrieve(config.HAIKU_MODEL)
        return f"{m.id} available"

    def rep():
        for k, m in config.IMAGE_MODELS.items():
            w.rep.schema_inputs(m["ref"])
        return "minimax and nano-banana model info read"

    run("Google Ads", ads)
    run("Anthropic", claude)
    run("Replicate", rep)
    return res


REQUIRED_VARS = ["SHEET_ID", "GOOGLE_SERVICE_ACCOUNT_JSON", "GOOGLE_ADS_CUSTOMER_ID",
                 "ANTHROPIC_API_KEY", "REPLICATE_API_TOKEN"]


def missing_vars():
    miss = [v for v in REQUIRED_VARS if not os.environ.get(v)]
    if os.environ.get("GOOGLE_ADS_USE_SERVICE_ACCOUNT") != "1":
        miss += [v for v in ("GOOGLE_ADS_CLIENT_ID", "GOOGLE_ADS_CLIENT_SECRET",
                             "GOOGLE_ADS_REFRESH_TOKEN") if not os.environ.get(v)]
    return miss


def volume_ok():
    """P12/R11: refuse to run on the container's temporary disk."""
    if config.DRY_RUN:
        return True
    d = os.path.realpath(config.DATA_DIR)
    return os.path.ismount(d) or os.path.realpath(
        os.environ.get("RAILWAY_VOLUME_MOUNT_PATH", "/nonexistent")) == d


def main():
    stop = threading.Event()

    def on_signal(sig, frame):
        print(f"signal {sig}: stopping after the current step", flush=True)
        stop.set()
    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    # Wait (instead of crash-looping) until every Railway variable is present.
    while not config.DRY_RUN and missing_vars() and not stop.is_set():
        print("Waiting for Railway variables:", ", ".join(missing_vars()), flush=True)
        stop.wait(300)
    while not volume_ok() and not stop.is_set():
        print(f"DATA_DIR {config.DATA_DIR} is not a mounted volume; refusing to start "
              f"(state would be lost on redeploy). Waiting.", flush=True)
        stop.wait(300)
    print("All Railway variables present. Connecting to the Sheet...", flush=True)
    w, delay = None, 30
    while w is None and not stop.is_set():
        try:
            w = build()
        except Exception:
            print("startup failed, retrying in", delay, "s:\n" + traceback.format_exc(), flush=True)
            stop.wait(delay)
            delay = min(delay * 2, 600)
    if w is None:
        return
    w.stop = stop
    w.log("Worker started. Sheet tabs ready.")
    db.kv_set(w.con, "connections", check_connections(w))
    while not w.acquire_lease() and not stop.is_set():      # live check writes state too
        w.log("another worker holds the database lease; waiting before the live check")
        stop.wait(60)
    if not config.DRY_RUN and not stop.is_set():
        try:
            from . import livecheck
            livecheck.run(w)              # read-only checks against the real accounts
        except Exception:
            w.log("live check error:\n" + traceback.format_exc())
    was_paused = None
    while not stop.is_set():
        try:
            w.tick()
        except Exception:
            w.log("loop error:\n" + traceback.format_exc())
        paused = w.paused()
        if paused != was_paused:
            w.log("PAUSED (untick Pause in Settings to run)" if paused else "RUNNING")
            was_paused = paused
        if db.kv_get(w.con, "enabled_3") and not db.kv_get(w.con, "finished_logged"):
            db.kv_set(w.con, "finished_logged", True)
            w.log("All waves enabled. The worker keeps syncing approvals, clicks and cost.")
        stop.wait(config.LOOP_SLEEP_S)
    w.release_lease()
    w.log("stopped cleanly")


if __name__ == "__main__":
    main()
