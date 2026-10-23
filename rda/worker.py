"""Main loop. Safe to stop and restart at any point: all state is in SQLite."""
import hashlib
import json
import os
import time
import traceback
from concurrent.futures import ThreadPoolExecutor

import requests

from . import alcohol, config, db, images, lint, llm, split
from .sheet import OUTPUT_HEADER

TERMINAL = ("planned", "uploaded", "skipped", "error")
IN_PROGRESS = ("themed", "copy_pending", "copy_done", "image_pending", "image_done",
               "check_pending")
MAX_INFLIGHT_IMAGES = 40


def article_id(url):
    return hashlib.sha1(url.strip().encode()).hexdigest()[:12]


class Worker:
    def __init__(self, con, sheet, claude, rep, ads_factory):
        self.con, self.sheet, self.claude, self.rep = con, sheet, claude, rep
        self._ads, self._ads_factory = None, ads_factory
        self.settings = {}

    @property
    def ads(self):
        if self._ads is None:
            self._ads = self._ads_factory(self.con)
        return self._ads

    def log(self, msg):
        print(time.strftime("%Y-%m-%d %H:%M:%S"), msg, flush=True)

    def s_bool(self, key):
        v = self.settings.get(key, False)
        return v is True or str(v).strip().upper() == "TRUE"

    # ------------------------------------------------------------------ load
    def step_load(self):
        if db.kv_get(self.con, "waves_assigned"):
            known = {r["id"] for r in self.con.execute("SELECT id FROM articles")}
            new = [r for r in self.sheet.read("Input")[1:] if len(r) >= 3 and
                   article_id(str(r[0])) not in known]
            for i, r in enumerate(new):
                self.con.execute(
                    "INSERT OR IGNORE INTO articles(id,row_num,url,title,category,status,skip_reason)"
                    " VALUES(?,?,?,?,?,'skipped','added after waves were planned')",
                    (article_id(str(r[0])), 0, str(r[0]).strip(), str(r[1]).strip(),
                     str(r[2]).strip()))
            return
        rows = self.sheet.read("Input")[1:]
        seen_titles = {r["title"].lower() for r in self.con.execute(
            "SELECT title FROM articles WHERE status!='skipped' OR skip_reason!='duplicate title'")}
        for n, r in enumerate(rows, start=2):
            if len(r) < 3 or not str(r[0]).strip():
                continue
            url, title, cat = str(r[0]).strip(), str(r[1]).strip(), str(r[2]).strip()
            aid = article_id(url)
            if self.con.execute("SELECT 1 FROM articles WHERE id=?", (aid,)).fetchone():
                continue
            if title.lower() in seen_titles:
                self.con.execute("INSERT INTO articles(id,row_num,url,title,category,status,"
                                 "skip_reason) VALUES(?,?,?,?,?,'skipped','duplicate title')",
                                 (aid, n, url, title, cat))
                continue
            seen_titles.add(title.lower())
            self.con.execute("INSERT INTO articles(id,row_num,url,title,category) VALUES(?,?,?,?,?)",
                             (aid, n, url, title, cat))
        queued = self.con.execute("SELECT id,url FROM articles WHERE status='queued'").fetchall()
        if queued:
            self.log(f"URL check: {len(queued)} articles")
            with ThreadPoolExecutor(8) as ex:
                results = list(ex.map(lambda q: (q["id"], self.check_url(q["url"])), queued))
            for aid, (ok, final, why) in results:
                if ok:
                    db.update_article(self.con, aid, status="url_ok", final_url=final)
                else:
                    db.update_article(self.con, aid, status="skipped", skip_reason=why)

    def check_url(self, url):
        if config.DRY_RUN:
            return True, url, None
        try:
            r = requests.get(url, allow_redirects=True, timeout=20, stream=True,
                             headers={"User-Agent": "Mozilla/5.0"})
            r.close()
            if r.status_code != 200:
                return False, None, f"URL returned {r.status_code}"
            if not r.url.startswith(config.DOMAIN_PREFIX):
                return False, None, f"URL ends off ponly.com: {r.url}"
            return True, r.url, None
        except Exception as e:
            return False, None, f"URL check failed: {type(e).__name__}"

    # --------------------------------------------------------------- alcohol
    def step_alcohol(self):
        con = self.con
        for j in db.open_jobs(con, "alcohol"):
            if not self.claude.batch_ended(j["external_id"]):
                return
            res = self.claude.batch_results(j["external_id"])
            for cid, (ok, data) in res.items():
                aid = cid.split("-", 1)[1]
                if ok:
                    db.update_article(con, aid, alcohol=int(bool(data["alcohol"])),
                                      alcohol_reason=f"Haiku: {data['reason']}"[:300],
                                      status="tagged")
                else:  # ambiguous = alcohol
                    db.update_article(con, aid, alcohol=1, status="tagged",
                                      alcohol_reason=f"check failed ({data}); treated as alcohol")
            db.set_job(con, j["id"], state="done")
        todo = con.execute("SELECT id,title FROM articles WHERE status='url_ok'").fetchall()
        if not todo or con.execute("SELECT 1 FROM articles WHERE status='queued'").fetchone():
            return
        reqs = []
        for a in todo:
            hit, why = alcohol.keyword_check(a["title"])
            if hit:
                db.update_article(con, a["id"], alcohol=1, alcohol_reason=why, status="tagged")
            else:
                reqs.append((f"alc-{a['id']}", llm.alcohol_params(a["title"])))
        if reqs:
            jid = db.new_job(con, "alcohol", state="creating")
            bid = self.claude.create_batch(reqs)
            db.set_job(con, jid, external_id=bid, state="running")
            for cid, _ in reqs:
                db.update_article(con, cid.split("-", 1)[1], status="alcohol_pending")
            self.log(f"alcohol check batch {bid}: {len(reqs)} titles")

    # ---------------------------------------------------------------- themes
    def step_themes(self):
        con = self.con
        if con.execute("SELECT 1 FROM articles WHERE status IN ('queued','url_ok',"
                       "'alcohol_pending')").fetchone():
            return
        rows = con.execute("SELECT id,title,category,alcohol FROM articles WHERE status='tagged'"
                           " ORDER BY row_num").fetchall()
        pools = {}
        for r in rows:
            pools.setdefault(split.pool_of(r), []).append(r)
        for pool, items in pools.items():
            if con.execute("SELECT 1 FROM themes WHERE category=?", (pool,)).fetchone():
                continue
            self.log(f"themes: {pool} ({len(items)} articles)")
            themes, assigned = llm.build_themes(self.claude, pool, [r["title"] for r in items])
            for r, t in zip(items, assigned):
                db.update_article(con, r["id"], theme=t, status="themed")
            con.execute("INSERT OR REPLACE INTO themes VALUES(?,?)", (pool, json.dumps(themes)))
            return  # one pool per loop keeps each loop short

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
        plan = split.assign_waves(arts, set(chosen))
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
        if not db.kv_get(self.con, f"enabled_{w}"):
            return False
        st = self.wave_stats(w)
        return (st["uploaded"] > 0 and st["approved_share"] >= config.PASS_APPROVED_MIN
                and st["bad_policy"] == 0 and st["image_fail_share"] <= config.PASS_IMAGE_FAIL_MAX)

    def chosen_model(self):
        m = str(self.settings.get("Image model", "")).strip().lower()
        return m if m in config.IMAGE_MODELS else None

    def content_allowed(self, w):
        if not db.kv_get(self.con, "waves_assigned"):
            return False
        if w == 1:
            return bool(db.kv_get(self.con, "preflight_passed"))
        if w == 2:
            return self.wave_passed(1) and self.chosen_model() is not None
        return self.wave_passed(2) and self.chosen_model() is not None

    # ------------------------------------------------------------------ copy
    def step_copy(self, w):
        con = self.con
        for j in db.open_jobs(con, "copy"):
            if j["wave"] != w:
                continue
            if not self.claude.batch_ended(j["external_id"]):
                return
            res = self.claude.batch_results(j["external_id"])
            for cid, (ok, data) in res.items():
                aid = cid.split("-")[1]
                a = con.execute("SELECT * FROM articles WHERE id=?", (aid,)).fetchone()
                problems = lint.check_copy(data, a["title"]) if ok else [f"API: {data}"]
                if not problems:
                    db.update_article(con, aid, copy_json=json.dumps(data), status="copy_done",
                                      error=None)
                elif a["copy_attempts"] >= 2:
                    db.update_article(con, aid, status="skipped",
                                      skip_reason="copy failed checks twice: " + "; ".join(problems)[:300])
                else:
                    db.update_article(con, aid, status="themed", error=json.dumps(problems))
            db.set_job(con, j["id"], state="done")
        todo = con.execute("SELECT * FROM articles WHERE wave=? AND status='themed'", (w,)).fetchall()
        if not todo:
            return
        reqs = []
        for a in todo:
            problems = json.loads(a["error"]) if a["error"] and a["error"].startswith("[") else None
            reqs.append((f"copy-{a['id']}-{a['copy_attempts'] + 1}",
                         llm.copy_params(a["title"], a["category"], a["theme"], problems)))
        jid = db.new_job(con, "copy", w, state="creating")
        bid = self.claude.create_batch(reqs)
        db.set_job(con, jid, external_id=bid, state="running")
        for a in todo:
            db.update_article(con, a["id"], status="copy_pending",
                              copy_attempts=a["copy_attempts"] + 1)
        self.log(f"wave {w}: copy batch {bid} ({len(reqs)} articles)")

    # ---------------------------------------------------------------- images
    def step_images(self, w):
        con = self.con
        images.poll_predictions(con, self.rep)
        inflight = con.execute("SELECT COUNT(*) n FROM predictions WHERE state NOT IN "
                               "('done','failed')").fetchone()["n"]
        room = MAX_INFLIGHT_IMAGES - inflight
        if room <= 0:
            return
        todo = con.execute("SELECT * FROM articles WHERE wave=? AND status='copy_done' LIMIT ?",
                           (w, room)).fetchall()
        for a in todo:
            model = a["image_model"] if w == 1 else self.chosen_model()
            if not model:
                return
            if a["image_attempts"] >= 3:
                db.update_article(con, a["id"], status="skipped",
                                  skip_reason="image could not be produced after 3 attempts")
                continue
            if a["image_attempts"] >= 1:   # regeneration: record its cost
                db.update_article(con, a["id"], regen_cost=(a["regen_cost"] or 0) +
                                  config.IMAGE_MODELS[model]["est_usd"])
            extra = (" Absolutely no writing or symbols of any kind on any surface."
                     if a["check_failed_once"] else "")
            try:
                images.start_prediction(con, self.rep, a, model, extra)
            except Exception as e:
                self.log(f"replicate create failed for {a['id']}: {e}")
                return

    # ----------------------------------------------------------- image check
    def step_check(self, w):
        con = self.con
        for j in db.open_jobs(con, "check"):
            if j["wave"] != w:
                continue
            if not self.claude.batch_ended(j["external_id"]):
                return
            res = self.claude.batch_results(j["external_id"])
            for cid, (ok, data) in res.items():
                aid = cid.split("-")[1]
                a = con.execute("SELECT * FROM articles WHERE id=?", (aid,)).fetchone()
                if not ok:
                    db.update_article(con, aid, status="image_done", error=f"check API: {data}")
                elif data["pass"]:
                    db.update_article(con, aid, status="checked", error=None)
                elif not a["check_failed_once"]:
                    self._delete_images(a)
                    db.update_article(con, aid, status="copy_done", check_failed_once=1,
                                      error="image check: " + "; ".join(data["problems"])[:300])
                else:
                    self._delete_images(a)
                    db.update_article(con, aid, status="skipped", skip_reason=(
                        "image failed check twice: " + "; ".join(data["problems"]))[:300])
            db.set_job(con, j["id"], state="done")
        # wait until this wave has no images still being made, then check in one batch
        if con.execute("SELECT 1 FROM articles WHERE wave=? AND status IN ('copy_done',"
                       "'image_pending','themed','copy_pending')", (w,)).fetchone():
            return
        todo = con.execute("SELECT * FROM articles WHERE wave=? AND status='image_done'",
                           (w,)).fetchall()
        if not todo:
            return
        reqs = []
        for a in todo:
            with open(a["landscape_path"], "rb") as f1, open(a["square_path"], "rb") as f2:
                reqs.append((f"chk-{a['id']}-{a['image_attempts']}",
                             llm.check_params(a["title"], f1.read(), f2.read())))
        for i in range(0, len(reqs), 250):   # keep each batch request body modest
            part = reqs[i:i + 250]
            jid = db.new_job(con, "check", w, state="creating")
            bid = self.claude.create_batch(part)
            db.set_job(con, jid, external_id=bid, state="running")
            for cid, _ in part:
                db.update_article(con, cid.split("-")[1], status="check_pending")
            self.log(f"wave {w}: image check batch {bid} ({len(part)})")

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
        start_nn = {r["pool"]: r["m"] + 1 for r in con.execute(
            "SELECT pool, MAX(nn) m FROM campaigns GROUP BY pool")}
        for c in split.plan_campaigns(arts, start_nn):
            ckey = f"w{w}-{c['pool']}-{c['nn']:03d}"
            con.execute("INSERT INTO campaigns(key,wave,pool,image_model,nn,theme,name,n) "
                        "VALUES(?,?,?,?,?,?,?,?)", (ckey, w, c["pool"], c["image_model"], c["nn"],
                                                    c["theme"], c["name"], len(c["articles"])))
            for g in c["adgroups"]:
                gkey = f"{ckey}-ag{g['nn']:02d}"
                con.execute("INSERT INTO adgroups(key,campaign_key,nn,theme,name,n,alcohol) "
                            "VALUES(?,?,?,?,?,?,?)", (gkey, ckey, g["nn"], g["theme"], g["name"],
                                                      len(g["articles"]),
                                                      int(c["pool"] == split.ALCOHOL_POOL)))
                for a in g["articles"]:
                    db.update_article(con, a["id"], adgroup_key=gkey, status="planned")
        db.kv_set(con, f"planned_{w}", True)
        self.log(f"wave {w}: campaigns planned")

    # ---------------------------------------------------------------- upload
    def step_upload(self, w):
        con = self.con
        if not db.kv_get(con, f"planned_{w}") or db.kv_get(con, f"uploaded_{w}"):
            return
        cap = int(float(self.settings.get("Daily API quota cap") or config.DEFAULT_QUOTA_CAP))
        left = cap - db.ops_last_24h(con) - 300   # reserve for sync / polls
        logos = self.ads.upload_logos()
        n = self.ads.run_upload(w, logos, max(0, left))
        if n == 0 and not db.open_jobs(con, "gads"):
            if self.ads.ops_for_wave(w, logos):
                return  # work left but no quota now; wait for the 24 h window
            for a in con.execute("SELECT * FROM articles WHERE wave=? AND status='uploaded'", (w,)):
                self._delete_images(a)
            db.kv_set(con, f"uploaded_{w}", True)
            self.log(f"wave {w}: upload complete")

    # ---------------------------------------------------------------- enable
    def step_enable(self, w):
        con = self.con
        if db.kv_get(con, f"enabled_{w}") or not db.kv_get(con, f"uploaded_{w}"):
            return
        if self.s_bool(f"W{w} enable"):
            n = self.ads.enable_wave(w)
            db.kv_set(con, f"enabled_{w}", True)
            db.kv_set(con, f"enabled_date_{w}", time.strftime("%Y-%m-%d"))
            db.kv_set(con, "sync_now", True)
            self.log(f"wave {w}: enabled {n} campaigns")

    # ------------------------------------------------------------------ sync
    def step_sync(self):
        con = self.con
        if not con.execute("SELECT 1 FROM campaigns WHERE campaign_res IS NOT NULL").fetchone():
            return
        last = db.kv_get(con, "last_sync", 0)
        if time.time() - last < 86400 and not db.kv_get(con, "sync_now"):
            return
        since = db.kv_get(con, "enabled_date_1")
        on = self.ads.sync(since)
        db.kv_set(con, "last_sync", time.time())
        db.kv_set(con, "sync_now", False)
        if db.kv_get(con, "enabled_3"):
            db.kv_set(con, "final_sync_done", True)
        if on:
            self.log(f"WARNING: {on} ads show asset enhancements or auto video ON")

    # --------------------------------------------------------------- reports
    def write_reports(self, force=False):
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
                        a["impressions"] or 0, a["clicks"] or 0, round(a["regen_cost"] or 0, 3)])
        self.sheet.write("Output", out, force)
        cm = [["Campaign ID", "Campaign name", "Ad group ID", "Ad group name", "Wave",
               "Image model", "Status"]]
        for g in ag.values():
            c = camp[g["campaign_key"]]
            cm.append([(c["campaign_res"] or "").split("/")[-1], c["name"],
                       (g["adgroup_res"] or "").split("/")[-1], g["name"], c["wave"],
                       c["image_model"] or "", c["status"]])
        self.sheet.write("Campaign map", cm, force)
        self.sheet.write("Summary", self.summary_rows(), force)

    def summary_rows(self):
        con = self.con
        counts = {r["status"]: r["n"] for r in con.execute(
            "SELECT status, COUNT(*) n FROM articles GROUP BY status")}
        preds = {r["model"]: r["n"] for r in con.execute(
            "SELECT model, COUNT(*) n FROM predictions GROUP BY model")}
        img_usd = sum(config.IMAGE_MODELS[m]["est_usd"] * n for m, n in preds.items())
        rows = [["Item", "Value"],
                ["Updated", time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime())],
                ["Mode", "DRY RUN (nothing real)" if config.DRY_RUN else "live"],
                ["Preflight", json.dumps(db.kv_get(con, "preflight", {}))[:45000]],
                ["Preflight passed", bool(db.kv_get(con, "preflight_passed"))],
                ["Connections (checked at startup)", json.dumps(db.kv_get(con, "connections", {}))],
                ["Pilot categories", "; ".join(db.kv_get(con, "pilot_categories", []))]]
        for k in sorted(counts):
            rows.append([f"Articles: {k}", counts[k]])
        rows += [["Campaigns created", con.execute(
                     "SELECT COUNT(*) n FROM campaigns WHERE campaign_res IS NOT NULL").fetchone()["n"]],
                 ["Images made (by model)", json.dumps(preds)],
                 ["Replicate spend (estimate, confirm in billing)", round(img_usd, 2)],
                 ["Google Ads API operations, last 24 h", db.ops_last_24h(con)],
                 ["Access level (Settings)", self.settings.get("Access level", "")],
                 ["Last sync", time.strftime("%Y-%m-%d %H:%M UTC",
                                             time.gmtime(db.kv_get(con, "last_sync", 0)))
                  if db.kv_get(con, "last_sync") else "never"],
                 ["Ads with enhancements/auto video ON", db.kv_get(con, "enhancements_on_count", 0)]]
        for w in (1, 2, 3):
            st = self.wave_stats(w)
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
            notes = con.execute("SELECT COUNT(*) n FROM articles WHERE image_model=? AND "
                                "policy_note IS NOT NULL", (m,)).fetchone()["n"]
            rows.append([f"{m}: ads / impressions / clicks / CTR % / ads with policy notes",
                         f"{r['n']} / {r['i']} / {r['c']} / {ctr:.2f} / {notes}"])
        return rows

    # ------------------------------------------------------------------ loop
    def tick(self):
        if hasattr(self.sheet, "ensure_tabs"):
            self.sheet.ensure_tabs()   # recreates any tab that was deleted (Pause starts ticked)
        self.settings = self.sheet.read_settings()
        if self.s_bool("Pause"):
            self.write_reports()
            return
        if not db.kv_get(self.con, "preflight_passed"):
            from . import preflight
            preflight.run(self)
        self.step_load()
        self.step_alcohol()
        self.step_themes()
        self.step_assign_waves()
        for w in (1, 2, 3):
            if not self.content_allowed(w):
                continue
            self.step_copy(w)
            self.step_images(w)
            self.step_check(w)
            self.step_plan(w)
            self.step_upload(w)
            self.step_enable(w)
        self.step_sync()
        self.write_reports()


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


def main():
    # Wait (instead of crash-looping) until every Railway variable is present.
    while not config.DRY_RUN and missing_vars():
        print("Waiting for Railway variables:", ", ".join(missing_vars()), flush=True)
        time.sleep(300)
    print("All Railway variables present. Connecting to the Sheet...", flush=True)
    w = build()
    w.log("Worker started. Sheet tabs ready.")
    db.kv_set(w.con, "connections", check_connections(w))
    was_paused = None
    while True:
        paused = w.s_bool("Pause") if w.settings else None
        if paused != was_paused and paused is not None:
            w.log("PAUSED (untick Pause in Settings to run)" if paused else "RUNNING")
            was_paused = paused
        try:
            w.tick()
        except Exception:
            w.log("loop error:\n" + traceback.format_exc())
        if db.kv_get(w.con, "enabled_3") and db.kv_get(w.con, "final_sync_done"):
            w.log("All waves enabled and synced. Work finished; stop the Railway service.")
            time.sleep(3600)
            continue
        time.sleep(config.LOOP_SLEEP_S)


if __name__ == "__main__":
    main()
