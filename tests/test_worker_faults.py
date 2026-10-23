"""Fault injection for the worker (P21): failing paid calls, crashes between steps,
broken predictions, bad batch entries, unreadable settings. Every article must end in a
final state and nothing may be created twice."""
import os
import random
import tempfile
import time

import pytest

from rda import config, db, fakes, images, llm, worker


@pytest.fixture
def tmp():
    with tempfile.TemporaryDirectory() as d:
        old = (config.DRY_RUN, config.DATA_DIR, config.DB_PATH, config.IMG_DIR,
               config.KEEP_IMAGES, config.SHEETS_MIN_WRITE_INTERVAL_S, worker.BACKOFF_S)
        config.DRY_RUN, config.DATA_DIR = True, d
        config.DB_PATH = os.path.join(d, "rda.sqlite")
        config.IMG_DIR = os.path.join(d, "images")
        config.KEEP_IMAGES = True
        config.SHEETS_MIN_WRITE_INTERVAL_S = 0
        worker.BACKOFF_S = (0, 0, 0)          # no waiting in tests
        yield d
        (config.DRY_RUN, config.DATA_DIR, config.DB_PATH, config.IMG_DIR, config.KEEP_IMAGES,
         config.SHEETS_MIN_WRITE_INTERVAL_S, worker.BACKOFF_S) = old


def make(every=25, claude=None, rep=None):
    w = worker.build()
    rows = w.sheet.tabs["Input"]
    w.sheet.tabs["Input"] = rows[:1] + rows[1::every]
    w.sheet.set_setting("Auto-apply recommendations off", True)
    w.sheet.set_setting("Pause", False)
    w.claude = claude or fakes.FakeClaude(fail_rate=0.02)
    if rep:
        w.rep = rep
    return w


class FlakyClaude(fakes.FakeClaude):
    """API errors, cut-off answers, missing results and failing batch creates."""

    def __init__(self, rate=0.1):
        super().__init__(fail_rate=0.02)
        self.frng = random.Random(7)
        self.rate = rate
        self.creates = 0

    def sync_json(self, params):
        r = self.frng.random()
        if r < self.rate:
            raise llm.LLMError("api", "overloaded")
        if r < self.rate * 1.3:
            raise llm.LLMError("max_tokens", "cut off")
        return super().sync_json(params)

    def create_batch(self, requests):
        if self.frng.random() < 0.2:
            raise RuntimeError("500 from create")
        self.creates += 1
        bid = super().create_batch(requests)
        for cid in list(self.batches[bid]):
            r = self.frng.random()
            if r < self.rate:
                self.batches[bid][cid] = ("api", "batch entry errored")
            elif r < self.rate * 1.3:
                self.batches[bid][cid] = ("max_tokens", "cut off")
            elif r < self.rate * 1.4:
                del self.batches[bid][cid]
        return bid


class FlakyReplicate(fakes.FakeReplicate):
    def __init__(self):
        super().__init__()
        self.fate = {}
        self.frng = random.Random(3)

    def create(self, ref, inputs):
        pid = super().create(ref, inputs)
        r = self.frng.random()
        self.fate[pid] = ("failed" if r < 0.05 else "nourl" if r < 0.1 else
                          "corrupt" if r < 0.15 else "geterr" if r < 0.2 else "ok")
        return pid

    def get(self, pid):
        f = self.fate[pid]
        if f == "geterr":
            self.fate[pid] = "ok"              # transient
            raise ConnectionError("reset")
        if f == "failed":
            return "failed", None, "NSFW"
        if f == "nourl":
            return "succeeded", None, None
        return "succeeded", f"fake://{pid}", None

    def download(self, url):
        if self.fate[url.split("//")[1]] == "corrupt":
            return b"not an image"
        return super().download(url)


def drive(w, ticks=400):
    con = w.con
    for _ in range(ticks):
        db.kv_set(con, "preflight_last_try", 0)     # retry failed preflight checks at once
        w.tick()
        for n in (1, 2, 3):
            if db.kv_get(con, f"uploaded_{n}"):
                w.sheet.set_setting(f"W{n} enable", True)
        if db.kv_get(con, "enabled_1"):
            w.sheet.set_setting("Image model", "Mini Max".replace(" ", ""))
        for n in (1, 2, 3):
            if db.kv_get(con, f"enabled_{n}") and not db.kv_get(con, f"synced_{n}"):
                db.kv_set(con, "sync_now", True)
                db.kv_set(con, f"synced_{n}", True)
        if db.kv_get(con, "enabled_3") and db.kv_get(con, "final_sync_done"):
            return True
    return False


def assert_all_final_and_unique(w):
    con = w.con
    states = {r["status"]: r["n"] for r in con.execute(
        "SELECT status, COUNT(*) n FROM articles GROUP BY status")}
    assert set(states) <= {"uploaded", "skipped", "error"}, states
    ads = w.ads.submitted.count("ad_group_ad_operation")
    up = states.get("uploaded", 0)
    assert ads == up, (ads, up)                                    # nothing created twice
    assert w.ads.submitted.count("campaign_operation") == con.execute(
        "SELECT COUNT(*) FROM campaigns WHERE campaign_res IS NOT NULL").fetchone()[0]
    assert len({r[0] for r in con.execute("SELECT ad_resource FROM articles WHERE "
                                          "ad_resource IS NOT NULL")}) == up


def test_clean_run_finishes(tmp):
    w = make()
    assert drive(w)
    assert_all_final_and_unique(w)
    assert w.con.execute("SELECT COUNT(*) FROM articles WHERE status='uploaded'").fetchone()[0] > 200


def test_flaky_everything_still_finishes(tmp):
    w = make(claude=FlakyClaude(), rep=FlakyReplicate())
    assert drive(w, 800)
    assert_all_final_and_unique(w)
    con = w.con
    assert con.execute("SELECT COUNT(*) FROM articles WHERE status='uploaded'").fetchone()[0] > 150
    # API errors never became "fix these problems" prompts
    assert not con.execute("SELECT 1 FROM articles WHERE error LIKE '%API%' AND "
                           "error LIKE '[%'").fetchone()
    assert any(r[0] == "abandoned" for r in con.execute("SELECT state FROM jobs"))
    rows = dict(w.summary_rows())
    assert "Last errors (step: error, failures in a row, time)" in rows


def test_recovery_after_crash_between_steps(tmp):
    w = make(every=200)
    con = w.con
    w.sheet.set_setting("Pause", True)
    w.tick()
    w.step_load()
    ids = [r["id"] for r in con.execute("SELECT id FROM articles WHERE status='url_ok' LIMIT 6")]
    a, b, c, d, e, f = ids
    # (1) died during create_batch: 'creating' row, articles untouched
    db.new_job(con, "alcohol", payload={"ids": [a], "cids": [f"alc-{a}"]}, state="creating")
    # (2) died after statuses changed but the job vanished: orphan pending article
    db.update_article(con, b, status="alcohol_pending")
    # (3) pilot sync call died midway
    db.new_job(con, "copy", 1, payload={"ids": [c], "cids": [f"copy-{c}-1"]}, state="sync")
    db.update_article(con, c, status="copy_pending", copy_attempts=1)
    # (4) a running batch job keeps its articles
    bid = w.claude.create_batch([(f"alc-{d}", llm.alcohol_params("Mojito"))])
    db.new_job(con, "alcohol", payload={"ids": [d], "cids": [f"alc-{d}"]},
               external_id=bid, state="running")
    db.update_article(con, d, status="alcohol_pending")
    w.recover()
    st = {r["id"]: dict(r) for r in con.execute("SELECT * FROM articles")}
    assert st[a]["status"] == "url_ok"
    assert st[b]["status"] == "url_ok"
    assert st[c]["status"] == "themed" and st[c]["copy_attempts"] == 0
    assert st[d]["status"] == "alcohol_pending"
    jobs = {r["id"]: r["state"] for r in con.execute("SELECT * FROM jobs")}
    assert sorted(jobs.values()) == ["abandoned", "abandoned", "running"]


def test_creating_row_adopts_exactly_one_matching_batch(tmp):
    w = make(every=200)
    con = w.con
    w.step_load()
    a = con.execute("SELECT id FROM articles WHERE status='url_ok'").fetchone()["id"]
    reqs = [(f"alc-{a}", llm.alcohol_params("Gin Fizz"))]
    jid = db.new_job(con, "alcohol", payload={"ids": [a], "cids": [f"alc-{a}"]}, state="creating")
    bid = w.claude.create_batch(reqs)                     # created, but the ID was never saved
    w.claude.find_batch = lambda after, before, n: [bid]
    w.recover()
    j = con.execute("SELECT * FROM jobs WHERE id=?", (jid,)).fetchone()
    assert j["state"] == "running" and j["external_id"] == bid
    w.step_alcohol()
    assert con.execute("SELECT status FROM articles WHERE id=?", (a,)).fetchone()[0] == "tagged"
    # two matches -> not adopted
    jid2 = db.new_job(con, "alcohol", payload={"ids": [a], "cids": [f"alc-{a}"]}, state="creating")
    w.claude.find_batch = lambda after, before, n: ["x", "y"]
    w.recover()
    assert con.execute("SELECT state FROM jobs WHERE id=?", (jid2,)).fetchone()[0] == "abandoned"


def test_wrong_batch_results_not_applied(tmp):
    w = make(every=200)
    con = w.con
    w.step_load()
    a = con.execute("SELECT id FROM articles WHERE status='url_ok'").fetchone()["id"]
    other = w.claude.create_batch([("alc-zzzzzzzzzzzz", llm.alcohol_params("Beer"))])
    db.new_job(con, "alcohol", payload={"ids": [a], "cids": [f"alc-{a}"]},
               external_id=other, state="running")
    db.update_article(con, a, status="alcohol_pending")
    with pytest.raises(RuntimeError):
        w.step_alcohol()
    w.recover()
    assert con.execute("SELECT status FROM articles WHERE id=?", (a,)).fetchone()[0] == "url_ok"


def test_api_errors_capped(tmp):
    w = make(every=200)
    con = w.con
    con.execute("INSERT INTO articles(id,url,title,category,status,copy_attempts) "
                "VALUES('aaaaaaaaaaaa','u','T','C','copy_pending',1)")
    for i in range(worker.API_ERROR_CAP):
        con.execute("UPDATE articles SET status='copy_pending', copy_attempts=1 WHERE "
                    "id='aaaaaaaaaaaa'")
        w._apply_copy_results({"copy-aaaaaaaaaaaa-1": ("api", "529 overloaded")})
        a = con.execute("SELECT * FROM articles WHERE id='aaaaaaaaaaaa'").fetchone()
        if i < worker.API_ERROR_CAP - 1:      # API errors never use up content attempts
            assert a["status"] == "themed" and a["copy_attempts"] == 0
            assert not (a["error"] or "").startswith("[")
    assert a["status"] == "skipped" and "API errors 5 times" in a["skip_reason"]


def test_max_tokens_retried_once_with_bigger_limit(tmp):
    w = make(every=200)
    con = w.con
    con.execute("INSERT INTO articles(id,url,title,category,status,copy_attempts) "
                "VALUES('bbbbbbbbbbbb','u','T','C','copy_pending',1)")
    w._apply_copy_results({"copy-bbbbbbbbbbbb-1": ("max_tokens", "cut")})
    a = con.execute("SELECT * FROM articles WHERE id='bbbbbbbbbbbb'").fetchone()
    assert a["status"] == "themed" and a["big_tokens"] & worker.BIG["copy"]
    assert llm.copy_params("T", "C", "X", big=True)["max_tokens"] == 16000
    con.execute("UPDATE articles SET status='copy_pending', copy_attempts=1 WHERE id='bbbbbbbbbbbb'")
    w._apply_copy_results({"copy-bbbbbbbbbbbb-1": ("max_tokens", "cut")})
    a = con.execute("SELECT * FROM articles WHERE id='bbbbbbbbbbbb'").fetchone()
    assert a["status"] == "themed" and "cut off" in a["error"]     # counted failure now


def test_alcohol_fallback_after_three_api_errors(tmp):
    w = make(every=200)
    con = w.con
    con.execute("INSERT INTO articles(id,url,title,category,status) "
                "VALUES('cccccccccccc','u','Punch','C','alcohol_pending')")
    for _ in range(3):
        con.execute("UPDATE articles SET status='alcohol_pending' WHERE id='cccccccccccc'")
        w._apply_alcohol_results({"alc-cccccccccccc": ("api", "500")})
    a = con.execute("SELECT * FROM articles WHERE id='cccccccccccc'").fetchone()
    assert a["status"] == "tagged" and a["alcohol"] == 1
    assert a["alcohol_reason"].startswith("check failed")


def test_predictions_timeout_no_output_corrupt(tmp):
    w = make(every=200)
    con = w.con

    class Rep(fakes.FakeReplicate):
        cancelled = []

        def get(self, pid):
            return {"p1": ("processing", None, None), "p2": ("succeeded", None, None),
                    "p3": ("succeeded", "fake://p3", None)}[pid]

        def download(self, url):
            return b"garbage"

        def cancel(self, pid):
            self.cancelled.append(pid)
    rep = Rep()
    for i, pid in enumerate(("p1", "p2", "p3")):
        aid = f"d{i}dddddddddd"
        con.execute("INSERT INTO articles(id,url,title,category,status,image_attempts) "
                    "VALUES(?,?,?,?,?,?)", (aid, "u", "T", "C", "image_pending", 1))
        con.execute("INSERT INTO predictions VALUES(?,?,?,?,?)",
                    (pid, aid, "minimax", "starting",
                     time.time() - (3600 if pid == "p1" else 10)))
    images.poll_predictions(con, rep)
    st = [r["status"] for r in con.execute("SELECT status FROM articles WHERE id LIKE 'd%' "
                                           "ORDER BY id")]
    assert st == ["copy_done"] * 3
    assert rep.cancelled == ["p1"]
    assert {r[0] for r in con.execute("SELECT state FROM predictions")} == {"failed"}


def test_theme_failures_default_after_three(tmp):
    class Broken(fakes.FakeClaude):
        def sync_json(self, params):
            if "themes" in params["output_config"]["format"]["schema"]["properties"]:
                return super().sync_json(params)
            raise llm.LLMError("api", "overloaded")
    w = make(every=100, claude=Broken())
    for _ in range(100):
        w.tick()
    con = w.con
    assert not con.execute("SELECT 1 FROM articles WHERE status='tagged'").fetchone()
    n = con.execute("SELECT COUNT(*) FROM articles WHERE note LIKE 'theme defaulted%'").fetchone()[0]
    assert n > 0
    assert dict(w.summary_rows())["Themes defaulted (see Output > Note)"] == n


def test_theme_progress_saved_and_old_format_migrated(tmp):
    w = make(every=100)
    con = w.con
    db.kv_set(con, "theme_lists", {"Old Pool": ["A", "B"]})
    assert w._theme_state()["Old Pool"] == {"themes": ["A", "B"], "assigned": {}}
    calls = []

    class Count(fakes.FakeClaude):
        def sync_json(self, params):
            calls.append(1)
            return super().sync_json(params)
    w.claude = Count()
    w.step_load()
    con.execute("UPDATE articles SET status='tagged', alcohol=0 WHERE status='url_ok'")
    pool, items = next(iter(w.theme_pools().items()))
    w.step_theme_pool(pool, items)
    first = len(calls)
    assert first >= 2 and db.kv_get(con, "theme_lists")[pool]["assigned"]
    con.execute("UPDATE articles SET status='tagged' WHERE status='themed'")   # DB write lost
    w.step_theme_pool(pool, items)
    assert len(calls) == first                    # nothing paid twice


def test_unreadable_or_missing_settings_mean_paused(tmp):
    w = make(every=200)

    def boom():
        raise ConnectionError("sheets down")
    w.sheet.read_settings = boom
    w.tick()
    assert w.settings is None and w.paused()
    assert not w.con.execute("SELECT 1 FROM articles").fetchone()      # nothing ran
    assert "Summary" in w.sheet.tabs                                    # reports still written
    w.sheet.read_settings = lambda: {"W1 enable": False}
    w.tick()
    assert w.paused()


def test_settings_parsing(tmp):
    w = make(every=200)
    w.settings = {"Daily API quota cap": "14,000", "Access level": "Explorer",
                  "Image model": "Nano Banana", "Articles per campaign (min)": "six"}
    assert w.quota_cap() == 2700
    w.settings["Access level"] = "Basic"
    assert w.quota_cap() == 14000
    assert w.chosen_model() == "nano-banana"
    assert w.sizes() == (6, 8, 2, 4)
    assert any("six" in p for p in w.settings_problems)


def test_failing_step_backs_off_and_reports(tmp):
    worker.BACKOFF_S = (600, 1800, 3600)
    w = make(every=200)
    calls = []

    def bad():
        calls.append(1)
        raise RuntimeError("boom")
    for _ in range(6):
        w.run_step("x", bad)
    assert len(calls) == 3                         # then waits 10 min
    e = db.kv_get(w.con, "step_errors")["x"]
    assert e["count"] == 3 and e["until"] > time.time() + 500
    w.write_reports()
    assert "boom" in dict(w.sheet.tabs["Summary"])["Last errors (step: error, failures in a row, time)"]


def test_lease_blocks_second_worker(tmp):
    w1 = make(every=200)
    assert w1.acquire_lease()
    w2 = worker.Worker(db.connect(), w1.sheet, w1.claude, w1.rep, fakes.fake_ads)
    assert not w2.acquire_lease()
    w2.tick()                                       # does nothing
    w1.release_lease()
    assert w2.acquire_lease()


def test_sheet_write_updates_before_clearing():
    from rda import sheet as sheet_mod
    calls = []

    class Req:
        def __init__(self, name):
            self.name = name

        def execute(self):
            calls.append(self.name)
            return {}

    class Values:
        def update(self, **k):
            return Req("update")

        def clear(self, **k):
            calls.append(("clear-range", k["range"]))
            return Req("clear")

    s = sheet_mod.Sheet.__new__(sheet_mod.Sheet)
    s.svc = type("S", (), {"spreadsheets": lambda self: type("X", (), {
        "values": lambda self: Values()})()})()
    s.sid, s._last_write = "id", {}
    s.write("Output", [["a"], ["b"]], force=True)
    assert calls == ["update", ("clear-range", "'Output'!A3:ZZ"), "clear"]


def test_sheet_write_ok_when_tab_ends_at_last_row():
    from rda import sheet as sheet_mod

    class Req:
        def __init__(self, fail=False):
            self.fail = fail

        def execute(self):
            if self.fail:
                raise RuntimeError('HttpError 400 "Range (Output!A3:ZZ) exceeds grid limits. '
                                   'Max rows: 2, max columns: 26"')
            return {}

    class Values:
        def update(self, **k):
            return Req()

        def clear(self, **k):
            return Req(fail=True)

    s = sheet_mod.Sheet.__new__(sheet_mod.Sheet)
    s.svc = type("S", (), {"spreadsheets": lambda self: type("X", (), {
        "values": lambda self: Values()})()})()
    s.sid, s._last_write = "id", {}
    assert s.write("Output", [["a"], ["b"]], force=True) is True
