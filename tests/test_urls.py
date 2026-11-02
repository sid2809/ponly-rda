"""ponly.com/cook/<slug> -> ponly.com/cooks/<slug> (Siddharth, 2 Nov 2026)."""
import json

from rda import config, db
from tests.test_worker_faults import tmp, make  # noqa: F401


def test_rewrite_only_the_prefix_slug_unchanged():
    assert config.rewrite_url("https://ponly.com/cook/salade-nicoise/") == \
        "https://ponly.com/cooks/salade-nicoise/"
    assert config.rewrite_url("https://ponly.com/cooks/x/") == "https://ponly.com/cooks/x/"
    assert config.rewrite_url("https://ponly.com/other/cook/x") == "https://ponly.com/other/cook/x"
    assert config.rewrite_url(None) is None


def _art(w, aid, status, ad=None, wave=2):
    w.con.execute("INSERT INTO articles(id,url,final_url,title,category,status,wave,ad_resource,"
                  "copy_json) VALUES(?,?,?,?,?,?,?,?,?)",
                  (aid, f"https://ponly.com/cook/{aid}/", f"https://ponly.com/cook/{aid}/", "T",
                   "C", status, wave, ad, json.dumps({"image_prompt": "p"})))


def url(w, aid):
    return w.con.execute("SELECT final_url FROM articles WHERE id=?", (aid,)).fetchone()[0]


def test_not_yet_uploaded_articles_move_to_cooks_uploaded_ones_stay(tmp):
    w = make(every=200)
    _art(w, "a1", "image_pending")
    _art(w, "a2", "checked")
    _art(w, "a3", "planned")
    _art(w, "p1", "uploaded", ad="customers/1/adGroupAds/1~2", wave=1)
    w.step_fix_urls()
    assert url(w, "a1") == "https://ponly.com/cooks/a1/"
    assert url(w, "a2") == "https://ponly.com/cooks/a2/"
    assert url(w, "a3") == "https://ponly.com/cooks/a3/"
    assert url(w, "p1") == "https://ponly.com/cook/p1/"                 # live ad untouched
    w.step_fix_urls()                                                    # idempotent
    assert url(w, "a1") == "https://ponly.com/cooks/a1/"


def test_never_while_an_upload_job_is_open_or_a_lost_job_is_being_matched(tmp):
    w = make(every=200)
    _art(w, "a1", "planned")
    db.new_job(w.con, "gads", 2, payload={"index": []}, state="running")
    w.step_fix_urls()
    assert url(w, "a1") == "https://ponly.com/cook/a1/"
    w.con.execute("UPDATE jobs SET state='done'")
    db.kv_set(w.con, "reconcile_due_2", 1)
    w.step_fix_urls()
    assert url(w, "a1") == "https://ponly.com/cook/a1/"
    w.con.execute("DELETE FROM kv WHERE k='reconcile_due_2'")
    w.step_fix_urls()
    assert url(w, "a1") == "https://ponly.com/cooks/a1/"


def test_the_ad_is_built_with_the_new_url():
    import tempfile
    from tests.test_gads import setup
    with tempfile.TemporaryDirectory() as tmp_:
        con, ads, stub, logos = setup(tmp_)
        con.execute("UPDATE articles SET final_url='https://ponly.com/cooks/'||id||'/'")
        for _ in range(4):
            ads.run_upload(1, logos, 10000)
        assert stub.ent["ad"] and all(u.startswith("https://ponly.com/cooks/")
                                      for _, u, _ in stub.ent["ad"])


def test_new_articles_are_loaded_with_the_new_url(tmp):
    w = make(every=200)
    w.read_settings()
    w.step_load()
    rows = w.con.execute("SELECT url, final_url FROM articles WHERE final_url IS NOT NULL").fetchall()
    assert rows and all(r["url"].startswith("https://ponly.com/cook/") and
                        r["final_url"] == config.rewrite_url(r["url"]) for r in rows)


def test_a_busy_wave_never_blocks_another_wave(tmp):
    w = make(every=200)
    _art(w, "a2", "planned", wave=2)
    _art(w, "a3", "themed", wave=3)
    db.kv_set(w.con, "reconcile_due_1", 1)                 # an old wave 1 marker left over
    db.new_job(w.con, "gads", 3, payload={"index": []}, state="running")
    w.step_fix_urls()
    assert url(w, "a2") == "https://ponly.com/cooks/a2/"   # wave 2 still updated
    assert url(w, "a3") == "https://ponly.com/cook/a3/"    # wave 3 waits for its job


# ---------- uploaded (pilot) ads: final URL updated in place, confirmed by read-back ----------
def _uploaded(tmp_):
    from tests.test_gads import setup
    con, ads, stub, logos = setup(tmp_)
    con.execute("UPDATE articles SET final_url='https://ponly.com/cook/'||id||'/'")
    for _ in range(4):
        ads.run_upload(1, logos, 10000)
    assert all(u.startswith("https://ponly.com/cook/") for _, u, _ in stub.ent["ad"])
    return con, ads, stub, logos


def test_live_ads_get_the_new_url_and_the_stored_url_follows_googles_read_back():
    import tempfile
    with tempfile.TemporaryDirectory() as t:
        con, ads, stub, logos = _uploaded(t)
        assert ads.update_final_urls() == 4
        assert all(u.startswith("https://ponly.com/cooks/") for _, u, _ in stub.ent["ad"])
        assert all(r[0].startswith("https://ponly.com/cooks/") for r in
                   con.execute("SELECT final_url FROM articles WHERE status='uploaded'"))
        n = stub.ad_updates
        assert ads.update_final_urls() == 0 and stub.ad_updates == n      # done once


def test_update_google_ignores_is_never_counted_and_stops_after_3():
    import tempfile
    with tempfile.TemporaryDirectory() as t:
        con, ads, stub, logos = _uploaded(t)
        stub.ignore_ad_updates = True
        for _ in range(5):
            ads.update_final_urls()
        rows = con.execute("SELECT final_url, url_attempts, url_error FROM articles WHERE "
                           "status='uploaded'").fetchall()
        assert all(r["final_url"].startswith("https://ponly.com/cook/") for r in rows)
        assert all(r["url_attempts"] == 3 and "still shows" in r["url_error"] for r in rows)
        assert stub.ad_updates == 3


def test_live_check_lists_pending_urls_and_flags_a_url_that_differs():
    import tempfile
    from rda import livecheck
    from tests.test_gads import _live_worker
    with tempfile.TemporaryDirectory() as t:
        con, ads, stub, logos = _uploaded(t)
        w = _live_worker(con, ads)
        livecheck.run(w)
        line = next(x for x in w.logs if "duplicates / mismatches" in x)
        assert "mismatches: none" in line and ": URL" in line          # pending, not a failure
        ads.update_final_urls()
        g, u, r = stub.ent["ad"][0]
        stub.ent["ad"][0] = (g, "https://ponly.com/cook/changed-back/", r)
        w = _live_worker(con, ads)
        livecheck.run(w)
        line = next(x for x in w.logs if "duplicates / mismatches" in x)
        assert "FAIL" in line and "differs from stored" in line
