"""One-off pilot re-run (Siddharth, 24 Oct), runs once when the worker is not paused:

1. remove the 10 first-run pilot campaigns (and their budgets) in Google Ads;
   they stay in the database as 'removed' (wave -1), so the re-run gets new campaign
   numbers and no name is ever reused;
2. one Replicate cancel test (one minimax image started and cancelled at once, ~$0.01);
3. reset the 100 pilot articles to 'themed'. Images and their Google Ads image assets are
   kept and reused; copy and image check now go through the Claude Batch API (the same
   code Wave 2 uses), so that path is tested live.

Only applies to a database whose Wave 1 was uploaded; anywhere else it does nothing."""
import time

from . import config, db

RESET = dict(status="themed", copy_json=None, copy_attempts=0, api_errors=0, big_tokens=0,
             check_api_errors=0, check_failed_once=0, error=None, skip_reason=None,
             adgroup_key=None, ad_resource=None, ad_id=None, ad_attempts=0, approval=None,
             policy_note=None, impressions=None, clicks=None, note=None)


def run(w):
    con = w.con
    if db.kv_get(con, "pilot_rerun_done"):
        return
    if not db.kv_get(con, "uploaded_1") or db.kv_get(con, "enabled_1"):
        db.kv_set(con, "pilot_rerun_done", True)      # nothing to re-run here
        db.kv_set(con, "pilot_rerun", {"skipped": "wave 1 not uploaded, or already enabled"})
        return
    report = db.kv_get(con, "pilot_rerun", {})

    # 1. remove first-run campaigns (network first, then record each success at once)
    camps = [dict(r) for r in con.execute(
        "SELECT * FROM campaigns WHERE wave=1 AND campaign_res IS NOT NULL AND "
        "status!='removed'")]
    errs, berr = w.ads.remove_campaigns(camps)
    with db.tx(con):
        for c in camps:
            if errs.get(c["key"]) is None:
                con.execute("UPDATE campaigns SET status='removed', wave=-1, error=? WHERE key=?",
                            ("removed for the pilot re-run (24 Oct)", c["key"]))
    report["removed"] = report.get("removed", 0) + sum(1 for v in errs.values() if v is None)
    report["budget_remove_errors"] = berr
    bad = {k: v for k, v in errs.items() if v}
    if bad:
        report["remove_errors"] = bad
        db.kv_set(con, "pilot_rerun", report)
        raise RuntimeError(f"pilot re-run: {len(bad)} campaigns could not be removed: "
                           f"{list(bad.values())[:3]} (will retry; nothing reset yet)")
    leftover = con.execute("SELECT COUNT(*) FROM campaigns WHERE wave=1").fetchone()[0]
    if leftover:
        raise RuntimeError(f"pilot re-run: {leftover} wave-1 campaign rows without a Google Ads "
                           f"campaign; not resetting")

    # 2. Replicate cancel test
    if "cancel_test" not in report:
        m = config.IMAGE_MODELS["minimax"]
        try:
            pid = w.rep.create(m["ref"], {"prompt": "A plain white ceramic plate on a wooden "
                                          "table, natural light, no text.", **m["input"]})
            db.kv_set(con, "last_image_request", time.time())
            w.rep.cancel(pid)
            status = None
            for _ in range(10):
                status, _, _ = w.rep.get(pid)
                if status in ("canceled", "succeeded", "failed"):
                    break
                if w.stop.wait(2):
                    break
            report["cancel_test"] = {
                "prediction": pid, "status_after_cancel": status,
                "result": "OK: cancelled" if status == "canceled" else
                          f"NOT CONFIRMED: status is {status}"}
        except Exception as e:
            report["cancel_test"] = {"result": f"FAILED: {type(e).__name__}: {str(e)[:300]}"}
        w.log(f"pilot re-run: Replicate cancel test {report['cancel_test']}")

    # 3. reset the pilot articles (images + image assets kept)
    arts = [r["id"] for r in con.execute("SELECT id FROM articles WHERE wave=1")]
    with db.tx(con):
        for aid in arts:
            db.update_article(con, aid, **RESET)
        for k in ("planned_1", "uploaded_1", "sizes_used_1", "passed_1", "enabled_date_1"):
            con.execute("DELETE FROM kv WHERE k=?", (k,))
        db.kv_set(con, "pilot_use_batch", True)
        db.kv_set(con, "reuse_images_1", True)
        report.update(articles_reset=len(arts), time=time.strftime("%Y-%m-%d %H:%M UTC",
                                                                     time.gmtime()))
        db.kv_set(con, "pilot_rerun", report)
        db.kv_set(con, "pilot_rerun_done", True)
    w.log(f"pilot re-run: removed {report['removed']} campaigns, reset {len(arts)} articles; "
          f"copy and image check now via the Batch API, images reused")
