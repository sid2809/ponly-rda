"""Live check against the real accounts. Runs once at every worker start (after the
connection check). Read-only, with two harmless exceptions that create nothing in the ad
account: a validate-only enable request (Google checks it and changes nothing) and an
empty batch job that is created and removed at once (tests the remove path).

Results go to the Railway log ("LIVE CHECK ...") and the Summary tab ("Live check")."""
import json
import os
import time
import traceback

from . import config, db


def run(w):
    con = w.con
    out = []

    def rec(name, ok, note):
        line = f"{'OK  ' if ok else 'FAIL'} {name}: {note}"
        out.append(line)
        w.log("LIVE CHECK " + line)

    def check(name, fn):
        try:
            ok, note = fn()
        except Exception as e:
            ok, note = False, f"{type(e).__name__}: {str(e)[:500]}"
            w.log(traceback.format_exc())
        rec(name, ok, note)

    ads = w.ads

    # ---------------------------------------------------------------- local state
    def volume():
        d = os.path.realpath(config.DATA_DIR)
        mounted = os.path.ismount(d) or os.path.realpath(
            os.environ.get("RAILWAY_VOLUME_MOUNT_PATH", "/nonexistent")) == d
        return mounted, f"{config.DATA_DIR} mounted={mounted}"

    def integrity():
        r = con.execute("PRAGMA integrity_check").fetchone()[0]
        return r == "ok", f"SQLite integrity: {r}"

    def consistency():
        q = lambda sql: con.execute(sql).fetchone()[0]
        probs = {
            "uploaded without ad": q("SELECT COUNT(*) FROM articles WHERE status='uploaded' "
                                     "AND ad_resource IS NULL"),
            "ad without uploaded status": q("SELECT COUNT(*) FROM articles WHERE ad_resource "
                                            "IS NOT NULL AND status!='uploaded'"),
            "duplicate ad resource": q("SELECT COUNT(*) FROM (SELECT ad_resource FROM articles "
                                       "WHERE ad_resource IS NOT NULL GROUP BY ad_resource "
                                       "HAVING COUNT(*)>1)"),
            "created campaign without resource": q("SELECT COUNT(*) FROM campaigns WHERE "
                                                   "status IN ('created','enabled') AND "
                                                   "campaign_res IS NULL"),
            "uploaded article in ad group without resource": q(
                "SELECT COUNT(*) FROM articles a JOIN adgroups g ON a.adgroup_key=g.key "
                "WHERE a.status='uploaded' AND g.adgroup_res IS NULL"),
            "pending article without open job": sum(
                q(f"SELECT COUNT(*) FROM articles WHERE status='{s}'")
                for s in ("copy_pending", "check_pending", "alcohol_pending")
                if not db.open_jobs(con, {"copy_pending": "copy", "check_pending": "check",
                                          "alcohol_pending": "alcohol"}[s])),
        }
        bad = {k: v for k, v in probs.items() if v}
        jobs = [dict(id=r["id"], kind=r["kind"], state=r["state"]) for r in con.execute(
            "SELECT id, kind, state FROM jobs WHERE state NOT IN ('done','abandoned')")]
        return not bad, f"problems={bad or 'none'}; jobs not done/abandoned={jobs}"

    check("volume", volume)
    check("database", integrity)
    check("state consistency", consistency)

    # ---------------------------------------------------------------- Google Ads
    def batch_results():
        notes, ok = [], True
        for j in con.execute("SELECT * FROM jobs WHERE kind='gads' AND external_id IS NOT NULL "
                             "ORDER BY id").fetchall():
            idx = json.loads(j["payload"] or "{}").get("index", [])
            rows = ads.search(f"SELECT batch_job.status FROM batch_job WHERE "
                              f"batch_job.resource_name = '{j['external_id']}'", "livecheck")
            if not rows:
                notes.append(f"job {j['id']}: not found in Google Ads (state {j['state']})")
                continue
            st = rows[0].batch_job.status.name
            if st != "DONE":
                notes.append(f"job {j['id']}: {st}")
                continue
            svc = ads.client.get_service("BatchJobService")
            n = errs = 0
            first_err = ""
            for r in svc.list_batch_job_results(request={"resource_name": j["external_id"],
                                                         "page_size": 1000}):
                n += 1
                if r.status.code:
                    errs += 1
                    first_err = first_err or ads.decode_status(r.status)[:200]
            db.log_ops(con, 1, "livecheck")
            if n != len(idx):
                ok = False
            notes.append(f"job {j['id']} ({j['state']}): {n} results for {len(idx)} operations, "
                         f"{errs} errors{(' e.g. ' + first_err) if first_err else ''}")
        return ok, "; ".join(notes) or "no Google Ads jobs yet"

    def duplicates():
        """Count every entity of our campaigns in the account; anything >1 is a duplicate."""
        camps = [dict(r) for r in con.execute("SELECT * FROM campaigns WHERE campaign_res IS "
                                              "NOT NULL AND status!='removed'")]
        if not camps:
            return True, "no campaigns yet"
        from .gads import _chunks, _q
        dup, mism, pending = [], [], []
        names = [c["name"] for c in camps]
        cnt = {}
        for part in _chunks(names):
            qn = ",".join(_q(n) for n in part)
            for x in ads.search("SELECT campaign.resource_name, campaign.name FROM campaign "
                                f"WHERE campaign.name IN ({qn}) AND campaign.status != 'REMOVED'",
                                "livecheck"):
                cnt.setdefault(x.campaign.name, []).append(x.campaign.resource_name)
            qb = ",".join(_q(f"{n} | budget") for n in part)
            bcnt = {}
            for x in ads.search("SELECT campaign_budget.name FROM campaign_budget WHERE "
                                f"campaign_budget.name IN ({qb}) AND "
                                "campaign_budget.status != 'REMOVED'", "livecheck"):
                bcnt[x.campaign_budget.name] = bcnt.get(x.campaign_budget.name, 0) + 1
            dup += [f"budget '{k}' x{v}" for k, v in bcnt.items() if v > 1]
        for c in camps:
            found = cnt.get(c["name"], [])
            if len(found) > 1:
                dup.append(f"campaign '{c['name']}' x{len(found)}")
            if c["campaign_res"] not in found:
                mism.append(f"campaign '{c['name']}' stored {c['campaign_res']} found {found}")
        res = [c["campaign_res"] for c in camps]
        crit, labels, ags = {}, {}, {}
        for part in _chunks(res):
            q = ",".join(_q(r) for r in part)
            for x in ads.search("SELECT campaign.resource_name, campaign_criterion.type FROM "
                                f"campaign_criterion WHERE campaign.resource_name IN ({q}) AND "
                                "campaign_criterion.type IN ('LOCATION','LANGUAGE')", "livecheck"):
                k = (x.campaign.resource_name, x.campaign_criterion.type_.name)
                crit[k] = crit.get(k, 0) + 1
            for x in ads.search("SELECT campaign.resource_name, label.name FROM campaign_label "
                                f"WHERE campaign.resource_name IN ({q})", "livecheck"):
                labels.setdefault(x.campaign.resource_name, []).append(x.label.name)
            for x in ads.search("SELECT campaign.resource_name, ad_group.resource_name, "
                                f"ad_group.name FROM ad_group WHERE campaign.resource_name IN ({q}) "
                                "AND ad_group.status != 'REMOVED'", "livecheck"):
                ags.setdefault((x.campaign.resource_name, x.ad_group.name), []).append(
                    x.ad_group.resource_name)
        dup += [f"{t} criterion x{v} on {r}" for (r, t), v in crit.items() if v > 1]
        for c in camps:
            for t in ("LOCATION", "LANGUAGE"):
                if not crit.get((c["campaign_res"], t)):
                    mism.append(f"{c['name']}: no {t}")
            have = labels.get(c["campaign_res"], [])
            dup += [f"label '{n}' x{have.count(n)} on {c['name']}" for n in set(have)
                    if have.count(n) > 1]
            want = set(ads.wanted_labels(c))
            if want - set(have):
                if c["status"] == "held":
                    pass                               # kept paused on purpose; not labelled
                elif (c["label_attempts"] or 0) < 3:   # the worker adds these on its next loop
                    pending.append(f"{c['name']}: {sorted(want - set(have))}")
                else:
                    mism.append(f"{c['name']}: labels missing {sorted(want - set(have))}")
        dup += [f"ad group '{n}' x{len(v)}" for (r, n), v in ags.items() if len(v) > 1]
        stored = {g["adgroup_res"] for g in con.execute("SELECT adgroup_res FROM adgroups WHERE "
                                                        "adgroup_res IS NOT NULL")}
        extra_ags = [n for (r, n), v in ags.items() for x in v if x not in stored]
        if extra_ags:
            mism.append(f"ad groups in Google Ads not in the database: {extra_ags[:10]}")
        ag_res = [x for v in ags.values() for x in v]
        ads_per, excl = {}, {}
        for part in _chunks(ag_res):
            q = ",".join(_q(r) for r in part)
            for x in ads.search("SELECT ad_group.resource_name, ad_group_ad.resource_name, "
                                "ad_group_ad.ad.final_urls FROM ad_group_ad WHERE "
                                f"ad_group.resource_name IN ({q}) AND "
                                "ad_group_ad.status != 'REMOVED'", "livecheck"):
                for u in x.ad_group_ad.ad.final_urls:
                    ads_per.setdefault((x.ad_group.resource_name, u), []).append(
                        x.ad_group_ad.resource_name)
            for x in ads.search("SELECT ad_group.resource_name, ad_group_criterion.age_range.type "
                                f"FROM ad_group_criterion WHERE ad_group.resource_name IN ({q}) "
                                "AND ad_group_criterion.type = 'AGE_RANGE' AND "
                                "ad_group_criterion.negative = TRUE", "livecheck"):
                if x.ad_group_criterion.age_range.type_.name != "AGE_RANGE_18_24":
                    continue
                k = x.ad_group.resource_name
                excl[k] = excl.get(k, 0) + 1
        dup += [f"ad x{len(v)} for {u}" for (g, u), v in ads_per.items() if len(v) > 1]
        dup += [f"18-24 exclusion x{v} on {g}" for g, v in excl.items() if v > 1]
        uploaded = {r["ad_resource"] for r in con.execute(
            "SELECT ad_resource FROM articles WHERE ad_resource IS NOT NULL")}
        all_ads = {x for v in ads_per.values() for x in v}
        if all_ads - uploaded:
            mism.append(f"{len(all_ads - uploaded)} ads in Google Ads not in the database")
        if uploaded - all_ads:
            mism.append(f"{len(uploaded - all_ads)} ads in the database not found in Google Ads")
        for g in con.execute("SELECT g.adgroup_res FROM adgroups g JOIN campaigns c ON "
                             "g.campaign_key=c.key WHERE g.alcohol=1 AND g.adgroup_res IS NOT "
                             "NULL AND c.status!='removed'").fetchall():
            if not excl.get(g["adgroup_res"]):
                mism.append(f"alcohol ad group {g['adgroup_res']} has no 18-24 exclusion")
        from .gads import asset_name
        a_names = [asset_name(r, t) for r in con.execute(
            "SELECT id, image_attempts FROM articles WHERE status='uploaded'") for t in ("L", "S")]
        acnt = {}
        for part in _chunks(a_names):
            q = ",".join(_q(n) for n in part)
            for x in ads.search(f"SELECT asset.name FROM asset WHERE asset.name IN ({q})",
                                "livecheck"):
                acnt[x.asset.name] = acnt.get(x.asset.name, 0) + 1
        dup += [f"image asset '{k}' x{v}" for k, v in acnt.items() if v > 1]
        summary = (f"{len(camps)} campaigns, {len(ags)} ad groups, {len(all_ads)} ads checked; "
                   f"duplicates: {dup[:20] or 'none'}; mismatches: {mism[:20] or 'none'}; "
                   f"labels still to be added by the worker: {len(pending)} campaigns "
                   f"{pending[:3] if pending else ''}")
        return not dup and not mism, summary

    def enable_validate_only():
        rows = con.execute("SELECT campaign_res FROM campaigns WHERE status='created' AND "
                           "campaign_res IS NOT NULL LIMIT 20").fetchall()   # few ops per restart
        if not rows:
            return True, "no paused campaigns to test"
        from google.api_core import protobuf_helpers
        ops = []
        for r in rows:
            op = ads.client.get_type("CampaignOperation")
            op.update.resource_name = r["campaign_res"]
            op.update.status = ads.e.CampaignStatusEnum.ENABLED
            ads.client.copy_from(op.update_mask, protobuf_helpers.field_mask(None, op.update._pb))
            ops.append(op)
        resp = ads.client.get_service("CampaignService").mutate_campaigns(request={
            "customer_id": ads.cid, "operations": ops, "partial_failure": True,
            "validate_only": True})
        db.log_ops(con, len(ops), "livecheck")
        fails = ads.partial_failures(resp)
        return not fails, (f"enable request for {len(ops)} campaigns accepted by Google "
                           f"(validate-only, nothing enabled); failures: {fails or 'none'}")

    def batch_remove():
        svc = ads.client.get_service("BatchJobService")
        op = ads.client.get_type("BatchJobOperation")
        op.create = ads.client.get_type("BatchJob")
        res = svc.mutate_batch_job(customer_id=ads.cid, operation=op).result.resource_name
        rm = ads.client.get_type("BatchJobOperation")
        rm.remove = res
        svc.mutate_batch_job(customer_id=ads.cid, operation=rm)
        db.log_ops(con, 2, "livecheck")
        return True, f"empty batch job created and removed ({res.split('/')[-1]})"

    def labels_tz():
        rows = ads.search("SELECT label.name, label.status FROM label", "livecheck")
        ours = {r.label.name: r.label.status.name for r in rows
                if r.label.name in db.kv_get(con, f"labels_{ads.cid}", {})}
        not_enabled = {k: v for k, v in ours.items() if v != "ENABLED"}
        today = ads.account_today()
        return not not_enabled, (f"{len(ours)} labels of ours, not enabled: "
                                 f"{not_enabled or 'none'}; account date {today} "
                                 f"(time zone {db.kv_get(con, f'tz_{ads.cid}')})")

    check("Google Ads batch results (the page_size fix)", batch_results)
    check("Google Ads duplicates / mismatches", duplicates)
    check("Google Ads enable (validate-only)", enable_validate_only)
    check("Google Ads batch job remove", batch_remove)
    check("Google Ads labels + time zone", labels_tz)

    # ---------------------------------------------------------------- Anthropic
    def anthropic_batch():
        j = con.execute("SELECT * FROM jobs WHERE kind IN ('alcohol','copy','check') AND "
                        "external_id IS NOT NULL ORDER BY id DESC LIMIT 1").fetchone()
        if not j:
            return True, "no Claude batch jobs yet"
        b = w.claude.c.messages.batches.retrieve(j["external_id"])
        rc = b.request_counts
        total = rc.processing + rc.succeeded + rc.errored + rc.canceled + rc.expired
        found = w.claude.find_batch(j["created"] - 10, j["created"] + 120, total)
        res = w.claude.batch_results(j["external_id"])
        kinds = {}
        for k, _ in res.values():
            kinds[k] = kinds.get(k, 0) + 1
        ok = found == [j["external_id"]] and len(res) == total
        return ok, (f"batch {j['external_id']} ({j['kind']}): find_batch -> {found}; results "
                    f"read {len(res)}/{total}, by kind {kinds} (re-reading costs nothing)")

    check("Anthropic batch find + results", anthropic_batch)

    # ---------------------------------------------------------------- Replicate
    def replicate_get():
        rows = con.execute("SELECT id, state FROM predictions ORDER BY created DESC LIMIT 3"
                           ).fetchall()
        if not rows:
            return True, "no predictions yet"
        notes = []
        for r in rows:
            try:
                st, url, err = w.rep.get(r["id"])
                notes.append(f"{r['id']}: {st}")
            except Exception as e:
                notes.append(f"{r['id']}: {type(e).__name__} {str(e)[:100]}")
        has_cancel = callable(getattr(w.rep, "cancel", None))
        return has_cancel, (f"get with the current key: {notes}; cancel available="
                            f"{has_cancel} (not called)")

    check("Replicate prediction read", replicate_get)

    passed = all(l.startswith("OK") for l in out)
    db.kv_set(con, "live_check", {"time": time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime()),
                                  "passed": passed, "results": out})
    w.log(f"LIVE CHECK finished: {'ALL OK' if passed else 'SOME FAILED'}")
    return passed
