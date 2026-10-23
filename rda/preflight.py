"""Preflight checks (spec section 8). Each check is stored once it passes, so paid checks
(test images, test campaign) are not repeated. Any failure blocks Wave 1."""
import json
import time
import traceback

from PIL import Image

from . import alcohol, config, db, images, lint, llm

TEST_TITLE = "Classic Chicken Pot Pie"


def run(w):
    con = w.con
    res = db.kv_get(con, "preflight", {})
    last = db.kv_get(con, "preflight_last_try", 0)
    if res and time.time() - last < 600:   # retry failed checks every 10 minutes at most
        return
    db.kv_set(con, "preflight_last_try", time.time())
    checks = [("logos", check_logos), ("alcohol_test_set", check_alcohol),
              ("haiku_copy", check_haiku), ("auto_apply_off", check_auto_apply),
              ("access_level", check_access), ("currency_inr", check_currency),
              ("campaign_room", check_room), ("image_models", check_image_models),
              ("test_campaign", check_test_campaign)]
    for name, fn in checks:
        if res.get(name, {}).get("ok"):
            continue
        try:
            ok, note = fn(w)
        except Exception as e:
            ok, note = False, f"{type(e).__name__}: {e}"[:800]
            w.log(traceback.format_exc())
        res[name] = {"ok": ok, "note": note}
        db.kv_set(con, "preflight", res)
        w.log(f"preflight {name}: {'OK' if ok else 'FAIL'} {note}")
    passed = all(res.get(n, {}).get("ok") for n, _ in checks)
    db.kv_set(con, "preflight_passed", passed)


def check_logos(w):
    out = []
    for path, ratio, min_w, min_h in ((config.LOGO_LANDSCAPE, 4.0, 512, 128),
                                      (config.LOGO_SQUARE, 1.0, 128, 128)):
        im = Image.open(path)
        wd, ht = im.size
        size_kb = __import__("os").path.getsize(path) / 1024
        if abs(wd / ht - ratio) / ratio > 0.01 or wd < min_w or ht < min_h or size_kb > 5120:
            return False, f"{path}: {wd}x{ht}, {size_kb:.0f} KB"
        out.append(f"{wd}x{ht}")
    return True, " and ".join(out)


def check_alcohol(w):
    wrong = []
    for t in alcohol.MUST_TAG:
        hit, _ = alcohol.keyword_check(t)
        if not hit:
            r = w.claude.sync_json(llm.alcohol_params(t))
            hit = r["alcohol"]
        if not hit:
            wrong.append(f"missed {t}")
    for t in alcohol.MUST_NOT_TAG:
        hit, _ = alcohol.keyword_check(t)
        if not hit:
            hit = w.claude.sync_json(llm.alcohol_params(t))["alcohol"]
        if hit:
            wrong.append(f"wrongly tagged {t}")
    return (not wrong), "; ".join(wrong) or "all test titles correct"


def check_haiku(w):
    data = w.claude.sync_json(llm.copy_params(TEST_TITLE, "Casseroles & Bakes", "Pot Pies"))
    problems = lint.check_copy(data, TEST_TITLE)
    db.kv_set(w.con, "preflight_test_copy", data)
    return (not problems), "; ".join(problems) or "copy passed checks"


def check_auto_apply(w):
    return w.s_bool("Auto-apply recommendations off"), "Settings tick box"


def check_access(w):
    if config.DRY_RUN:
        return True, "dry run: skipped"
    lvl = str(w.settings.get("Access level", "")).strip().lower()
    if lvl not in ("basic", "explorer", "standard"):
        return False, f"Settings 'Access level' is '{lvl}' (need Basic/Explorer/Standard)"
    # A Test-level project cannot read a production account: this call proves it is not Test.
    w.ads.search("SELECT customer.id FROM customer LIMIT 1", "preflight")
    return True, f"{lvl}; production account readable"


def check_currency(w):
    if config.DRY_RUN:
        return True, "dry run: skipped"
    rows = w.ads.search("SELECT customer.currency_code FROM customer LIMIT 1", "preflight")
    cur = rows[0].customer.currency_code if rows else "?"
    return cur == "INR", f"account currency {cur}"


def check_room(w):
    if config.DRY_RUN:
        return True, "dry run: skipped"
    rows = w.ads.search("SELECT campaign.id FROM campaign WHERE campaign.status != 'REMOVED'",
                        "preflight")
    n_articles = w.con.execute("SELECT COUNT(*) n FROM articles").fetchone()["n"] or 8405
    planned = -(-n_articles // 10)
    return len(rows) + planned <= 10000, f"{len(rows)} existing + ~{planned} planned"


def _aspect_ok(schemas):
    inp = schemas.get("Input", {}).get("properties", {}).get("aspect_ratio", {})
    enum = inp.get("enum")
    if not enum:
        for ref in [inp.get("$ref", "")] + [x.get("$ref", "") for x in inp.get("allOf", [])]:
            if ref:
                enum = schemas.get(ref.split("/")[-1], {}).get("enum")
    return enum and "16:9" in enum, enum


def check_image_models(w):
    notes = []
    store = db.kv_get(w.con, "preflight_images", {})
    for key, m in config.IMAGE_MODELS.items():
        if key in store:
            notes.append(store[key]["note"])
            continue
        ok, enum = _aspect_ok(w.rep.schema_inputs(m["ref"]))
        if not ok:
            return False, f"{key}: aspect_ratio 16:9 not in schema ({enum})"
        pid = w.rep.create(m["ref"], {"prompt": "A photorealistic plated chicken pot pie on a "
                                      "wooden table, natural light, no text, no people.",
                                      **m["input"]})
        url = None
        for _ in range(60):
            status, url, err = w.rep.get(pid)
            if status in ("succeeded", "failed", "canceled"):
                break
            time.sleep(5)
        if status != "succeeded" or not url:
            return False, f"{key}: prediction {status} {err}"
        raw = w.rep.download(url)
        im = Image.open(__import__("io").BytesIO(raw))
        lp, sp = images.save_crops(f"preflight_{key}", raw)
        note = (f"{key}: output {im.size[0]}x{im.size[1]}, crops OK. Price: confirm in "
                f"Replicate billing (estimate ${m['est_usd']})")
        store[key] = {"note": note, "landscape": lp, "square": sp}
        db.kv_set(w.con, "preflight_images", store)
        notes.append(note)
    return True, " | ".join(notes)


def check_test_campaign(w):
    """Create one paused test campaign with every setting, read it back, then remove it."""
    from .gads import Ads  # noqa: F401  (real client only)
    ads = w.ads
    if config.DRY_RUN:
        return True, "dry run: skipped"
    imgs = db.kv_get(w.con, "preflight_images", {})
    sample = next(iter(imgs.values()))
    copy = db.kv_get(w.con, "preflight_test_copy")
    logos = ads.upload_logos()
    stamp = time.strftime("%Y%m%d%H%M%S")
    con = w.con
    # temporary rows so the normal operation builder is exercised end to end
    con.execute("INSERT INTO campaigns(key,wave,pool,nn,theme,name,n) VALUES(?,?,?,?,?,?,?)",
                ("preflight", 0, "TEST", 1, "Test", f"TEST preflight {stamp}", 1))
    con.execute("INSERT INTO adgroups(key,campaign_key,nn,theme,name,n,alcohol) VALUES(?,?,?,?,?,?,?)",
                ("preflight-ag", "preflight", 1, "Test", f"TEST ag {stamp}", 1, 1))
    con.execute("INSERT INTO articles(id,url,final_url,title,category,status,copy_json,"
                "landscape_path,square_path,adgroup_key) VALUES(?,?,?,?,?,?,?,?,?,?)",
                ("preflight", config.DOMAIN_PREFIX, config.DOMAIN_PREFIX, TEST_TITLE, "TEST",
                 "planned", json.dumps(copy), sample["landscape"], sample["square"],
                 "preflight-ag"))
    try:
        (ops, idx, _), = ads.ops_for_wave(0, logos)
        svc = ads.client.get_service("GoogleAdsService")
        r = svc.mutate(customer_id=ads.cid, mutate_operations=ops)
        db.log_ops(con, len(ops), "preflight")
        names = {}
        for e, x in zip(idx, r.mutate_operation_responses):
            names[e["t"]] = ads._resource_from_response(x)
            ads.apply_result(e, names[e["t"]], None)   # stored so cleanup can remove them
        camp_id = names["campaign"].split("/")[-1]
        rows = ads.search(
            "SELECT campaign.bidding_strategy_type, campaign.target_spend.cpc_bid_ceiling_micros,"
            " campaign.geo_target_type_setting.positive_geo_target_type, campaign.status,"
            " campaign.final_url_suffix, ad_group.optimized_targeting_enabled,"
            " ad_group_ad.ad.responsive_display_ad.control_spec.enable_asset_enhancements,"
            " ad_group_ad.ad.responsive_display_ad.control_spec.enable_autogen_video,"
            " ad_group_ad.policy_summary.review_status"
            f" FROM ad_group_ad WHERE campaign.id = {camp_id}", "preflight")
        x = rows[0]
        ex = ads.search("SELECT ad_group_criterion.negative, ad_group_criterion.age_range.type"
                        f" FROM ad_group_criterion WHERE campaign.id = {camp_id}"
                        " AND ad_group_criterion.type = 'AGE_RANGE'", "preflight")
        found = {
            "bidding": x.campaign.bidding_strategy_type.name,
            "cpc_cap_micros": x.campaign.target_spend.cpc_bid_ceiling_micros,
            "geo": x.campaign.geo_target_type_setting.positive_geo_target_type.name,
            "status": x.campaign.status.name,
            "utm_suffix_ok": x.campaign.final_url_suffix == config.UTM_SUFFIX,
            "optimized_targeting": x.ad_group.optimized_targeting_enabled,
            "enhancements": x.ad_group_ad.ad.responsive_display_ad.control_spec.enable_asset_enhancements,
            "autogen_video": x.ad_group_ad.ad.responsive_display_ad.control_spec.enable_autogen_video,
            "age_18_24_excluded": any(e.ad_group_criterion.negative and
                                      e.ad_group_criterion.age_range.type_.name == "AGE_RANGE_18_24"
                                      for e in ex),
            "review_status": x.ad_group_ad.policy_summary.review_status.name,
        }
        problems = []
        if found["bidding"] != "TARGET_SPEND" or found["cpc_cap_micros"] != config.MAX_CPC_INR * 1_000_000:
            problems.append("Max Clicks + Rs 2 cap NOT stored: STOP and ask Siddharth")
        if found["geo"] != "PRESENCE":
            problems.append("Presence not stored")
        if found["status"] != "PAUSED":
            problems.append("not paused")
        if not found["utm_suffix_ok"]:
            problems.append("UTM suffix differs")
        if found["optimized_targeting"]:
            problems.append("optimized targeting still ON: turn off per ad group in UI")
        if found["enhancements"] or found["autogen_video"]:
            problems.append("enhancements/auto video not off")
        if not found["age_18_24_excluded"]:
            problems.append("18-24 exclusion missing")
        return (not problems), json.dumps(found) + (" | " + "; ".join(problems) if problems else "")
    finally:
        _remove_test(w, con)


def _remove_test(w, con):
    row = con.execute("SELECT campaign_res, budget_res FROM campaigns WHERE key='preflight'").fetchone()
    ads = w.ads
    try:
        if row and row["campaign_res"]:
            op = ads.client.get_type("CampaignOperation")
            op.remove = row["campaign_res"]
            ads.client.get_service("CampaignService").mutate_campaigns(
                customer_id=ads.cid, operations=[op])
            db.log_ops(con, 1, "preflight")
        if row and row["budget_res"]:
            op = ads.client.get_type("CampaignBudgetOperation")
            op.remove = row["budget_res"]
            ads.client.get_service("CampaignBudgetService").mutate_campaign_budgets(
                customer_id=ads.cid, operations=[op])
            db.log_ops(con, 1, "preflight")
    finally:
        con.execute("DELETE FROM articles WHERE id='preflight'")
        con.execute("DELETE FROM adgroups WHERE key='preflight-ag'")
        con.execute("DELETE FROM campaigns WHERE key='preflight'")
