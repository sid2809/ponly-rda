"""One-off placement quota test (Siddharth, 1 Nov 2026). Runs once, when "Run placement
test" is ticked in Settings (whether or not Pause is ticked).

Question: does adding placements count against the daily Google Ads API quota
(Basic access: 15,000 operations per Cloud project, sliding 24 hours)?

What it does:
  1. picks 20 different random articles from the live pilot (10 per campaign); their copy,
     images and Google Ads image assets already exist, so nothing new is generated or uploaded.
     Alcohol articles are left out, so no test ad group needs the 18-24 exclusion;
  2. campaign 1: PAUSED, pipeline settings (Max Clicks + Rs 2 cap, US presence, English,
     Display only), 1 ad group (optimized targeting off, placements set to Targeting),
     10 ads, then ALL placements from the Sheet tab "Placements" in ONE request;
  3. reads campaign 1 back: placements stored, Targeting (not Observation), optimized off;
  4. campaign 2: same, but placements are sent 500 per request until all are in or Google
     returns a quota error;
  5. reads campaign 2 back (waits for quota if needed) and writes everything to the
     Sheet tab "Placement test" and the logs.

How to read it: if placements count 1 operation each, campaign 2 stops near
15,000 - (operations the worker sent in the 24 h before) - campaign 1. If campaign 2 gets
every placement in, placements did not use up the quota at 1 operation each.
Both campaigns stay paused. Nothing is removed."""
import json
import random
import re
import time

from . import config, db
from .gads import QuotaExhausted, is_quota_error, _q
from .log import short

KEY = "placement_test"
SETTING = "Run placement test"
TAB_IN = "Placements"
TAB_OUT = "Placement test"
CHUNK_2 = 500
DOMAIN = re.compile(r"^[a-z0-9.-]+\.[a-z]{2,}$")
NAMES = ("Placement test 1 (Nov 2026)", "Placement test 2 (Nov 2026)")


def wanted(w):
    return bool(w.settings) and w.s_bool(SETTING) and not db.kv_get(w.con, KEY, {}).get("done")


def load_domains(sheet):
    rows = sheet.read(TAB_IN)
    vals = [str(r[0]).strip().lower() for r in rows if r and str(r[0]).strip()]
    if vals and not DOMAIN.match(vals[0]):
        vals = vals[1:]                          # header row
    bad = [v for v in vals if not DOMAIN.match(v)]
    dup = len(vals) - len(set(vals))
    if not vals or bad or dup:
        raise RuntimeError(f"placement test: tab '{TAB_IN}' must hold one plain domain per row "
                           f"(found {len(vals)} rows, {len(bad)} not plain domains e.g. "
                           f"{bad[:3]}, {dup} duplicates)")
    return vals


def pick_articles(con):
    ids = [r["id"] for r in con.execute(
        "SELECT id FROM articles WHERE wave=1 AND ad_resource IS NOT NULL AND "
        "COALESCE(alcohol,0)=0 AND copy_json IS NOT NULL AND landscape_asset IS NOT NULL AND "
        "square_asset IS NOT NULL AND final_url IS NOT NULL")]
    if len(ids) < 20:
        raise RuntimeError(f"placement test: only {len(ids)} usable pilot articles (need 20)")
    chosen = random.sample(ids, 20)
    return [chosen[:10], chosen[10:]]


def _save(con, st):
    db.kv_set(con, KEY, st)


def _create_campaign(w, st, i, logos):
    """Budget + paused campaign + geo + language + ad group + 10 ads, all or nothing."""
    ads, con = w.ads, w.con
    c = st["c"][i]
    name = NAMES[i]
    found = ads.search("SELECT campaign.resource_name, campaign.status FROM campaign WHERE "
                       f"campaign.name = {_q(name)} AND campaign.status != 'REMOVED'", KEY)
    c["ops"] += 1
    if found:                    # created before a crash: never create a second one
        camp = found[0].campaign.resource_name
        ag = ads.search("SELECT ad_group.resource_name FROM ad_group WHERE campaign.resource_name"
                        f" = {_q(camp)}", KEY)
        c["ops"] += 1
        c.update(campaign=camp, adgroup=ag[0].ad_group.resource_name if ag else None,
                 note="found after a restart; not created again")
        return
    e, ops = ads.e, []
    tid = iter(range(-1, -100, -1))
    bud = ads.path("CampaignBudgetService", "campaign_budget_path", next(tid))
    camp = ads.path("CampaignService", "campaign_path", next(tid))
    ag = ads.path("AdGroupService", "ad_group_path", next(tid))
    op = ads._op(); b = op.campaign_budget_operation.create
    b.resource_name = bud
    b.name = f"{name} | budget"
    b.amount_micros = config.BUDGET_PER_ARTICLE_INR * 10 * 1_000_000
    b.delivery_method = e.BudgetDeliveryMethodEnum.STANDARD
    b.explicitly_shared = False
    ops.append(op)
    op = ads._op(); x = op.campaign_operation.create
    x.resource_name = camp
    x.name = name
    x.status = e.CampaignStatusEnum.PAUSED
    x.advertising_channel_type = e.AdvertisingChannelTypeEnum.DISPLAY
    x.campaign_budget = bud
    x.target_spend.cpc_bid_ceiling_micros = config.MAX_CPC_INR * 1_000_000
    x.contains_eu_political_advertising = (
        e.EuPoliticalAdvertisingStatusEnum.DOES_NOT_CONTAIN_EU_POLITICAL_ADVERTISING)
    x.geo_target_type_setting.positive_geo_target_type = e.PositiveGeoTargetTypeEnum.PRESENCE
    x.network_settings.target_content_network = True
    x.network_settings.target_google_search = False
    x.network_settings.target_search_network = False
    x.final_url_suffix = config.UTM_SUFFIX
    ops.append(op)
    for kind in ("geo", "lang"):
        op = ads._op(); cc = op.campaign_criterion_operation.create
        cc.campaign = camp
        if kind == "geo":
            cc.location.geo_target_constant = config.GEO_US
        else:
            cc.language.language_constant = config.LANG_EN
        ops.append(op)
    op = ads._op(); a = op.ad_group_operation.create
    a.resource_name = ag
    a.name = f"{name} | ad group"
    a.campaign = camp
    a.type_ = e.AdGroupTypeEnum.DISPLAY_STANDARD
    a.status = e.AdGroupStatusEnum.ENABLED
    a.optimized_targeting_enabled = False
    tr = ads.client.get_type("TargetRestriction")
    tr.targeting_dimension = e.TargetingDimensionEnum.PLACEMENT
    tr.bid_only = False                      # Targeting (not Observation)
    a.targeting_setting.target_restrictions.append(tr)
    ops.append(op)
    for aid in st["articles"][i]:
        art = con.execute("SELECT * FROM articles WHERE id=?", (aid,)).fetchone()
        ops.append(ads._ad_op(ag, art, {"L": art["landscape_asset"], "S": art["square_asset"]},
                              logos))
    c["ops"] += len(ops)
    db.log_ops(con, len(ops), KEY)
    r = ads.client.get_service("GoogleAdsService").mutate(customer_id=ads.cid,
                                                          mutate_operations=ops)
    names = [ads._resource_from_response(x) for x in r.mutate_operation_responses]
    c.update(budget=names[0], campaign=names[1], adgroup=names[4], ads=len(names) - 5)


def _placement_ops(ads, ag, domains):
    ops = []
    for d in domains:
        op = ads.client.get_type("AdGroupCriterionOperation")
        cr = op.create
        cr.ad_group = ag
        cr.status = ads.e.AdGroupCriterionStatusEnum.ENABLED
        cr.placement.url = d
        ops.append(op)
    return ops


def _send(w, c, ag, domains):
    """One request, partial failure on. Returns {domain: error} for rejected ones."""
    ads = w.ads
    ops = _placement_ops(ads, ag, domains)
    c["ops"] += len(ops)
    db.log_ops(w.con, len(ops), KEY)
    resp = ads.client.get_service("AdGroupCriterionService").mutate_ad_group_criteria(
        request={"customer_id": ads.cid, "operations": ops, "partial_failure": True})
    failed = ads.partial_failures(resp)
    if None in failed:
        return {d: failed[None] for d in domains}
    return {domains[i]: m for i, m in failed.items()}


def _readback(w, c):
    ads = w.ads
    ag = c["adgroup"]
    rows = ads.search("SELECT ad_group_criterion.placement.url FROM ad_group_criterion WHERE "
                      f"ad_group.resource_name = {_q(ag)} AND ad_group_criterion.type = "
                      "'PLACEMENT' AND ad_group_criterion.status != 'REMOVED' AND "
                      "ad_group_criterion.negative = FALSE", KEY)
    g = ads.search("SELECT ad_group.optimized_targeting_enabled, "
                   "ad_group.targeting_setting.target_restrictions FROM ad_group WHERE "
                   f"ad_group.resource_name = {_q(ag)}", KEY)
    c["ops"] += 2
    tr = [(t.targeting_dimension.name, t.bid_only)
          for t in g[0].ad_group.targeting_setting.target_restrictions] if g else []
    plc = [b for d, b in tr if d == "PLACEMENT"]
    c["readback"] = {
        "placements_stored": len(rows),
        "placement_setting": ("Targeting" if plc == [False] else "Observation" if plc == [True]
                              else f"not stored ({tr})"),
        "optimized_targeting": g[0].ad_group.optimized_targeting_enabled if g else None,
        "time": time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime())}
    return {r.ad_group_criterion.placement.url for r in rows}


def run(w):
    con, ads = w.con, w.ads
    st = db.kv_get(con, KEY, {})
    domains = load_domains(w.sheet)
    if not st:
        st = {"articles": pick_articles(con), "domains": len(domains),
              "worker_ops_24h_at_start": db.ops_last_24h(con),
              "started": time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime()),
              "c": [{"ops": 0, "rejected": {}} for _ in range(2)]}
        _save(con, st)
        w.log(f"placement test: started with {len(domains)} domains; articles "
              f"{st['articles']}; worker operations in the last 24 h: "
              f"{st['worker_ops_24h_at_start']}")
    logos = ads.upload_logos()
    try:
        for i in range(2):
            c = st["c"][i]
            if not c.get("campaign"):
                _create_campaign(w, st, i, logos)
                _save(con, st)
                w.log(f"placement test: campaign {i + 1} created (paused): {c['campaign']}")
            if i == 0 and not c.get("placements_done"):
                if c.get("in_flight"):          # crashed mid-request: read, never resend
                    have = _readback(w, c)
                    c["note"] = "restart during the placement request; counted by read-back"
                    c["accepted"] = len(have)
                else:
                    c["in_flight"] = True
                    c["worker_ops_24h_before"] = db.ops_last_24h(con)
                    _save(con, st)
                    rej = _send(w, c, c["adgroup"], domains)
                    c["rejected"] = rej
                    c["accepted"] = len(domains) - len(rej)
                c["in_flight"] = False
                c["placements_done"] = True
                _save(con, st)
                _readback(w, c)
                _save(con, st)
                w.log(f"placement test: campaign 1: {c['accepted']} placements accepted, "
                      f"{len(c['rejected'])} rejected, read back {c['readback']}")
            if i == 1 and not c.get("placements_done"):
                have = _readback(w, c) if c.get("sent") else set()
                c.setdefault("worker_ops_24h_before", db.ops_last_24h(con))
                todo = [d for d in domains if d not in have and d not in c["rejected"]]
                c["sent"] = c.get("sent", 0)
                for k in range(0, len(todo), CHUNK_2):
                    part = todo[k:k + CHUNK_2]
                    rej = _send(w, c, c["adgroup"], part)
                    c["rejected"].update(rej)
                    c["sent"] += len(part)
                    _save(con, st)
                c["placements_done"] = True
                c["result"] = "all placements sent; no quota error"
                _save(con, st)
    except Exception as e:
        if not (isinstance(e, QuotaExhausted) or is_quota_error(e)):
            raise
        c = st["c"][1] if st["c"][0].get("placements_done") else st["c"][0]
        c["quota_error"] = short(str(e), 500)
        c["quota_error_time"] = time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime())
        c["worker_ops_24h_at_error"] = db.ops_last_24h(con)
        c["placements_done"] = True
        c["result"] = "stopped by Google's quota error"
        _save(con, st)
        w.log(f"placement test: QUOTA ERROR on campaign {st['c'].index(c) + 1} after "
              f"{c.get('sent', c.get('accepted', 0))} placements sent: {c['quota_error']}")
        db.kv_set(con, "quota_wait_until", time.time() + 3600)
    # final read-back of campaign 2 (needs quota; retried every loop until it works)
    c2 = st["c"][1]
    if c2.get("adgroup") and "readback" not in c2:
        try:
            _readback(w, c2)
        except Exception as e:
            if isinstance(e, QuotaExhausted) or is_quota_error(e):
                _save(con, st)
                write_report(w, st)
                return                  # read back later, when the quota allows
            raise
    st["done"] = True
    st["finished"] = time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime())
    _save(con, st)
    write_report(w, st)
    w.log("placement test: finished; see the 'Placement test' tab")


def report_rows(st):
    c1, c2 = st["c"]
    rows = [["Item", "Value"],
            ["Started", st.get("started")], ["Finished", st.get("finished", "not yet")],
            ["Domains in the Placements tab", st.get("domains")],
            ["Worker operations in the 24 h before the test (worker's own count)",
             st.get("worker_ops_24h_at_start")]]
    for n, c in ((1, c1), (2, c2)):
        rb = c.get("readback", {})
        rows += [[f"Campaign {n}", c.get("campaign") or "not created"],
                 [f"Campaign {n}: operations sent (worker's count)", c.get("ops")],
                 [f"Campaign {n}: worker operations in the 24 h before its placements",
                  c.get("worker_ops_24h_before")],
                 [f"Campaign {n}: placements sent", c.get("sent", c.get("accepted"))],
                 [f"Campaign {n}: rejected by Google", len(c.get("rejected", {}))],
                 [f"Campaign {n}: result", c.get("result", "done" if c.get("placements_done")
                                                 else "not run")],
                 [f"Campaign {n}: quota error", c.get("quota_error", "none")],
                 [f"Campaign {n}: worker operations in the 24 h at the quota error",
                  c.get("worker_ops_24h_at_error", "")],
                 [f"Campaign {n}: placements stored (read back)",
                  rb.get("placements_stored", "not read yet")],
                 [f"Campaign {n}: placement setting (read back)", rb.get("placement_setting", "")],
                 [f"Campaign {n}: optimized targeting (read back)",
                  rb.get("optimized_targeting", "")]]
    rows += [["How to read it", "If placements count 1 operation each, campaign 2 stops near "
              "15,000 minus the worker's operations in the 24 h before its placements. If "
              "campaign 2 stored every placement, they did not use up the quota at 1 each. "
              "Other tools on the Cloud project are not in the worker's count."],
             ["", ""], ["Rejected domain", "Google's message"]]
    rej = {**c1.get("rejected", {}), **c2.get("rejected", {})}
    rows += [[d, m] for d, m in sorted(rej.items())]
    return rows


def write_report(w, st):
    try:
        if hasattr(w.sheet, "ensure_tab"):
            w.sheet.ensure_tab(TAB_OUT)
        w.sheet.write(TAB_OUT, report_rows(st), force=True)
    except Exception as e:
        w.log(f"placement test: report not written: {e}")
    w.log("placement test report: " + json.dumps(
        {k: v for k, v in st.items() if k != "articles"}, default=str)[:3000])
