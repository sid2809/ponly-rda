"""Google Ads: build operations, upload via BatchJob (resumable), validate-only test,
enable waves, daily sync. Every API request is logged against the daily quota."""
import json
import os
import tempfile
import time

from . import config, db

API_VERSION = "v26"


def make_client():
    from google.ads.googleads.client import GoogleAdsClient
    cfg = {"use_proto_plus": True}
    if config.DEVELOPER_TOKEN:
        cfg["developer_token"] = config.DEVELOPER_TOKEN
    if config.LOGIN_CUSTOMER_ID:
        cfg["login_customer_id"] = config.LOGIN_CUSTOMER_ID
    if config.ADS_USE_SERVICE_ACCOUNT:
        fd, path = tempfile.mkstemp(suffix=".json")   # ephemeral, not on the volume
        with os.fdopen(fd, "w") as f:
            f.write(config.SA_JSON)
        os.chmod(path, 0o600)
        cfg["json_key_file_path"] = path
    else:
        cfg.update(client_id=config.OAUTH_CLIENT_ID, client_secret=config.OAUTH_CLIENT_SECRET,
                   refresh_token=config.OAUTH_REFRESH_TOKEN)
    return GoogleAdsClient.load_from_dict(cfg, version=API_VERSION)


class Ads:
    def __init__(self, con, client=None):
        self.con = con
        self.client = client or make_client()
        self.cid = config.CUSTOMER_ID
        self.e = self.client.enums

    # ---------- helpers ----------
    def _op(self):
        return self.client.get_type("MutateOperation")

    _COLL = {"campaign_budget_path": "campaignBudgets", "campaign_path": "campaigns",
             "ad_group_path": "adGroups", "asset_path": "assets"}

    def path(self, service, method, rid):
        """Resource name, e.g. customers/123/campaigns/-1 (no API call)."""
        return f"customers/{self.cid}/{self._COLL[method]}/{rid}"

    def search(self, query, kind="search"):
        svc = self.client.get_service("GoogleAdsService")
        db.log_ops(self.con, 1, kind)
        rows = []
        for batch in svc.search_stream(customer_id=self.cid, query=query):
            rows.extend(batch.results)
        return rows

    # ---------- operation builders ----------
    def ops_for_wave(self, wave, logos, temp_start=-1):
        """Build MutateOperations for every entity of this wave that has no resource yet.
        Returns (units) where each unit = (ops, index_entries, approx_bytes) per campaign."""
        con = self.con
        tid = [temp_start]

        def temp():
            v = tid[0]
            tid[0] -= 1
            return v

        units = []
        camps = con.execute("SELECT * FROM campaigns WHERE wave=? AND status!='failed' ORDER BY key",
                            (wave,)).fetchall()
        for c in camps:
            ops, idx, nbytes = [], [], 0
            budget_res = c["budget_res"]
            if not budget_res:
                budget_res = self.path("CampaignBudgetService", "campaign_budget_path", temp())
                op = self._op()
                b = op.campaign_budget_operation.create
                b.resource_name = budget_res
                b.name = f"{c['name']} | budget"
                b.amount_micros = config.BUDGET_PER_ARTICLE_INR * c["n"] * 1_000_000
                b.delivery_method = self.e.BudgetDeliveryMethodEnum.STANDARD
                b.explicitly_shared = False
                ops.append(op); idx.append({"t": "budget", "k": c["key"]})
            camp_res = c["campaign_res"]
            if not camp_res:
                camp_res = self.path("CampaignService", "campaign_path", temp())
                op = self._op()
                x = op.campaign_operation.create
                x.resource_name = camp_res
                x.name = c["name"]
                x.status = self.e.CampaignStatusEnum.PAUSED
                x.advertising_channel_type = self.e.AdvertisingChannelTypeEnum.DISPLAY
                x.campaign_budget = budget_res
                x.target_spend.cpc_bid_ceiling_micros = config.MAX_CPC_INR * 1_000_000
                x.contains_eu_political_advertising = (
                    self.e.EuPoliticalAdvertisingStatusEnum.DOES_NOT_CONTAIN_EU_POLITICAL_ADVERTISING)
                x.geo_target_type_setting.positive_geo_target_type = (
                    self.e.PositiveGeoTargetTypeEnum.PRESENCE)
                x.network_settings.target_content_network = True
                x.network_settings.target_google_search = False
                x.network_settings.target_search_network = False
                x.final_url_suffix = config.UTM_SUFFIX
                ops.append(op); idx.append({"t": "campaign", "k": c["key"]})
            for col, kind, value in (("geo_res", "geo", config.GEO_US),
                                     ("lang_res", "lang", config.LANG_EN)):
                if not c[col]:
                    op = self._op()
                    cc = op.campaign_criterion_operation.create
                    cc.campaign = camp_res
                    if kind == "geo":
                        cc.location.geo_target_constant = value
                    else:
                        cc.language.language_constant = value
                    ops.append(op); idx.append({"t": kind, "k": c["key"]})
            ags = con.execute("SELECT * FROM adgroups WHERE campaign_key=? AND COALESCE(attempts,0)<3"
                              " ORDER BY nn", (c["key"],)).fetchall()
            for g in ags:
                ag_res = g["adgroup_res"]
                if not ag_res:
                    ag_res = self.path("AdGroupService", "ad_group_path", temp())
                    op = self._op()
                    a = op.ad_group_operation.create
                    a.resource_name = ag_res
                    a.name = g["name"]
                    a.campaign = camp_res
                    a.type_ = self.e.AdGroupTypeEnum.DISPLAY_STANDARD
                    a.status = self.e.AdGroupStatusEnum.ENABLED
                    a.optimized_targeting_enabled = False
                    ops.append(op); idx.append({"t": "adgroup", "k": g["key"]})
                if g["alcohol"] and not g["exclusion_res"]:
                    op = self._op()
                    cr = op.ad_group_criterion_operation.create
                    cr.ad_group = ag_res
                    cr.negative = True
                    cr.age_range.type_ = self.e.AgeRangeTypeEnum.AGE_RANGE_18_24
                    ops.append(op); idx.append({"t": "exclusion", "k": g["key"]})
                arts = con.execute("SELECT * FROM articles WHERE adgroup_key=? AND status='planned'"
                                   " ORDER BY id", (g["key"],)).fetchall()
                for art in arts:
                    imgs = {}
                    for col, path_col, tag in (("landscape_asset", "landscape_path", "L"),
                                               ("square_asset", "square_path", "S")):
                        res = art[col]
                        if not res:
                            res = self.path("AssetService", "asset_path", temp())
                            with open(art[path_col], "rb") as f:
                                data = f.read()
                            op = self._op()
                            s = op.asset_operation.create
                            s.resource_name = res
                            s.name = f"ponly {art['id']} {tag}"
                            s.type_ = self.e.AssetTypeEnum.IMAGE
                            s.image_asset.data = data
                            nbytes += len(data)
                            ops.append(op); idx.append({"t": "asset" + tag, "k": art["id"]})
                        imgs[tag] = res
                    ops.append(self._ad_op(ag_res, art, imgs, logos))
                    idx.append({"t": "ad", "k": art["id"]})
            if ops:
                units.append((ops, idx, nbytes + 2000 * len(ops)))
        return units

    def _ad_op(self, ag_res, art, imgs, logos):
        copy = json.loads(art["copy_json"])
        op = self._op()
        aga = op.ad_group_ad_operation.create
        aga.ad_group = ag_res
        aga.status = self.e.AdGroupAdStatusEnum.ENABLED
        ad = aga.ad
        ad.final_urls.append(art["final_url"])
        rda = ad.responsive_display_ad
        for h in copy["headlines"]:
            t = self.client.get_type("AdTextAsset"); t.text = h; rda.headlines.append(t)
        rda.long_headline.text = copy["long_headline"]
        for d in copy["descriptions"]:
            t = self.client.get_type("AdTextAsset"); t.text = d; rda.descriptions.append(t)
        for field, res in (("marketing_images", imgs["L"]), ("square_marketing_images", imgs["S"]),
                           ("logo_images", logos["landscape"]),
                           ("square_logo_images", logos["square"])):
            im = self.client.get_type("AdImageAsset"); im.asset = res
            getattr(rda, field).append(im)
        rda.business_name = config.BUSINESS_NAME
        rda.control_spec.enable_asset_enhancements = False
        rda.control_spec.enable_autogen_video = False
        return op

    # ---------- result handling ----------
    def apply_result(self, entry, resource_name, error):
        con, k, t = self.con, entry["k"], entry["t"]
        if error:
            msg = f"{t}: {error}"[:500]
            if t in ("ad", "assetL", "assetS"):
                a = con.execute("SELECT ad_attempts, status FROM articles WHERE id=?",
                                (k,)).fetchone()
                if a["status"] == "error":   # already explained by its campaign/ad group
                    return
                n = (a["ad_attempts"] or 0) + 1
                fields = {"ad_attempts": n, "error": f"upload {msg}"}
                if n >= 3:
                    fields.update(status="error")
                db.update_article(con, k, **fields)
            elif t in ("budget", "campaign", "geo", "lang"):
                c = con.execute("SELECT * FROM campaigns WHERE key=?", (k,)).fetchone()
                # count one attempt per job: on the campaign op, or on geo/lang once it exists
                counts = t == "campaign" or (t in ("geo", "lang") and c["campaign_res"])
                n = (c["attempts"] or 0) + (1 if counts else 0)
                con.execute("UPDATE campaigns SET error=?, attempts=? WHERE key=?", (msg, n, k))
                if n >= 3:
                    con.execute("UPDATE campaigns SET status='failed' WHERE key=?", (k,))
                    for a in con.execute("SELECT a.id FROM articles a JOIN adgroups g ON "
                                         "a.adgroup_key=g.key WHERE g.campaign_key=? AND "
                                         "a.status='planned'", (k,)).fetchall():
                        db.update_article(con, a["id"], status="error",
                                          error=f"campaign not created: {msg}")
            elif t in ("adgroup", "exclusion"):
                g = con.execute("SELECT * FROM adgroups WHERE key=?", (k,)).fetchone()
                counts = t == "adgroup" or (t == "exclusion" and g["adgroup_res"])
                n = (g["attempts"] or 0) + (1 if counts else 0)
                con.execute("UPDATE adgroups SET error=?, attempts=? WHERE key=?", (msg, n, k))
                if n >= 3:
                    for a in con.execute("SELECT id FROM articles WHERE adgroup_key=? AND "
                                         "status='planned'", (k,)).fetchall():
                        db.update_article(con, a["id"], status="error",
                                          error=f"ad group not created: {msg}")
            return
        col = {"budget": ("campaigns", "budget_res"), "campaign": ("campaigns", "campaign_res"),
               "geo": ("campaigns", "geo_res"), "lang": ("campaigns", "lang_res"),
               "adgroup": ("adgroups", "adgroup_res"),
               "exclusion": ("adgroups", "exclusion_res")}.get(t)
        if col:
            con.execute(f"UPDATE {col[0]} SET {col[1]}=? WHERE key=?", (resource_name, k))
            if t == "campaign":
                con.execute("UPDATE campaigns SET status='created' WHERE key=?", (k,))
        elif t in ("assetL", "assetS"):
            db.update_article(con, k, **{("landscape_asset" if t == "assetL" else
                                          "square_asset"): resource_name})
            a = con.execute("SELECT * FROM articles WHERE id=?", (k,)).fetchone()
            if a["landscape_asset"] and a["square_asset"] and k != "preflight" and not config.KEEP_IMAGES:
                for p in (a["landscape_path"], a["square_path"]):   # Google has it now
                    if p and os.path.exists(p):
                        os.remove(p)
        elif t == "ad":
            db.update_article(con, k, ad_resource=resource_name,
                              ad_id=resource_name.split("~")[-1], status="uploaded", error=None)

    @staticmethod
    def _resource_from_response(resp):
        """MutateOperationResponse -> resource name of whichever field is set."""
        field = resp._pb.WhichOneof("response")
        return getattr(resp, field).resource_name if field else ""

    # ---------- batch job (resumable) ----------
    def run_upload(self, wave, logos, quota_left):
        """Upload whatever is missing for this wave, sized to remaining quota.
        Returns number of operations submitted (0 = nothing to do or no quota)."""
        con = self.con
        # 1) Resume any unfinished job first; never create a new one while one is open.
        for j in db.open_jobs(con, "gads"):
            self._resume(j)
            return -1
        units = self.ops_for_wave(wave, logos)
        if not units:
            return 0
        chosen, total = [], 0
        for u in units:
            if total + len(u[0]) + 20 > quota_left:
                break
            chosen.append(u)
            total += len(u[0])
        if not chosen:
            return 0
        ops = [o for u in chosen for o in u[0]]
        index = [e for u in chosen for e in u[1]]
        svc = self.client.get_service("BatchJobService")
        jid = db.new_job(con, "gads", wave, payload={"index": index}, state="creating")
        bop = self.client.get_type("BatchJobOperation")
        bop.create = self.client.get_type("BatchJob")
        resp = svc.mutate_batch_job(customer_id=self.cid, operation=bop)
        db.log_ops(con, 1, "batch_create")
        res = resp.result.resource_name
        db.set_job(con, jid, external_id=res, state="adding")   # saved before adding/running
        token, i = None, 0
        while i < len(ops):
            part, nbytes = [], 0
            while i < len(ops) and len(part) < config.OPS_PER_ADD_REQUEST:
                size = ops[i]._pb.ByteSize()
                if part and nbytes + size > config.BYTES_PER_ADD_REQUEST:
                    break
                part.append(ops[i]); nbytes += size; i += 1
            kw = {"resource_name": res, "mutate_operations": part}
            if token:
                kw["sequence_token"] = token
            r = svc.add_batch_job_operations(**kw)
            token = r.next_sequence_token
            db.log_ops(con, len(part), "batch_ops")
        svc.run_batch_job(resource_name=res)
        db.log_ops(con, 1, "batch_run")
        db.set_job(con, jid, state="running")
        return len(ops)

    def _resume(self, job):
        con = self.con
        res = job["external_id"]
        if not res:            # crashed before the job existed server-side
            db.set_job(con, job["id"], state="abandoned")
            return
        rows = self.search(f"SELECT batch_job.status FROM batch_job "
                           f"WHERE batch_job.resource_name = '{res}'", "batch_poll")
        status = rows[0].batch_job.status.name if rows else "UNKNOWN"
        if status == "PENDING":
            # Never ran -> nothing was created. Abandon; Google deletes it after 7 days.
            db.set_job(con, job["id"], state="abandoned")
            return
        if status != "DONE":
            return   # RUNNING: check again next loop
        index = json.loads(job["payload"])["index"]
        svc = self.client.get_service("BatchJobService")
        for r in svc.list_batch_job_results(resource_name=res, page_size=1000):
            entry = index[r.operation_index]
            err = r.status.message if r.status and r.status.code else None
            name = None if err else self._resource_from_response(r.mutate_operation_response)
            self.apply_result(entry, name, err)
        db.log_ops(con, max(1, len(index) // 1000 + 1), "batch_results")
        db.set_job(con, job["id"], state="done")

    # ---------- validate-only (Phase 1 test: nothing is created) ----------
    def validate_wave(self, wave):
        """Validate-only: logos are sent as temporary assets inside each request, so
        nothing at all is created in the account."""
        svc = self.client.get_service("GoogleAdsService")
        errors = []
        logo_ops, logos = [], {}
        for tag, path, rid in (("landscape", config.LOGO_LANDSCAPE, -900001),
                               ("square", config.LOGO_SQUARE, -900002)):
            op = self._op()
            a = op.asset_operation.create
            a.resource_name = self.path("AssetService", "asset_path", rid)
            a.name = f"ponly logo {tag}"
            a.type_ = self.e.AssetTypeEnum.IMAGE
            with open(path, "rb") as f:
                a.image_asset.data = f.read()
            logo_ops.append(op)
            logos[tag] = a.resource_name
        for ops, idx, _ in self.ops_for_wave(wave, logos):
            try:
                svc.mutate(customer_id=self.cid, mutate_operations=logo_ops + ops,
                           validate_only=True)
            except Exception as e:
                errors.append(str(e)[:2000])
            db.log_ops(self.con, len(ops) + 2, "validate")
        return errors

    # ---------- logos ----------
    def upload_logos(self):
        logos = db.kv_get(self.con, "logos")
        if logos:
            return logos
        svc = self.client.get_service("AssetService")
        ops = []
        for tag, path in (("landscape", config.LOGO_LANDSCAPE), ("square", config.LOGO_SQUARE)):
            op = self.client.get_type("AssetOperation")
            a = op.create
            a.name = f"ponly logo {tag}"
            a.type_ = self.e.AssetTypeEnum.IMAGE
            with open(path, "rb") as f:
                a.image_asset.data = f.read()
            ops.append(op)
        r = svc.mutate_assets(customer_id=self.cid, operations=ops)
        db.log_ops(self.con, 2, "logos")
        logos = {"landscape": r.results[0].resource_name, "square": r.results[1].resource_name}
        db.kv_set(self.con, "logos", logos)
        return logos

    # ---------- enable ----------
    def enable_wave(self, wave):
        con = self.con
        rows = con.execute("SELECT key, campaign_res FROM campaigns WHERE wave=? AND "
                           "campaign_res IS NOT NULL AND status='created'", (wave,)).fetchall()
        svc = self.client.get_service("CampaignService")
        from google.api_core import protobuf_helpers
        for i in range(0, len(rows), 1000):
            ops = []
            for r in rows[i:i + 1000]:
                op = self.client.get_type("CampaignOperation")
                op.update.resource_name = r["campaign_res"]
                op.update.status = self.e.CampaignStatusEnum.ENABLED
                self.client.copy_from(op.update_mask, protobuf_helpers.field_mask(None, op.update._pb))
                ops.append(op)
            svc.mutate_campaigns(customer_id=self.cid, operations=ops)
            db.log_ops(con, len(ops), "enable")
            for r in rows[i:i + 1000]:
                con.execute("UPDATE campaigns SET status='enabled', enabled_at=? WHERE key=?",
                            (time.time(), r["key"]))
        return len(rows)

    # ---------- daily sync ----------
    def sync(self, since_date):
        con = self.con
        ids = [r["campaign_res"].split("/")[-1] for r in con.execute(
            "SELECT campaign_res FROM campaigns WHERE campaign_res IS NOT NULL")]
        by_ad = {r["ad_resource"]: r["id"] for r in con.execute(
            "SELECT id, ad_resource FROM articles WHERE ad_resource IS NOT NULL")}
        enh_on = 0
        for i in range(0, len(ids), 400):
            id_list = ",".join(ids[i:i + 400])
            q = ("SELECT ad_group_ad.resource_name, ad_group_ad.policy_summary.approval_status, "
                 "ad_group_ad.policy_summary.review_status, "
                 "ad_group_ad.policy_summary.policy_topic_entries, "
                 "ad_group_ad.ad.responsive_display_ad.control_spec.enable_asset_enhancements, "
                 "ad_group_ad.ad.responsive_display_ad.control_spec.enable_autogen_video "
                 f"FROM ad_group_ad WHERE campaign.id IN ({id_list})")
            for row in self.search(q, "sync"):
                aga = row.ad_group_ad
                aid = by_ad.get(aga.resource_name)
                if not aid:
                    continue
                topics = "; ".join(t.topic for t in aga.policy_summary.policy_topic_entries)
                cs = aga.ad.responsive_display_ad.control_spec
                if cs.enable_asset_enhancements or cs.enable_autogen_video:
                    enh_on += 1
                db.update_article(con, aid, approval=aga.policy_summary.approval_status.name,
                                  policy_note=topics or None)
            if since_date:
                q2 = ("SELECT ad_group_ad.resource_name, metrics.impressions, metrics.clicks "
                      f"FROM ad_group_ad WHERE campaign.id IN ({id_list}) "
                      f"AND segments.date BETWEEN '{since_date}' AND '{time.strftime('%Y-%m-%d')}'")
                for row in self.search(q2, "sync"):
                    aid = by_ad.get(row.ad_group_ad.resource_name)
                    if aid:
                        db.update_article(con, aid, impressions=row.metrics.impressions,
                                          clicks=row.metrics.clicks)
        db.kv_set(con, "enhancements_on_count", enh_on)
        return enh_on
