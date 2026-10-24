"""Google Ads: build operations, upload via BatchJob (resumable, never double-creates),
validate-only test, labels, enable waves, daily sync. Every API request is logged against
the daily quota. Network calls never run inside a database transaction."""
import datetime
import json
import os
import tempfile
import time

from . import config, db
from .log import log, short

API_VERSION = "v26"
MAX_ASSET_BYTES = 8_000_000          # one op must fit in a 9 MB add request (P8)
LOST_TRIES = 10                      # missing job / unlistable results (R12)
RUNNING_REPORT_S = 24 * 3600
RECONCILE_DELAY_S = 900              # let Google's search index catch up before adopting
JOB_MAX_BYTES = 150_000_000          # one upload job holds at most ~150 MB of operations
EXCL_TRIES = 5                       # 18-24 exclusion attempts before the campaign is held
ENABLE_TRIES = 3


def is_quota_error(e):
    """RESOURCE_EXHAUSTED from a Google Ads call (search or mutate) only: a Google Sheets 429
    or another service's limit must not pause the Google Ads steps."""
    if isinstance(e, QuotaExhausted):
        return True
    if type(e).__name__ != "GoogleAdsException":
        return False
    code = ""
    try:
        code = e.error.code().name
    except Exception:
        pass
    return "RESOURCE_EXHAUSTED" in code + str(e) + str(getattr(e, "failure", ""))


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


def _q(name):
    """Quote a string for GAQL."""
    return "'" + name.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _chunks(seq, n=200):
    seq = list(seq)
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def asset_name(art, tag):
    """Image asset name. Images made again get a new version, so names stay unique
    (first-run assets were named without the version)."""
    v = art["image_attempts"] or 1
    return f"ponly {art['id']} {tag}" if v <= 1 else f"ponly {art['id']} {tag} v{v}"


class QuotaExhausted(Exception):
    pass


class Ads:
    def __init__(self, con, client=None):
        self.con = con
        self.client = client or make_client()
        self.cid = config.CUSTOMER_ID
        self.e = self.client.enums
        self.progress = lambda: None      # the worker sets this to its lease heartbeat

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
        try:
            for batch in svc.search_stream(customer_id=self.cid, query=query):
                rows.extend(batch.results)
        except Exception as e:
            if is_quota_error(e):
                raise QuotaExhausted(str(e)[:300])
            raise
        return rows

    def decode_status(self, status):
        """BatchJob result status -> readable message (R16/R18)."""
        msgs = []
        for d in getattr(status, "details", []) or []:
            if not str(getattr(d, "type_url", "")).endswith("GoogleAdsFailure"):
                continue
            try:
                f = self.client.get_type("GoogleAdsFailure")
                failure = type(f).deserialize(d.value)
                msgs += [f"{e.error_code}: {e.message}".replace("\n", " ") for e in failure.errors]
            except Exception:
                pass
        return "; ".join(msgs) or status.message or f"error code {status.code}"

    def partial_failures(self, response):
        """{operation index: message} from a partial_failure_error (R15)."""
        out = {}
        pfe = getattr(response, "partial_failure_error", None)
        if not pfe or not getattr(pfe, "code", 0):
            return out
        for d in pfe.details:
            try:
                f = self.client.get_type("GoogleAdsFailure")
                failure = type(f).deserialize(d.value)
            except Exception:
                continue
            for e in failure.errors:
                idx = None
                for el in e.location.field_path_elements:
                    if el.field_name == "operations":
                        idx = el.index
                        break
                code = ""
                try:
                    f_ = e.error_code._pb.WhichOneof("error_code")
                    code = f"{f_}.{getattr(e.error_code, f_).name}: " if f_ else ""
                except Exception:
                    pass
                out[idx] = (out.get(idx, "") + f" {code}{e.message}").strip()
        if not out:
            out[None] = pfe.message
        return out

    # ---------- labels ----------
    def labels_for(self, c):
        """'Ponly Cook' + category (+ the image model); ALCOHOL campaigns get 'ALCOHOL' + each
        category inside."""
        model = [config.MODEL_LABELS[c["image_model"]]] if c["image_model"] in \
            config.MODEL_LABELS else []
        if c["pool"] != "ALCOHOL":
            return [config.BRAND_LABEL, c["pool"]] + model
        cats = [r["category"] for r in self.con.execute(
            "SELECT DISTINCT a.category FROM articles a JOIN adgroups g ON a.adgroup_key=g.key "
            "WHERE g.campaign_key=? ORDER BY a.category", (c["key"],))]
        return [config.BRAND_LABEL, "ALCOHOL"] + cats + model

    def add_missing_labels(self):
        """Labels wanted on campaigns of waves already uploaded but not applied yet (e.g. a
        label added later, like the image model). Never touches anything else."""
        con = self.con
        camps = [dict(c) for c in con.execute(
            "SELECT * FROM campaigns WHERE campaign_res IS NOT NULL AND status IN "
            "('created','enabled','held') AND COALESCE(label_attempts,0)<3")
            if db.kv_get(con, f"uploaded_{c['wave']}")]
        todo = []
        for c in camps:
            done = set(json.loads(c["labels_done"] or "[]"))
            todo += [(c, n) for n in self.wanted_labels(c) if n not in done]
        if not todo:
            return 0
        known = self.ensure_labels([n for _, n in todo])
        todo = [(c, n) for c, n in todo if n in known]
        if not todo:
            return 0
        ops = []
        for c, n in todo:
            op = self.client.get_type("CampaignLabelOperation")
            op.create.campaign = c["campaign_res"]
            op.create.label = known[n]
            ops.append(op)
        r = self.client.get_service("CampaignLabelService").mutate_campaign_labels(
            request={"customer_id": self.cid, "operations": ops, "partial_failure": True})
        db.log_ops(con, len(ops), "labels")
        failed = self.partial_failures(r)
        added = 0
        with db.tx(con):
            for i, (c, n) in enumerate(todo):
                row = con.execute("SELECT labels_done, label_attempts FROM campaigns WHERE key=?",
                                  (c["key"],)).fetchone()
                done = json.loads(row["labels_done"] or "[]")
                err = failed.get(i) or failed.get(None)
                ok = not err and i < len(r.results) and r.results[i].resource_name
                if ok or (err and ("RESOURCE_ALREADY_EXISTS" in err or "DUPLICATE" in err)):
                    if n not in done:
                        done.append(n)
                    con.execute("UPDATE campaigns SET labels_done=? WHERE key=?",
                                (json.dumps(done), c["key"]))
                    added += 1
                    db._log_after_commit(con, f"label '{n}' added to campaign {c['name']}")
                else:
                    con.execute("UPDATE campaigns SET label_attempts=?, error=? WHERE key=?",
                                ((row["label_attempts"] or 0) + 1,
                                 f"label '{n}': {err}"[:500], c["key"]))
                    db._log_after_commit(con, f"label '{n}' FAILED on campaign {c['name']}: "
                                              f"{short(err, 300)}")
        return added

    def label_problems(self):
        return db.kv_get(self.con, f"label_problems_{self.cid}", {})

    def wanted_labels(self, c):
        bad = self.label_problems()
        return [n for n in self.labels_for(c) if n not in bad]

    def ensure_labels(self, names):
        """Find (case-insensitive) or create labels; unusable ones are reported and skipped,
        never block uploads (R15)."""
        known = db.kv_get(self.con, f"labels_{self.cid}", {})
        problems = self.label_problems()
        need = sorted({n for n in names if n not in known and n not in problems})
        if not need:
            return known
        existing = {}
        for row in self.search("SELECT label.resource_name, label.name, label.status FROM label",
                               "labels"):
            existing.setdefault(row.label.name.lower(), []).append(
                (row.label.resource_name, row.label.status.name, row.label.name))
        create = []
        for n in need:
            matches = existing.get(n.lower(), [])
            enabled = [m for m in matches if m[1] == "ENABLED"]
            if enabled:
                known[n] = enabled[0][0]
            elif matches:
                problems[n] = f"label exists but is {matches[0][1]} ('{matches[0][2]}'); not applied"
            else:
                create.append(n)
        if create:
            ops = []
            for n in create:
                op = self.client.get_type("LabelOperation")
                op.create.name = n
                ops.append(op)
            r = self.client.get_service("LabelService").mutate_labels(
                request={"customer_id": self.cid, "operations": ops, "partial_failure": True})
            db.log_ops(self.con, len(ops), "labels")
            failed = self.partial_failures(r)
            for i, (n, res) in enumerate(zip(create, r.results)):
                if i in failed or not res.resource_name:
                    problems[n] = f"label could not be created: {failed.get(i) or failed.get(None)}"
                else:
                    known[n] = res.resource_name
                    log(f"label created: '{n}'")
        for n in need:
            if n in problems:
                log(f"label problem (not applied): '{n}': {problems[n]}")
            elif n in known and n not in create:
                log(f"label found in account and reused: '{n}'")
        db.kv_set(self.con, f"labels_{self.cid}", known)
        db.kv_set(self.con, f"label_problems_{self.cid}", problems)
        return known

    # ---------- operation builders ----------
    def iter_units(self, wave, logos, label_res=None):
        """Yield one unit per campaign: (campaign_key, ops, index_entries). Built lazily, so
        image bytes are read only for units actually used (P7). Temp IDs unique per call."""
        con = self.con
        tid = [-1]

        def temp():
            v = tid[0]
            tid[0] -= 1
            return v

        camps = con.execute("SELECT * FROM campaigns WHERE wave=? AND status!='failed' ORDER BY key",
                            (wave,)).fetchall()
        for c in camps:
            ops, idx = [], []
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
            if label_res is not None and wave != 0 and (c["label_attempts"] or 0) < 3:
                done = json.loads(c["labels_done"] or "[]")
                for name in self.wanted_labels(c):
                    if name in done or name not in label_res:
                        continue
                    op = self._op()
                    cl = op.campaign_label_operation.create
                    cl.campaign = camp_res
                    cl.label = label_res[name]
                    ops.append(op); idx.append({"t": "label", "k": c["key"], "l": name})
            ags = con.execute("SELECT * FROM adgroups WHERE campaign_key=? AND "
                              "(COALESCE(attempts,0)<3 OR adgroup_res IS NOT NULL) ORDER BY nn",
                              (c["key"],)).fetchall()
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
                if g["alcohol"] and not g["exclusion_res"] and \
                        (g["excl_attempts"] or 0) < EXCL_TRIES:
                    op = self._op()
                    cr = op.ad_group_criterion_operation.create
                    cr.ad_group = ag_res
                    cr.negative = True
                    cr.age_range.type_ = self.e.AgeRangeTypeEnum.AGE_RANGE_18_24
                    ops.append(op); idx.append({"t": "exclusion", "k": g["key"]})
                arts = con.execute("SELECT * FROM articles WHERE adgroup_key=? AND status='planned'"
                                   " ORDER BY id", (g["key"],)).fetchall()
                for art in arts:
                    imgs, art_ops, art_idx, ok = {}, [], [], True
                    for col, path_col, tag in (("landscape_asset", "landscape_path", "L"),
                                               ("square_asset", "square_path", "S")):
                        res = art[col]
                        if not res:
                            try:
                                with open(art[path_col], "rb") as f:
                                    data = f.read()
                            except (OSError, TypeError) as e:   # missing file never blocks (P5)
                                db.update_article(con, art["id"], status="error",
                                                  error=f"image file missing at upload: {e}"[:300])
                                ok = False
                                break
                            if len(data) > MAX_ASSET_BYTES:
                                db.update_article(con, art["id"], status="error",
                                                  error=f"image too large to upload ({len(data)} bytes)")
                                ok = False
                                break
                            res = self.path("AssetService", "asset_path", temp())
                            op = self._op()
                            s = op.asset_operation.create
                            s.resource_name = res
                            s.name = asset_name(art, tag)
                            s.type_ = self.e.AssetTypeEnum.IMAGE
                            s.image_asset.data = data
                            art_ops.append(op); art_idx.append({"t": "asset" + tag, "k": art["id"]})
                        imgs[tag] = res
                    if not ok:
                        continue
                    art_ops.append(self._ad_op(ag_res, art, imgs, logos))
                    art_idx.append({"t": "ad", "k": art["id"]})
                    ops += art_ops
                    idx += art_idx
            if ops:
                yield c["key"], ops, idx

    def ops_for_wave(self, wave, logos, temp_start=-1):
        """List form (tests / preflight): [(ops, index_entries, 0), ...]."""
        names = []
        if wave != 0:
            for c in self.con.execute("SELECT * FROM campaigns WHERE wave=? AND status!='failed'",
                                      (wave,)):
                names += self.wanted_labels(c)
        label_res = self.ensure_labels(names) if names else {}
        return [(ops, idx, 0) for _, ops, idx in self.iter_units(wave, logos, label_res)]

    def has_work(self, wave):
        """Cheap SQL check: is anything of this wave still missing in Google Ads? (P7)"""
        con = self.con
        if con.execute("SELECT 1 FROM campaigns WHERE wave=? AND status!='failed' AND "
                       "(budget_res IS NULL OR campaign_res IS NULL OR geo_res IS NULL OR "
                       "lang_res IS NULL)", (wave,)).fetchone():
            return True
        if con.execute("SELECT 1 FROM adgroups g JOIN campaigns c ON g.campaign_key=c.key WHERE "
                       "c.wave=? AND c.status!='failed' AND ((COALESCE(g.attempts,0)<3 AND "
                       "g.adgroup_res IS NULL) OR (g.alcohol=1 AND g.adgroup_res IS NOT NULL AND "
                       "g.exclusion_res IS NULL AND COALESCE(g.excl_attempts,0)<?))",
                       (wave, EXCL_TRIES)).fetchone():
            return True
        if con.execute("SELECT 1 FROM articles a JOIN adgroups g ON a.adgroup_key=g.key JOIN "
                       "campaigns c ON g.campaign_key=c.key WHERE c.wave=? AND c.status!='failed' "
                       "AND COALESCE(g.attempts,0)<3 AND a.status='planned'", (wave,)).fetchone():
            return True
        for c in con.execute("SELECT * FROM campaigns WHERE wave=? AND status!='failed' AND "
                             "COALESCE(label_attempts,0)<3", (wave,)).fetchall():
            done = set(json.loads(c["labels_done"] or "[]"))
            if set(self.wanted_labels(c)) - done:
                return True
        return False

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
    def apply_result(self, entry, resource_name, error, counted=None):
        """counted: set of entities already charged an attempt in this job (R18)."""
        con, k, t = self.con, entry["k"], entry["t"]
        counted = counted if counted is not None else set()
        if error:
            msg = f"{t}: {error}"[:500]
            if t in ("ad", "assetL", "assetS"):
                a = con.execute("SELECT ad_attempts, status FROM articles WHERE id=?",
                                (k,)).fetchone()
                if not a or a["status"] == "error":   # already explained by campaign/ad group
                    return
                if ("art", k) in counted:
                    db.update_article(con, k, error=f"upload {msg}")
                    return
                counted.add(("art", k))
                n = (a["ad_attempts"] or 0) + 1
                fields = {"ad_attempts": n, "error": f"upload {msg}"}
                if n >= 3:
                    fields.update(status="error")
                db.update_article(con, k, **fields)
            elif t in ("budget", "campaign", "geo", "lang"):
                c = con.execute("SELECT * FROM campaigns WHERE key=?", (k,)).fetchone()
                n = c["attempts"] or 0
                if ("camp", k) not in counted:
                    counted.add(("camp", k))
                    n += 1
                con.execute("UPDATE campaigns SET error=?, attempts=? WHERE key=?", (msg, n, k))
                if n >= 3:
                    self.fail_campaign(k, f"campaign not created: {msg}")
            elif t == "label":
                c = con.execute("SELECT label_attempts FROM campaigns WHERE key=?", (k,)).fetchone()
                n = c["label_attempts"] or 0
                if ("label", k) not in counted:
                    counted.add(("label", k))
                    n += 1
                con.execute("UPDATE campaigns SET error=?, label_attempts=? WHERE key=?",
                            (f"label '{entry.get('l')}': {error}"[:500], n, k))
            elif t == "exclusion":
                g = con.execute("SELECT * FROM adgroups WHERE key=?", (k,)).fetchone()
                n = g["excl_attempts"] or 0
                if ("excl", k) not in counted:
                    counted.add(("excl", k))
                    n += 1
                con.execute("UPDATE adgroups SET error=?, excl_attempts=? WHERE key=?", (msg, n, k))
                if n >= EXCL_TRIES:
                    con.execute("UPDATE campaigns SET status='held', error=? WHERE key=? AND "
                                "status IN ('planned','created')",
                                (f"kept paused: 18-24 exclusion could not be added ({msg})"[:500],
                                 g["campaign_key"]))
                    db._log_after_commit(con, f"campaign HELD (kept paused): 18-24 exclusion "
                                              f"failed {n} times on ad group {k}: {short(msg, 200)}")
            elif t == "adgroup":
                g = con.execute("SELECT * FROM adgroups WHERE key=?", (k,)).fetchone()
                n = g["attempts"] or 0
                if ("ag", k) not in counted:
                    counted.add(("ag", k))
                    n += 1
                con.execute("UPDATE adgroups SET error=?, attempts=? WHERE key=?", (msg, n, k))
                if n >= 3:
                    for a in con.execute("SELECT id FROM articles WHERE adgroup_key=? AND "
                                         "status='planned'", (k,)).fetchall():
                        db.update_article(con, a["id"], status="error",
                                          error=f"ad group not created: {msg}")
            return
        if not resource_name:
            return      # never store an empty resource name (R15)
        col = {"budget": ("campaigns", "budget_res"), "campaign": ("campaigns", "campaign_res"),
               "geo": ("campaigns", "geo_res"), "lang": ("campaigns", "lang_res"),
               "adgroup": ("adgroups", "adgroup_res"),
               "exclusion": ("adgroups", "exclusion_res")}.get(t)
        if col:
            con.execute(f"UPDATE {col[0]} SET {col[1]}=? WHERE key=?", (resource_name, k))
            if t == "campaign":
                con.execute("UPDATE campaigns SET status='created' WHERE key=? AND "
                            "status='planned'", (k,))
        elif t == "label":
            c = con.execute("SELECT labels_done FROM campaigns WHERE key=?", (k,)).fetchone()
            done = json.loads(c["labels_done"] or "[]")
            if entry["l"] not in done:
                done.append(entry["l"])
            con.execute("UPDATE campaigns SET labels_done=? WHERE key=?", (json.dumps(done), k))
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

    def fail_campaign(self, key, msg):
        con = self.con
        con.execute("UPDATE campaigns SET status='failed', error=? WHERE key=?", (msg[:500], key))
        for a in con.execute("SELECT a.id FROM articles a JOIN adgroups g ON a.adgroup_key=g.key "
                             "WHERE g.campaign_key=? AND a.status='planned'", (key,)).fetchall():
            db.update_article(con, a["id"], status="error", error=msg[:300])

    @staticmethod
    def _resource_from_response(resp):
        """MutateOperationResponse -> resource name of whichever field is set."""
        field = resp._pb.WhichOneof("response")
        return getattr(resp, field).resource_name if field else ""

    # ---------- collisions (R14/R17) ----------
    def check_collisions(self, wave):
        """Block campaigns whose name (or budget name) already exists in the account as an
        entity that is not ours. Run after reconcile, before each job."""
        con = self.con
        rows = con.execute("SELECT * FROM campaigns WHERE wave=? AND status!='failed' AND "
                           "campaign_res IS NULL", (wave,)).fetchall()
        if not rows:
            return 0
        by_name = {r["name"]: r for r in rows}
        found_c, found_b = {}, {}
        for part in _chunks(by_name):
            q = ",".join(_q(n) for n in part)
            for x in self.search(f"SELECT campaign.resource_name, campaign.name FROM campaign "
                                 f"WHERE campaign.name IN ({q}) AND campaign.status != 'REMOVED'",
                                 "collision"):
                found_c[x.campaign.name] = x.campaign.resource_name
            need_b = [n for n in part if not by_name[n]["budget_res"]]
            if need_b:
                bq = ",".join(_q(f"{n} | budget") for n in need_b)
                for x in self.search(f"SELECT campaign_budget.resource_name, campaign_budget.name "
                                     f"FROM campaign_budget WHERE campaign_budget.name IN ({bq}) "
                                     f"AND campaign_budget.status != 'REMOVED'", "collision"):
                    found_b[x.campaign_budget.name] = x.campaign_budget.resource_name
        blocked = 0
        with db.tx(con):
            for name, r in by_name.items():
                bres = found_b.get(f"{name} | budget")
                if name in found_c and found_c[name] != r["campaign_res"]:
                    self.fail_campaign(r["key"], "a campaign with this name already exists in "
                                                 "the account (not created by this run)")
                    blocked += 1
                    log(f"campaign BLOCKED (name already in account): {name}")
                elif bres and not r["budget_res"]:
                    self.fail_campaign(r["key"], "a budget named '<campaign> | budget' already "
                                                 "exists in the account (not created by this run)")
                    blocked += 1
                    log(f"campaign BLOCKED (budget name already in account): {name}")
        return blocked

    # ---------- batch job (resumable) ----------
    def run_upload(self, wave, logos, quota_left):
        """Upload whatever is missing for this wave, sized to remaining quota.
        Returns ops submitted; -1 while a job is open; 0 if nothing to do / no quota."""
        con = self.con
        for j in db.open_jobs(con, "gads"):
            self._resume(j)
            return -1
        due = db.kv_get(con, f"reconcile_due_{wave}")
        if due:
            if time.time() < due:
                return -1                     # waiting before reconcile: build nothing yet
            self.reconcile(wave)
            con.execute("DELETE FROM kv WHERE k=?", (f"reconcile_due_{wave}",))
            log(f"wave {wave}: account search done; existing items recorded, rebuild continues")
            return -1
        if not self.has_work(wave) or quota_left < 50:
            return 0
        self.check_collisions(wave)
        names = []
        for c in con.execute("SELECT * FROM campaigns WHERE wave=? AND status!='failed'", (wave,)):
            names += self.wanted_labels(c)
        label_res = self.ensure_labels(names)
        chosen_ops, index, total, nbytes_job = [], [], 0, 0
        for key, ops, idx in self.iter_units(wave, logos, label_res):
            size = sum(op._pb.ByteSize() for op in ops)
            if total + len(ops) + 20 > quota_left:
                break
            if chosen_ops and nbytes_job + size > JOB_MAX_BYTES:
                break                     # memory cap: the rest goes in the next job
            chosen_ops += ops
            index += idx
            total += len(ops)
            nbytes_job += size
        if not chosen_ops:
            return 0
        svc = self.client.get_service("BatchJobService")
        jid = db.new_job(con, "gads", wave, payload={"index": index}, state="creating")
        bop = self.client.get_type("BatchJobOperation")
        bop.create = self.client.get_type("BatchJob")
        resp = svc.mutate_batch_job(customer_id=self.cid, operation=bop)
        db.log_ops(con, 1, "batch_create")
        res = resp.result.resource_name
        db.set_job(con, jid, external_id=res, state="adding")   # saved before adding/running
        token, i = None, 0
        while i < len(chosen_ops):
            part, nbytes = [], 0
            while i < len(chosen_ops) and len(part) < config.OPS_PER_ADD_REQUEST:
                size = chosen_ops[i]._pb.ByteSize()
                if part and nbytes + size > config.BYTES_PER_ADD_REQUEST:
                    break
                part.append(chosen_ops[i]); nbytes += size; i += 1
            kw = {"resource_name": res, "mutate_operations": part}
            if token:
                kw["sequence_token"] = token
            r = svc.add_batch_job_operations(**kw)
            token = r.next_sequence_token
            db.log_ops(con, len(part), "batch_ops")
            self.progress()
        db.set_job(con, jid, state="run_requested")             # before run (R12)
        svc.run_batch_job(resource_name=res)
        db.log_ops(con, 1, "batch_run")
        db.set_job(con, jid, state="running")
        kinds = {}
        for e in index:
            kinds[e["t"]] = kinds.get(e["t"], 0) + 1
        log(f"Google Ads job {jid} ({res.split('/')[-1]}) started for wave {wave}: "
            f"{len(chosen_ops)} operations {kinds}")
        return len(chosen_ops)

    def _fail_count(self, job):
        k = f"resume_fail_{job['id']}"
        n = db.kv_get(self.con, k, 0) + 1
        db.kv_set(self.con, k, n)
        return n

    def _resume(self, job):
        con = self.con
        res = job["external_id"]
        if not res:            # crashed before the job existed server-side
            db.set_job(con, job["id"], state="abandoned")
            return
        rows = self.search(f"SELECT batch_job.status FROM batch_job "
                           f"WHERE batch_job.resource_name = '{res}'", "batch_poll")
        if not rows:
            n = self._fail_count(job)
            log(f"Google Ads job {job['id']}: not found ({n}/{LOST_TRIES})")
            if n >= LOST_TRIES:
                self._lost(job, "batch job not found")
            return
        status = rows[0].batch_job.status.name
        age = time.time() - (job["created"] or time.time())
        if db.kv_get(con, f"job_status_{job['id']}") != status:
            db.kv_set(con, f"job_status_{job['id']}", status)
            log(f"Google Ads job {job['id']}: {status}")
        if status == "PENDING":
            if job["state"] in ("adding", "creating"):
                # never asked to run -> nothing was created; Google deletes it after 7 days
                db.set_job(con, job["id"], state="abandoned")
            elif job["state"] == "run_requested" and age <= RUNNING_REPORT_S and \
                    time.time() - db.kv_get(con, f"rerun_{job['id']}", 0) > 600:
                # the run request may never have reached Google: ask again (a job that is
                # no longer PENDING is refused, so this can't run anything twice)
                db.kv_set(con, f"rerun_{job['id']}", time.time())
                try:
                    self.client.get_service("BatchJobService").run_batch_job(resource_name=res)
                    db.log_ops(con, 1, "batch_run")
                    db.set_job(con, job["id"], state="running")
                    log(f"Google Ads job {job['id']}: run requested again")
                except Exception as e:
                    log(f"Google Ads job {job['id']}: run again refused: {short(e, 200)}")
            elif age > RUNNING_REPORT_S:
                try:
                    op = self.client.get_type("BatchJobOperation")
                    op.remove = res
                    self.client.get_service("BatchJobService").mutate_batch_job(
                        customer_id=self.cid, operation=op)
                    db.log_ops(con, 1, "batch_remove")
                    self._lost(job, "pending for over 24 h after run; removed")
                except Exception as e:
                    if "CAN_ONLY_REMOVE_PENDING_JOB" not in str(e):
                        raise
                    # it has started running: keep waiting
            return
        if status != "DONE":
            if age > RUNNING_REPORT_S and not db.kv_get(con, f"slow_job_{job['id']}"):
                db.kv_set(con, f"slow_job_{job['id']}", True)
                raise RuntimeError(f"batch job {res} still {status} after 24 h (waiting)")
            return
        index = json.loads(job["payload"])["index"]
        svc = self.client.get_service("BatchJobService")
        try:   # read every page first; apply only when complete (R6/B5)
            results = []
            for r in svc.list_batch_job_results(request={"resource_name": res,
                                                         "page_size": 1000}):
                code = r.status.code if r.status else 0
                results.append((r.operation_index, code,
                                self.decode_status(r.status) if code else None,
                                None if code else self._resource_from_response(
                                    r.mutate_operation_response)))
        except Exception as e:
            db.log_ops(con, 1, "batch_results")
            msg = f"{type(e).__name__}: {e}"[:400]
            db.kv_set(con, f"resume_error_{job['id']}", msg)
            log(f"Google Ads job {job['id']}: results could not be listed: {msg}")
            if self._fail_count(job) >= LOST_TRIES:
                self._lost(job, "results could not be listed")
            return
        db.log_ops(con, max(1, len(index) // 1000 + 1), "batch_results")
        counted = set()
        with db.tx(con):
            for op_index, code, err, name in results:
                if 0 <= op_index < len(index):
                    self.apply_result(index[op_index], name, err if code else None, counted)
            db.set_job(con, job["id"], state="done")
        ok, bad = {}, {}
        for op_index, code, err, name in results:
            t = index[op_index]["t"] if 0 <= op_index < len(index) else "?"
            (bad if code else ok)[t] = (bad if code else ok).get(t, 0) + 1
        log(f"Google Ads job {job['id']} done: {len(results)} results for {len(index)} "
            f"operations; created {ok}; errors {bad or 'none'}")
        for op_index, code, err, name in [r for r in results if r[1]][:50]:
            e = index[op_index] if 0 <= op_index < len(index) else {}
            log(f"Google Ads error: {e.get('t')} {e.get('k')}: {short(err, 300)}")

    def _lost(self, job, why):
        """Reconcile only after a delay: right after a job, Google's search may not show
        everything yet, and adopting too early would let the rebuild create duplicates."""
        db.set_job(self.con, job["id"], state="lost")
        db.kv_set(self.con, f"lost_job_{job['id']}", why)
        log(f"Google Ads job {job['id']} LOST ({why}); account search in "
            f"{RECONCILE_DELAY_S // 60} min before anything is rebuilt")
        db.kv_set(self.con, f"reconcile_due_{job['wave']}", time.time() + RECONCILE_DELAY_S)

    # ---------- reconcile by search (R13) ----------
    def reconcile(self, wave):
        """Adopt anything of this wave that exists in Google Ads but is not recorded here,
        so a rebuild never creates duplicates. Searches first, then one transaction."""
        con = self.con
        camps = [dict(r) for r in con.execute("SELECT * FROM campaigns WHERE wave=? AND "
                                              "status!='failed'", (wave,))]
        if not camps:
            return
        found = {"budget": {}, "campaign": {}, "crit": {}, "label": {}, "ag": {}, "excl": {},
                 "asset": {}, "ad": {}}
        names = [c["name"] for c in camps]
        for part in _chunks(names):
            q = ",".join(_q(f"{n} | budget") for n in part)
            for x in self.search("SELECT campaign_budget.resource_name, campaign_budget.name FROM "
                                 f"campaign_budget WHERE campaign_budget.name IN ({q}) AND "
                                 "campaign_budget.status != 'REMOVED'", "reconcile"):
                found["budget"][x.campaign_budget.name] = x.campaign_budget.resource_name
            q = ",".join(_q(n) for n in part)
            for x in self.search("SELECT campaign.resource_name, campaign.name FROM campaign WHERE "
                                 f"campaign.name IN ({q}) AND campaign.status != 'REMOVED'",
                                 "reconcile"):
                found["campaign"][x.campaign.name] = x.campaign.resource_name
        camp_res = [found["campaign"].get(c["name"]) or c["campaign_res"] for c in camps]
        camp_res = [r for r in camp_res if r]
        for part in _chunks(camp_res):
            q = ",".join(_q(r) for r in part)
            for x in self.search("SELECT campaign_criterion.resource_name, campaign_criterion.type, "
                                 "campaign.resource_name FROM campaign_criterion WHERE "
                                 f"campaign.resource_name IN ({q}) AND campaign_criterion.type IN "
                                 "('LOCATION','LANGUAGE') AND campaign_criterion.negative = FALSE",
                                 "reconcile"):
                found["crit"][(x.campaign.resource_name, x.campaign_criterion.type_.name)] = \
                    x.campaign_criterion.resource_name
            for x in self.search("SELECT campaign_label.resource_name, campaign.resource_name, "
                                 f"label.name FROM campaign_label WHERE campaign.resource_name IN ({q})",
                                 "reconcile"):
                found["label"].setdefault(x.campaign.resource_name, set()).add(x.label.name)
            for x in self.search("SELECT ad_group.resource_name, ad_group.name, campaign.resource_name "
                                 f"FROM ad_group WHERE campaign.resource_name IN ({q}) AND "
                                 "ad_group.status != 'REMOVED'", "reconcile"):
                found["ag"][(x.campaign.resource_name, x.ad_group.name)] = x.ad_group.resource_name
        ag_res = list(found["ag"].values())
        for part in _chunks(ag_res):
            q = ",".join(_q(r) for r in part)
            for x in self.search("SELECT ad_group_criterion.resource_name, ad_group.resource_name, "
                                 "ad_group_criterion.age_range.type FROM "
                                 f"ad_group_criterion WHERE ad_group.resource_name IN ({q}) AND "
                                 "ad_group_criterion.type = 'AGE_RANGE' AND "
                                 "ad_group_criterion.negative = TRUE", "reconcile"):
                if x.ad_group_criterion.age_range.type_.name == "AGE_RANGE_18_24":
                    found["excl"][x.ad_group.resource_name] = x.ad_group_criterion.resource_name
            for x in self.search("SELECT ad_group_ad.resource_name, ad_group.resource_name, "
                                 "ad_group_ad.ad.final_urls FROM ad_group_ad WHERE "
                                 f"ad_group.resource_name IN ({q}) AND ad_group_ad.status != 'REMOVED'",
                                 "reconcile"):
                for u in x.ad_group_ad.ad.final_urls:
                    found["ad"][(x.ad_group.resource_name, u)] = x.ad_group_ad.resource_name
        arts = [dict(r) for r in con.execute(
            "SELECT a.* FROM articles a JOIN adgroups g ON a.adgroup_key=g.key JOIN campaigns c ON "
            "g.campaign_key=c.key WHERE c.wave=? AND a.status='planned'", (wave,))]
        asset_names = [asset_name(a, t) for a in arts for t in ("L", "S")]
        for part in _chunks(asset_names):
            q = ",".join(_q(n) for n in part)
            for x in self.search(f"SELECT asset.resource_name, asset.name FROM asset WHERE "
                                 f"asset.name IN ({q})", "reconcile"):
                found["asset"][x.asset.name] = x.asset.resource_name
        db.kv_set(con, f"reconciled_{wave}", {k: len(v) for k, v in found.items()})
        log(f"wave {wave}: account search found {db.kv_get(con, f'reconciled_{wave}')}")
        with db.tx(con):
            for c in camps:
                b = found["budget"].get(f"{c['name']} | budget")
                if b and not c["budget_res"]:
                    con.execute("UPDATE campaigns SET budget_res=? WHERE key=?", (b, c["key"]))
                cr = found["campaign"].get(c["name"]) or c["campaign_res"]
                if not cr:
                    continue
                con.execute("UPDATE campaigns SET campaign_res=?, status=CASE WHEN status='planned' "
                            "THEN 'created' ELSE status END WHERE key=?", (cr, c["key"]))
                g_ = found["crit"].get((cr, "LOCATION"))
                l_ = found["crit"].get((cr, "LANGUAGE"))
                if g_:
                    con.execute("UPDATE campaigns SET geo_res=? WHERE key=? AND geo_res IS NULL",
                                (g_, c["key"]))
                if l_:
                    con.execute("UPDATE campaigns SET lang_res=? WHERE key=? AND lang_res IS NULL",
                                (l_, c["key"]))
                labels = found["label"].get(cr, set())
                done = sorted(set(json.loads(c["labels_done"] or "[]")) | labels)
                con.execute("UPDATE campaigns SET labels_done=? WHERE key=?",
                            (json.dumps(done), c["key"]))
                for g in con.execute("SELECT * FROM adgroups WHERE campaign_key=?",
                                     (c["key"],)).fetchall():
                    gr = found["ag"].get((cr, g["name"])) or g["adgroup_res"]
                    if not gr:
                        continue
                    con.execute("UPDATE adgroups SET adgroup_res=? WHERE key=?", (gr, g["key"]))
                    if found["excl"].get(gr):
                        con.execute("UPDATE adgroups SET exclusion_res=? WHERE key=? AND "
                                    "exclusion_res IS NULL", (found["excl"][gr], g["key"]))
                    for a in arts:
                        if a["adgroup_key"] != g["key"]:
                            continue
                        for tag, col in (("L", "landscape_asset"), ("S", "square_asset")):
                            r = found["asset"].get(asset_name(a, tag))
                            if r and not a[col]:
                                db.update_article(con, a["id"], **{col: r})
                        ad = found["ad"].get((gr, a["final_url"]))
                        if ad:
                            db.update_article(con, a["id"], ad_resource=ad,
                                              ad_id=ad.split("~")[-1], status="uploaded",
                                              error=None)

    # ---------- validate-only (Phase 1 test: nothing is created) ----------
    def validate_wave(self, wave):
        """Validate-only: logos are sent as temporary assets inside each request, so
        nothing at all is created in the account (labels are left out)."""
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
        for _, ops, idx in self.iter_units(wave, logos, None):
            try:
                svc.mutate(request={"customer_id": self.cid, "validate_only": True,
                                    "mutate_operations": logo_ops + ops})
            except Exception as e:
                errors.append(str(e)[:2000])
            db.log_ops(self.con, len(ops) + 2, "validate")
        return errors

    # ---------- logos ----------
    def upload_logos(self):
        logos = db.kv_get(self.con, f"logos_{self.cid}")
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
        log(f"logos uploaded: {logos}")
        db.kv_set(self.con, f"logos_{self.cid}", logos)
        return logos

    # ---------- enable (R15: partial failure) ----------
    def pre_enable_check(self, rows):
        """Before anything is enabled: every alcohol ad group must have its 18-24 exclusion
        confirmed in Google Ads (not just recorded here); a campaign must have ads; its
        budget must be 300 x its uploaded ads. Returns {key: reason} for campaigns to hold."""
        con = self.con
        hold = {}
        keys = [r["key"] for r in rows]
        if not keys:
            return hold, {}
        q = ",".join("?" * len(keys))
        ads_n = {r["campaign_key"]: r["n"] for r in con.execute(
            "SELECT g.campaign_key, COUNT(a.id) n FROM adgroups g JOIN articles a ON "
            f"a.adgroup_key=g.key WHERE g.campaign_key IN ({q}) AND a.status='uploaded' "
            "GROUP BY g.campaign_key", keys)}
        alc = [dict(g) for g in con.execute(
            f"SELECT * FROM adgroups WHERE campaign_key IN ({q}) AND alcohol=1", keys)]
        confirmed = set()
        res = [g["adgroup_res"] for g in alc if g["adgroup_res"]]
        for part in _chunks(res):
            qq = ",".join(_q(r) for r in part)
            for x in self.search("SELECT ad_group.resource_name, ad_group_criterion.age_range.type "
                                 f"FROM ad_group_criterion WHERE ad_group.resource_name IN ({qq}) "
                                 "AND ad_group_criterion.type = 'AGE_RANGE' AND "
                                 "ad_group_criterion.negative = TRUE AND "
                                 "ad_group_criterion.status != 'REMOVED'", "pre_enable"):
                if x.ad_group_criterion.age_range.type_.name == "AGE_RANGE_18_24":
                    confirmed.add(x.ad_group.resource_name)
        for g in alc:
            if g["adgroup_res"] and g["adgroup_res"] not in confirmed:
                hold[g["campaign_key"]] = (f"18-24 exclusion not found in Google Ads on ad "
                                           f"group {g['name']}")
        # every ad group Google has in an alcohol campaign, even one missing from the database
        alc_camps = {r["campaign_res"]: r["key"] for r in rows if r["pool"] == "ALCOHOL"}
        known_alc = {g["adgroup_res"] for g in alc}
        extra = {}
        for part in _chunks(list(alc_camps)):
            qq = ",".join(_q(r) for r in part)
            for x in self.search("SELECT campaign.resource_name, ad_group.resource_name FROM "
                                 f"ad_group WHERE campaign.resource_name IN ({qq}) AND "
                                 "ad_group.status != 'REMOVED'", "pre_enable"):
                if x.ad_group.resource_name not in known_alc:
                    extra[x.ad_group.resource_name] = alc_camps[x.campaign.resource_name]
        for part in _chunks(list(extra)):
            qq = ",".join(_q(r) for r in part)
            for x in self.search("SELECT ad_group.resource_name, ad_group_criterion.age_range.type "
                                 f"FROM ad_group_criterion WHERE ad_group.resource_name IN ({qq}) "
                                 "AND ad_group_criterion.type = 'AGE_RANGE' AND "
                                 "ad_group_criterion.negative = TRUE AND "
                                 "ad_group_criterion.status != 'REMOVED'", "pre_enable"):
                if x.ad_group_criterion.age_range.type_.name == "AGE_RANGE_18_24":
                    extra.pop(x.ad_group.resource_name, None)
        for ag, key in extra.items():
            hold.setdefault(key, f"ad group {ag.split('/')[-1]} in Google Ads has no 18-24 "
                                 f"exclusion (not in the database)")
        for r in rows:
            if not ads_n.get(r["key"]):
                hold.setdefault(r["key"], "no ads uploaded in this campaign")
        by_name = {}
        names = [r["name"] for r in rows]
        for part in _chunks(names):
            qq = ",".join(_q(n) for n in part)
            for x in self.search("SELECT campaign.resource_name, campaign.name FROM campaign WHERE "
                                 f"campaign.name IN ({qq}) AND campaign.status != 'REMOVED'",
                                 "pre_enable"):
                by_name.setdefault(x.campaign.name, set()).add(x.campaign.resource_name)
        for r in rows:
            found = by_name.get(r["name"], set())
            if len(found) > 1 or (found and r["campaign_res"] not in found):
                hold.setdefault(r["key"], f"campaign name found {len(found)} times in the "
                                          f"account (possible duplicate)")
        return hold, ads_n

    def resize_budgets(self, rows, ads_n):
        """Budget = 300 x uploaded ads (planned articles may have failed). Returns
        {key: error} for budgets that could not be changed."""
        from google.api_core import protobuf_helpers
        change = [r for r in rows if r["budget_res"] and ads_n.get(r["key"]) and
                  r["n"] != ads_n[r["key"]]]
        if not change:
            return {}
        ops = []
        for r in change:
            op = self.client.get_type("CampaignBudgetOperation")
            op.update.resource_name = r["budget_res"]
            op.update.amount_micros = config.BUDGET_PER_ARTICLE_INR * ads_n[r["key"]] * 1_000_000
            self.client.copy_from(op.update_mask, protobuf_helpers.field_mask(None, op.update._pb))
            ops.append(op)
        resp = self.client.get_service("CampaignBudgetService").mutate_campaign_budgets(
            request={"customer_id": self.cid, "operations": ops, "partial_failure": True})
        db.log_ops(self.con, len(ops), "budget")
        failed = self.partial_failures(resp)
        errs = {}
        for i, r in enumerate(change):
            err = failed.get(i) or failed.get(None)
            if err:
                errs[r["key"]] = f"budget not resized: {err}"
                log(f"budget resize FAILED: campaign {r['name']}: {short(err, 200)}")
            else:
                self.con.execute("UPDATE campaigns SET n=? WHERE key=?", (ads_n[r["key"]], r["key"]))
                log(f"budget resized: campaign {r['name']}: {r['n']} -> {ads_n[r['key']]} ads "
                    f"(Rs {config.BUDGET_PER_ARTICLE_INR * ads_n[r['key']]}/day)")
        return errs

    def enable_wave(self, wave):
        """Enable the wave's paused campaigns that pass pre_enable_check. Campaigns that fail
        the check or the enable stay paused with the reason; enable failures are retried up to
        ENABLE_TRIES times on later loops."""
        con = self.con
        rows = [dict(r) for r in con.execute(
            "SELECT * FROM campaigns WHERE wave=? AND campaign_res IS NOT NULL AND "
            "status='created' AND COALESCE(enable_attempts,0)<?", (wave, ENABLE_TRIES))]
        if not rows:
            return 0
        hold, ads_n = self.pre_enable_check(rows)
        for r in rows:
            if r["key"] in hold:
                msg = f"not enabled: {hold[r['key']]}"[:500]
                if r["error"] != msg:                  # log once per new reason
                    log(f"enable HELD (kept paused): campaign {r['name']}: {hold[r['key']]}")
                con.execute("UPDATE campaigns SET error=? WHERE key=?", (msg, r["key"]))
        rows = [r for r in rows if r["key"] not in hold]
        berr = self.resize_budgets(rows, ads_n)
        for k, e in berr.items():
            con.execute("UPDATE campaigns SET error=?, enable_attempts=COALESCE(enable_attempts,0)+1"
                        " WHERE key=?", (e[:500], k))
        rows = [r for r in rows if r["key"] not in berr]
        svc = self.client.get_service("CampaignService")
        from google.api_core import protobuf_helpers
        enabled = 0
        for i in range(0, len(rows), 1000):
            chunk = rows[i:i + 1000]
            ops = []
            for r in chunk:
                op = self.client.get_type("CampaignOperation")
                op.update.resource_name = r["campaign_res"]
                op.update.status = self.e.CampaignStatusEnum.ENABLED
                self.client.copy_from(op.update_mask, protobuf_helpers.field_mask(None, op.update._pb))
                ops.append(op)
            resp = svc.mutate_campaigns(request={"customer_id": self.cid, "operations": ops,
                                                 "partial_failure": True})
            db.log_ops(con, len(ops), "enable")
            failed = self.partial_failures(resp)
            with db.tx(con):
                for j, r in enumerate(chunk):
                    ok = (j not in failed and None not in failed and j < len(resp.results)
                          and resp.results[j].resource_name)
                    if not ok:
                        err = failed.get(j) or failed.get(None) or "no result"
                        con.execute("UPDATE campaigns SET error=?, enable_attempts="
                                    "COALESCE(enable_attempts,0)+1 WHERE key=?",
                                    (f"enable failed: {err}"[:500], r["key"]))
                        db._log_after_commit(con, f"enable FAILED: campaign {r['name']}: "
                                                  f"{short(err, 300)}")
                    else:
                        enabled += 1
                        con.execute("UPDATE campaigns SET status='enabled', enabled_at=?, "
                                    "error=NULL WHERE key=?", (time.time(), r["key"]))
                        db._log_after_commit(con, f"enabled: campaign {r['name']} "
                                                  f"({r['campaign_res'].split('/')[-1]})")
        return enabled

    # ---------- remove campaigns (pilot re-run) ----------
    def remove_campaigns(self, camps):
        """Remove campaigns, then their budgets. Returns ({key: error or None}, budget_errors).
        Removed campaigns cannot be restored in Google Ads."""
        if not camps:
            return {}, {}
        ops = []
        for c in camps:
            op = self.client.get_type("CampaignOperation")
            op.remove = c["campaign_res"]
            ops.append(op)
        resp = self.client.get_service("CampaignService").mutate_campaigns(request={
            "customer_id": self.cid, "operations": ops, "partial_failure": True})
        db.log_ops(self.con, len(ops), "remove")
        failed = self.partial_failures(resp)
        for i, c in enumerate(camps):
            log(f"remove campaign {c['name']}: "
                f"{'FAILED ' + short(failed.get(i) or failed.get(None)) if (i in failed or None in failed) else 'removed'}")
        out = {}
        for i, c in enumerate(camps):
            ok = (i not in failed and None not in failed and i < len(resp.results)
                  and resp.results[i].resource_name)
            out[c["key"]] = None if ok else (failed.get(i) or failed.get(None) or "no result")
        bud = [c for c in camps if out[c["key"]] is None and c["budget_res"]]
        berr = {}
        if bud:
            bops = []
            for c in bud:
                op = self.client.get_type("CampaignBudgetOperation")
                op.remove = c["budget_res"]
                bops.append(op)
            r = self.client.get_service("CampaignBudgetService").mutate_campaign_budgets(
                request={"customer_id": self.cid, "operations": bops, "partial_failure": True})
            db.log_ops(self.con, len(bops), "remove")
            bf = self.partial_failures(r)
            berr = {bud[i]["key"]: m for i, m in bf.items() if i is not None}
            if None in bf:
                berr = {c["key"]: bf[None] for c in bud}
        return out, berr

    # ---------- time zone (P19) ----------
    def account_today(self):
        tz = db.kv_get(self.con, f"tz_{self.cid}")
        if not tz:
            rows = self.search("SELECT customer.time_zone FROM customer LIMIT 1", "tz")
            tz = rows[0].customer.time_zone if rows else "UTC"
            db.kv_set(self.con, f"tz_{self.cid}", tz)
        try:
            from zoneinfo import ZoneInfo
            return datetime.datetime.now(ZoneInfo(tz)).date().isoformat()
        except Exception:
            return datetime.datetime.now(datetime.timezone.utc).date().isoformat()

    # ---------- daily sync ----------
    def sync(self, since_date, metrics_only=False):
        """metrics_only: just impressions, clicks and cost (cheap; run hourly while spending)."""
        con = self.con
        ids = [r["campaign_res"].split("/")[-1] for r in con.execute(
            "SELECT campaign_res FROM campaigns WHERE campaign_res IS NOT NULL AND "
            "status!='removed'")]
        by_ad = {r["ad_resource"]: r["id"] for r in con.execute(
            "SELECT id, ad_resource FROM articles WHERE ad_resource IS NOT NULL")}
        today = self.account_today()
        enh_on = 0
        updates = []
        totals = {}
        asset_notes = {}
        for i in range(0, len(ids), 400):
            id_list = ",".join(ids[i:i + 400])
            q = ("SELECT ad_group_ad.resource_name, ad_group_ad.policy_summary.approval_status, "
                 "ad_group_ad.policy_summary.review_status, "
                 "ad_group_ad.policy_summary.policy_topic_entries, "
                 "ad_group_ad.ad.responsive_display_ad.control_spec.enable_asset_enhancements, "
                 "ad_group_ad.ad.responsive_display_ad.control_spec.enable_autogen_video "
                 f"FROM ad_group_ad WHERE campaign.id IN ({id_list})")
            for row in ([] if metrics_only else self.search(q, "sync")):
                aga = row.ad_group_ad
                aid = by_ad.get(aga.resource_name)
                if not aid:
                    continue
                topics = "; ".join(t.topic for t in aga.policy_summary.policy_topic_entries)
                cs = aga.ad.responsive_display_ad.control_spec
                if cs.enable_asset_enhancements or cs.enable_autogen_video:
                    enh_on += 1
                updates.append((aid, {"approval": aga.policy_summary.approval_status.name,
                                      "review_status": aga.policy_summary.review_status.name,
                                      "policy_note": topics or None}))
            if since_date:
                q2 = ("SELECT ad_group_ad.resource_name, metrics.impressions, metrics.clicks, "
                      "metrics.cost_micros "
                      f"FROM ad_group_ad WHERE campaign.id IN ({id_list}) "
                      f"AND segments.date BETWEEN '{since_date}' AND '{today}'")
                for row in self.search(q2, "sync"):   # one row per ad per day: add them up
                    aid = by_ad.get(row.ad_group_ad.resource_name)
                    if aid:
                        t = totals.setdefault(aid, [0, 0, 0])
                        t[0] += row.metrics.impressions
                        t[1] += row.metrics.clicks
                        t[2] += row.metrics.cost_micros
        updates += [(aid, {"impressions": i, "clicks": c, "cost": round(m / 1e6, 2)})
                    for aid, (i, c, m) in totals.items()]
        # image approvals: ad_group_ad_asset_view does not support responsive display ads, so
        # read each uploaded image asset's own policy summary instead
        by_asset = {}
        for r in con.execute("SELECT id, landscape_asset, square_asset FROM articles WHERE "
                             "status='uploaded'"):
            for col, tag in (("landscape_asset", "landscape image"), ("square_asset",
                                                                     "square image")):
                if r[col]:
                    by_asset[r[col]] = (r["id"], tag)
        seen = 0
        for part in ([] if metrics_only else _chunks(list(by_asset))):
            qa = ",".join(_q(x) for x in part)
            for row in self.search("SELECT asset.resource_name, asset.policy_summary.approval_status, "
                                   "asset.policy_summary.review_status FROM asset WHERE "
                                   f"asset.resource_name IN ({qa})", "sync"):
                seen += 1
                aid, tag = by_asset.get(row.asset.resource_name, (None, None))
                st = row.asset.policy_summary.approval_status.name
                if aid:
                    notes = asset_notes.setdefault(aid, [])
                    if st in ("DISAPPROVED", "APPROVED_LIMITED", "AREA_OF_INTEREST_ONLY"):
                        notes.append(f"{tag} {st}")
        if by_asset and not seen and not metrics_only:
            log(f"sync WARNING: none of {len(by_asset)} image assets could be read back")
        updates += [(aid, {"asset_policy": "; ".join(n) or None})
                    for aid, n in asset_notes.items()]
        with db.tx(con):
            for aid, fields in updates:
                db.update_article(con, aid, **fields)
        if not metrics_only:
            db.kv_set(con, "enhancements_on_count", enh_on)
        appr = {}
        for aid, f in updates:
            if "approval" in f:
                appr[f["approval"]] = appr.get(f["approval"], 0) + 1
        rev = {}
        for aid, f in updates:
            if "review_status" in f:
                rev[f["review_status"]] = rev.get(f["review_status"], 0) + 1
        bad_assets = sum(1 for n in asset_notes.values() if any("DISAPPROVED" in x for x in n))
        log(f"sync: {len(ids)} campaigns; approvals {appr or 'none yet'}; review {rev or '-'}; "
            f"ads with a disapproved image/text {bad_assets}; impressions "
            f"{sum(t[0] for t in totals.values())}, clicks {sum(t[1] for t in totals.values())}, "
            f"cost Rs {round(sum(t[2] for t in totals.values()) / 1e6, 2)} since "
            f"{since_date or '-'}; ads with enhancements/auto video ON: {enh_on}")
        return enh_on
