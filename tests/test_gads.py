"""Google Ads upload logic against a stateful stub API: resume, no duplicates, partial
failures, lost jobs + reconcile, labels, collisions, partial enable (P9-P11, P21)."""
import json
import os
import re
import tempfile
import time
import types

from google.ads.googleads.client import GoogleAdsClient
from PIL import Image

from rda import config, db, gads
from rda.gads import API_VERSION, Ads

NS = types.SimpleNamespace
COLL = {"campaign_budget_operation": "campaignBudgets", "campaign_operation": "campaigns",
        "campaign_criterion_operation": "campaignCriteria",
        "campaign_label_operation": "campaignLabels", "ad_group_operation": "adGroups",
        "ad_group_criterion_operation": "adGroupCriteria", "asset_operation": "assets",
        "ad_group_ad_operation": "adGroupAds"}


class Stub:
    """Behaves like Google Ads for the calls the pipeline makes."""

    def __init__(self, client, fail_types=()):
        self.client = client
        self.jobs, self.n = {}, 0
        self.fail_types = list(fail_types)
        self.created_counts = {}
        self.add_calls = []          # bytes per add request
        self.hide_jobs = False       # simulate a job that cannot be found
        self.run_crash = False       # run_batch_job raises before Google gets it
        self.labels = []             # (name, status, resource)
        self.label_fail = set()
        self.enable_fail = set()
        self.ent = {k: [] for k in ("budget", "campaign", "crit", "clabel", "ag", "agc",
                                    "asset", "ad")}
        self.temp = {}

    # ---- batch jobs
    def mutate_batch_job(self, customer_id, operation):
        if operation.remove:
            job = self.jobs[operation.remove]
            if job["status"] != "PENDING":
                raise RuntimeError("CAN_ONLY_REMOVE_PENDING_JOB")
            job["status"] = "REMOVED"
            return NS(result=NS(resource_name=operation.remove))
        self.n += 1
        name = f"customers/{customer_id}/batchJobs/{self.n}"
        self.jobs[name] = {"ops": [], "status": "PENDING", "results": []}
        return NS(result=NS(resource_name=name))

    def add_batch_job_operations(self, resource_name, mutate_operations, sequence_token=None):
        self.add_calls.append(sum(op._pb.ByteSize() for op in mutate_operations))
        self.jobs[resource_name]["ops"].extend(mutate_operations)
        return NS(next_sequence_token="t")

    def run_batch_job(self, resource_name):
        if self.run_crash:
            raise RuntimeError("crash before run reached Google")
        job = self.jobs[resource_name]
        job["results"] = [self._exec(i, op) for i, op in enumerate(job["ops"])]
        job["status"] = "DONE"

    def list_batch_job_results(self, request):
        return self.jobs[request["resource_name"]]["results"]

    def _ref(self, r):
        return self.temp.get(r, r) if r else r

    def _exec(self, i, op):
        field = op._pb.WhichOneof("operation")
        x = getattr(op, field).create
        refs = [getattr(x, a, "") for a in ("campaign_budget", "campaign", "ad_group")]
        if field == "ad_group_ad_operation":
            rda = x.ad.responsive_display_ad
            refs += [rda.marketing_images[0].asset, rda.square_marketing_images[0].asset]
        bad_ref = any(r and r.split("/")[-1].lstrip("~").startswith("-") and r not in self.temp
                      for r in refs)
        if field in self.fail_types or bad_ref:
            if field in self.fail_types:
                self.fail_types.remove(field)
            return NS(operation_index=i, mutate_operation_response=None,
                      status=NS(code=3, message=f"{field} failed", details=[]))
        self.n += 1
        if field == "ad_group_ad_operation":
            res = f"customers/1/adGroupAds/{self._ref(x.ad_group).split('/')[-1]}~{self.n}"
        else:
            res = f"customers/1/{COLL[field]}/{self.n}"
        if getattr(x, "resource_name", ""):
            self.temp[x.resource_name] = res
        self.created_counts[field] = self.created_counts.get(field, 0) + 1
        if field == "campaign_budget_operation":
            self.ent["budget"].append((x.name, res))
        elif field == "campaign_operation":
            self.ent["campaign"].append((x.name, res))
        elif field == "campaign_criterion_operation":
            self.ent["crit"].append((self._ref(x.campaign),
                                     x._pb.WhichOneof("criterion").upper(), res))
        elif field == "campaign_label_operation":
            name = next(n for n, _, r in self.labels if r == x.label)
            self.ent["clabel"].append((self._ref(x.campaign), name))
        elif field == "ad_group_operation":
            self.ent["ag"].append((self._ref(x.campaign), x.name, res))
        elif field == "ad_group_criterion_operation":
            self.ent["agc"].append((self._ref(x.ad_group), res))
        elif field == "asset_operation":
            self.ent["asset"].append((x.name, res))
        elif field == "ad_group_ad_operation":
            self.ent["ad"].append((self._ref(x.ad_group), x.ad.final_urls[0], res))
        resp = NS(_pb=NS(WhichOneof=lambda _: "x"), x=NS(resource_name=res))
        return NS(operation_index=i, mutate_operation_response=resp,
                  status=NS(code=0, message="", details=[]))

    # ---- labels / campaigns (partial failure)
    def _failure(self, idxs, msg):
        f = self.client.get_type("GoogleAdsFailure")
        for i in idxs:
            e = self.client.get_type("GoogleAdsError")
            e.message = msg
            el = type(e.location).FieldPathElement(field_name="operations", index=i)
            e.location.field_path_elements.append(el)
            f.errors.append(e)
        return NS(code=3, message=msg, details=[NS(
            type_url="type.googleapis.com/google.ads.googleads.v26.errors.GoogleAdsFailure",
            value=type(f).serialize(f))])

    def mutate_labels(self, request):
        out, bad = [], []
        for i, op in enumerate(request["operations"]):
            if op.create.name in self.label_fail:
                bad.append(i)
                out.append(NS(resource_name=""))
                continue
            self.n += 1
            r = f"customers/1/labels/{self.n}"
            self.labels.append((op.create.name, "ENABLED", r))
            out.append(NS(resource_name=r))
        return NS(results=out, partial_failure_error=self._failure(bad, "DUPLICATE_NAME")
                  if bad else None)

    def mutate_campaign_budgets(self, request):
        out = []
        for op in request["operations"]:
            self.ent["budget"] = [b for b in self.ent["budget"] if b[1] != op.remove]
            out.append(NS(resource_name=op.remove))
        return NS(results=out, partial_failure_error=None)

    def mutate_campaigns(self, request):
        out, bad = [], []
        if request["operations"] and request["operations"][0].remove:
            for i, op in enumerate(request["operations"]):
                if op.remove in self.enable_fail:
                    bad.append(i)
                    out.append(NS(resource_name=""))
                    continue
                self.ent["campaign"] = [c for c in self.ent["campaign"] if c[1] != op.remove]
                out.append(NS(resource_name=op.remove))
            return NS(results=out, partial_failure_error=self._failure(bad, "cannot remove")
                      if bad else None)
        for i, op in enumerate(request["operations"]):
            if op.update.resource_name in self.enable_fail:
                bad.append(i)
                out.append(NS(resource_name=""))
            else:
                out.append(NS(resource_name=op.update.resource_name))
        return NS(results=out, partial_failure_error=self._failure(bad, "budget too low")
                  if bad else None)

    # ---- search
    def search(self, query):
        frm = re.search(r"FROM (\w+)", query).group(1)
        e = self.ent
        if frm == "batch_job":
            res = query.split("'")[1]
            if self.hide_jobs or res not in self.jobs:
                return []
            return [NS(batch_job=NS(status=NS(name=self.jobs[res]["status"])))]
        if frm == "label":
            return [NS(label=NS(name=n, status=NS(name=s), resource_name=r))
                    for n, s, r in self.labels]
        if frm == "customer":
            return [NS(customer=NS(time_zone="Asia/Kolkata"))]
        if frm == "campaign_budget":
            return [NS(campaign_budget=NS(name=n, resource_name=r)) for n, r in e["budget"]]
        if frm == "campaign":
            return [NS(campaign=NS(name=n, resource_name=r)) for n, r in e["campaign"]]
        if frm == "campaign_criterion":
            return [NS(campaign=NS(resource_name=c), campaign_criterion=NS(
                resource_name=r, type_=NS(name=t))) for c, t, r in e["crit"]]
        if frm == "campaign_label":
            return [NS(campaign=NS(resource_name=c), label=NS(name=n)) for c, n in e["clabel"]]
        if frm == "ad_group":
            return [NS(campaign=NS(resource_name=c), ad_group=NS(name=n, resource_name=r))
                    for c, n, r in e["ag"]]
        if frm == "ad_group_criterion":
            return [NS(ad_group=NS(resource_name=g), ad_group_criterion=NS(resource_name=r))
                    for g, r in e["agc"]]
        if frm == "ad_group_ad":
            return [NS(ad_group=NS(resource_name=g), ad_group_ad=NS(
                resource_name=r, ad=NS(final_urls=[u]))) for g, u, r in e["ad"]]
        if frm == "asset":
            return [NS(asset=NS(name=n, resource_name=r)) for n, r in e["asset"]]
        raise AssertionError(query)


class Strict:
    """Every call must bind to the REAL google-ads client method signature, so a wrong
    keyword (like page_size= on list_batch_job_results) fails here as it does live."""

    def __init__(self, stub, service):
        import importlib
        mod = importlib.import_module("google.ads.googleads.v26.services.services." +
                                      re.sub(r"(?<!^)(?=[A-Z])", "_", service).lower())
        self.real = getattr(mod, service + "Client")
        self.stub = stub

    def __getattr__(self, name):
        import inspect
        sig = inspect.signature(getattr(self.real, name))

        def call(*a, **k):
            sig.bind(None, *a, **k)            # raises TypeError on a bad keyword
            return getattr(self.stub, name)(*a, **k)
        return call


def setup(tmp, fail_types=(), n_articles=4):
    config.KEEP_IMAGES = False
    config.IMG_DIR = os.path.join(tmp, "img")
    config.CUSTOMER_ID = "1"
    os.makedirs(config.IMG_DIR, exist_ok=True)
    con = db.connect(os.path.join(tmp, "t.sqlite"))
    client = GoogleAdsClient(credentials=None, developer_token="x", use_proto_plus=True,
                             version=API_VERSION)
    stub = Stub(client, fail_types)

    class TAds(Ads):
        def search(self, query, kind="search"):
            db.log_ops(self.con, 1, kind)
            return stub.search(query)

    ads = TAds(con, client)
    ads.cid = "1"
    client.get_service = lambda name, **k: Strict(stub, name)
    copy = {"headlines": ["a", "b", "c", "d", "e"], "long_headline": "l",
            "descriptions": ["1", "2", "3", "4", "5"], "image_prompt": "p"}
    con.execute("INSERT INTO campaigns(key,wave,pool,nn,theme,name,n) VALUES('c1',1,'P',1,'T','C1',4)")
    con.execute("INSERT INTO adgroups(key,campaign_key,nn,theme,name,n,alcohol) "
                "VALUES('g1','c1',1,'T','G1',4,1)")
    for i in range(n_articles):
        lp, sp = os.path.join(tmp, f"{i}l.jpg"), os.path.join(tmp, f"{i}s.jpg")
        Image.new("RGB", (1200, 628)).save(lp)
        Image.new("RGB", (720, 720)).save(sp)
        con.execute("INSERT INTO articles(id,url,final_url,title,category,status,copy_json,"
                    "landscape_path,square_path,adgroup_key) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (f"a{i}", f"https://ponly.com/x{i}/", f"https://ponly.com/x{i}/", "T", "P",
                     "planned", json.dumps(copy), lp, sp, "g1"))
    logos = {"landscape": "customers/1/assets/91", "square": "customers/1/assets/92"}
    return con, ads, stub, logos


def drive(ads, logos, rounds=6):
    for _ in range(rounds):
        ads.run_upload(1, logos, 10000)


def uploaded(con):
    return con.execute("SELECT COUNT(*) n FROM articles WHERE status='uploaded'").fetchone()["n"]


def final_states(con):
    return {r["status"] for r in con.execute("SELECT status FROM articles")}


def assert_once(stub, n=4):
    c = stub.created_counts
    assert c.get("campaign_operation") == 1 and c.get("campaign_budget_operation") == 1
    assert c.get("ad_group_operation") == 1
    assert c.get("campaign_criterion_operation") == 2         # location + language
    assert c.get("ad_group_ad_operation") == n
    assert c.get("asset_operation") == 2 * n


def test_full_upload_no_duplicates():
    with tempfile.TemporaryDirectory() as tmp:
        con, ads, stub, logos = setup(tmp)
        drive(ads, logos)
        assert_once(stub)
        assert stub.created_counts["ad_group_criterion_operation"] == 1  # 18-24 exclusion
        assert stub.created_counts["campaign_label_operation"] == 2      # Ponly Cook + category
        assert json.loads(con.execute("SELECT labels_done FROM campaigns").fetchone()[0]) == \
            ["Ponly Cook", "P"]
        assert uploaded(con) == 4
        assert not ads.has_work(1) and ads.ops_for_wave(1, logos) == []
        assert not any(os.path.exists(os.path.join(tmp, f"{i}l.jpg")) for i in range(4))


def test_crash_while_adding_abandons_and_redoes_once():
    with tempfile.TemporaryDirectory() as tmp:
        con, ads, stub, logos = setup(tmp)
        orig = stub.add_batch_job_operations
        stub.add_batch_job_operations = lambda **k: (_ for _ in ()).throw(RuntimeError("crash"))
        try:
            ads.run_upload(1, logos, 10000)
        except RuntimeError:
            pass
        stub.add_batch_job_operations = orig
        drive(ads, logos)
        assert_once(stub)
        assert uploaded(con) == 4
        assert "abandoned" in [r["state"] for r in con.execute("SELECT state FROM jobs")]


def test_create_batch_raises_changes_nothing():
    with tempfile.TemporaryDirectory() as tmp:
        con, ads, stub, logos = setup(tmp)
        orig = stub.mutate_batch_job
        stub.mutate_batch_job = lambda **k: (_ for _ in ()).throw(RuntimeError("503"))
        try:
            ads.run_upload(1, logos, 10000)
        except RuntimeError:
            pass
        stub.mutate_batch_job = orig
        drive(ads, logos)
        assert_once(stub)
        assert uploaded(con) == 4


def test_pending_after_run_waits_then_removed_and_rebuilt_once():
    with tempfile.TemporaryDirectory() as tmp:
        con, ads, stub, logos = setup(tmp)
        stub.run_crash = True
        try:
            ads.run_upload(1, logos, 10000)
        except RuntimeError:
            pass
        stub.run_crash = False
        job = con.execute("SELECT * FROM jobs").fetchone()
        assert job["state"] == "run_requested"
        drive(ads, logos, 3)                    # PENDING after run: keep waiting (R12)
        assert con.execute("SELECT state FROM jobs").fetchone()["state"] == "run_requested"
        assert not stub.created_counts
        con.execute("UPDATE jobs SET created=?", (time.time() - 25 * 3600,))
        drive(ads, logos, 2)
        assert stub.jobs[job["external_id"]]["status"] == "REMOVED"
        assert not stub.created_counts           # reconcile delay before any rebuild
        con.execute("UPDATE kv SET v=? WHERE k='reconcile_due_1'", (json.dumps(time.time() - 1),))
        drive(ads, logos)
        assert_once(stub)
        assert uploaded(con) == 4


def test_lost_job_is_reconciled_without_duplicates():
    with tempfile.TemporaryDirectory() as tmp:
        con, ads, stub, logos = setup(tmp)
        ads.run_upload(1, logos, 10000)        # everything is created in Google Ads ...
        stub.hide_jobs = True                   # ... but the job can no longer be found
        for _ in range(gads.LOST_TRIES):
            ads.run_upload(1, logos, 10000)
        assert con.execute("SELECT state FROM jobs").fetchone()["state"] == "lost"
        stub.hide_jobs = False
        before = dict(stub.created_counts)
        drive(ads, logos, 3)                    # reconcile waits (search may lag)
        assert stub.created_counts == before and uploaded(con) == 0
        con.execute("UPDATE kv SET v=? WHERE k='reconcile_due_1'", (json.dumps(time.time() - 1),))
        drive(ads, logos)
        assert_once(stub)                       # reconcile adopted it all
        assert stub.created_counts["campaign_label_operation"] == 2
        assert stub.created_counts["ad_group_criterion_operation"] == 1
        assert uploaded(con) == 4
        assert db.kv_get(con, "lost_job_1")


def test_partial_failure_retries_only_missing():
    with tempfile.TemporaryDirectory() as tmp:
        con, ads, stub, logos = setup(tmp, fail_types=["ad_group_ad_operation"])
        drive(ads, logos)
        assert_once(stub)
        assert uploaded(con) == 4


def test_quota_respected():
    with tempfile.TemporaryDirectory() as tmp:
        con, ads, stub, logos = setup(tmp)
        assert ads.run_upload(1, logos, 10) == 0
        assert stub.n == 0


def test_campaign_failure_reported_and_stops_after_3():
    with tempfile.TemporaryDirectory() as tmp:
        con, ads, stub, logos = setup(tmp)
        stub.fail_types = ["campaign_operation"] * 3
        drive(ads, logos, rounds=12)
        c = con.execute("SELECT * FROM campaigns WHERE key='c1'").fetchone()
        assert c["status"] == "failed" and c["attempts"] == 3 and "campaign" in c["error"]
        errs = [r["error"] for r in con.execute("SELECT error FROM articles WHERE status='error'")]
        assert len(errs) == 4 and all("campaign not created" in e for e in errs)
        assert not ads.has_work(1)
        assert final_states(con) == {"error"}


def test_add_requests_split_by_bytes():
    with tempfile.TemporaryDirectory() as tmp:
        old = config.BYTES_PER_ADD_REQUEST
        config.BYTES_PER_ADD_REQUEST = 3000
        try:
            con, ads, stub, logos = setup(tmp)
            drive(ads, logos)
        finally:
            config.BYTES_PER_ADD_REQUEST = old
        assert len(stub.add_calls) > 1
        assert uploaded(con) == 4
        assert_once(stub)


def test_oversized_and_missing_image_become_article_errors():
    with tempfile.TemporaryDirectory() as tmp:
        con, ads, stub, logos = setup(tmp)
        with open(os.path.join(tmp, "0l.jpg"), "wb") as f:
            f.write(os.urandom(60_000))
        os.remove(os.path.join(tmp, "1s.jpg"))
        old = gads.MAX_ASSET_BYTES
        gads.MAX_ASSET_BYTES = 30_000
        try:
            drive(ads, logos)
        finally:
            gads.MAX_ASSET_BYTES = old
        st = {r["id"]: (r["status"], r["error"]) for r in con.execute("SELECT * FROM articles")}
        assert st["a0"][0] == "error" and "too large" in st["a0"][1]
        assert st["a1"][0] == "error" and "missing" in st["a1"][1]
        assert uploaded(con) == 2 and not ads.has_work(1)


def test_removed_label_reported_and_skipped():
    with tempfile.TemporaryDirectory() as tmp:
        con, ads, stub, logos = setup(tmp)
        stub.labels.append(("p", "REMOVED", "customers/1/labels/777"))   # case-insensitive
        drive(ads, logos)
        assert uploaded(con) == 4
        assert json.loads(con.execute("SELECT labels_done FROM campaigns").fetchone()[0]) == \
            ["Ponly Cook"]
        assert "P" in db.kv_get(con, "label_problems_1")
        assert stub.created_counts["campaign_label_operation"] == 1


def test_label_create_failure_never_blocks():
    with tempfile.TemporaryDirectory() as tmp:
        con, ads, stub, logos = setup(tmp)
        stub.label_fail = {"P"}
        drive(ads, logos)
        assert uploaded(con) == 4
        assert "could not be created" in db.kv_get(con, "label_problems_1")["P"]


def test_existing_label_reused():
    with tempfile.TemporaryDirectory() as tmp:
        con, ads, stub, logos = setup(tmp)
        stub.labels.append(("ponly cook", "ENABLED", "customers/1/labels/555"))
        drive(ads, logos)
        assert db.kv_get(con, "labels_1")["Ponly Cook"] == "customers/1/labels/555"


def test_name_collision_blocks_campaign():
    with tempfile.TemporaryDirectory() as tmp:
        con, ads, stub, logos = setup(tmp)
        stub.ent["campaign"].append(("C1", "customers/1/campaigns/999"))   # someone else's
        drive(ads, logos)
        c = con.execute("SELECT * FROM campaigns").fetchone()
        assert c["status"] == "failed" and "already exists" in c["error"]
        assert not stub.created_counts
        assert final_states(con) == {"error"}


def test_partial_enable_failure():
    with tempfile.TemporaryDirectory() as tmp:
        con, ads, stub, logos = setup(tmp)
        con.execute("INSERT INTO campaigns(key,wave,pool,nn,theme,name,n) "
                    "VALUES('c2',1,'P',2,'T','C2',0)")
        drive(ads, logos)
        con.execute("UPDATE campaigns SET campaign_res='customers/1/campaigns/2000', "
                    "status='created' WHERE key='c2'")
        stub.enable_fail = {"customers/1/campaigns/2000"}
        assert ads.enable_wave(1) == 1
        st = {r["key"]: (r["status"], r["error"]) for r in con.execute("SELECT * FROM campaigns")}
        assert st["c1"][0] == "enabled"
        assert st["c2"][0] == "created" and "budget too low" in st["c2"][1]


def test_account_today_uses_account_time_zone():
    with tempfile.TemporaryDirectory() as tmp:
        con, ads, stub, logos = setup(tmp)
        import datetime
        from zoneinfo import ZoneInfo
        assert ads.account_today() == datetime.datetime.now(ZoneInfo("Asia/Kolkata")).date().isoformat()
        assert db.kv_get(con, "tz_1") == "Asia/Kolkata"


def test_decode_status_reads_google_ads_failure():
    with tempfile.TemporaryDirectory() as tmp:
        con, ads, stub, logos = setup(tmp)
        st = stub._failure([0], "too many campaigns")
        assert "too many campaigns" in ads.decode_status(st)
        assert ads.decode_status(NS(details=[], message="plain", code=3)) == "plain"


def test_validate_wave_uses_real_signature():
    with tempfile.TemporaryDirectory() as tmp:
        con, ads, stub, logos = setup(tmp)
        seen = []
        stub.mutate = lambda request: seen.append(request["validate_only"])
        config.LOGO_LANDSCAPE = config.LOGO_LANDSCAPE   # real logo files in assets/
        assert ads.validate_wave(1) == [] and seen == [True]


def _live_worker(con, ads):
    from rda import fakes
    claude = fakes.FakeClaude()
    claude.c = NS(messages=NS(batches=NS(retrieve=lambda bid: NS(request_counts=NS(
        processing=0, succeeded=len(claude.batches[bid]), errored=0, canceled=0, expired=0)))))
    logs = []
    return NS(con=con, ads=ads, claude=claude, rep=fakes.FakeReplicate(),
              log=logs.append, logs=logs)


def test_live_check_runs_and_finds_duplicates():
    from rda import livecheck
    with tempfile.TemporaryDirectory() as tmp:
        con, ads, stub, logos = setup(tmp)
        drive(ads, logos)
        w = _live_worker(con, ads)
        bid = w.claude.create_batch([("alc-a0", {"output_config": {"format": {"schema": {
            "properties": {"alcohol": {}}}}}, "messages": [{"content": "x"}]})])
        db.new_job(con, "alcohol", payload={}, external_id=bid, state="done")
        w.claude.find_batch = lambda a, b, n: [bid]
        livecheck.run(w)
        res = db.kv_get(con, "live_check")["results"]
        fails = [r for r in res if not r.startswith("OK")]
        # volume is not a mount in tests; everything else must pass
        assert [f.split(":")[0] for f in fails] == ["FAIL volume"], fails
        assert any("batch results" in r and "0 errors" in r for r in res)
        g, u, r = stub.ent["ad"][0]
        stub.ent["ad"].append((g, u, r + "dup"))                   # a duplicate ad
        livecheck.run(w)
        res = db.kv_get(con, "live_check")["results"]
        assert any(r.startswith("FAIL Google Ads duplicates") and "ad x2" in r for r in res)


def test_remove_campaigns_real_signature_and_partial_failure():
    with tempfile.TemporaryDirectory() as tmp:
        con, ads, stub, logos = setup(tmp)
        drive(ads, logos)
        c = dict(con.execute("SELECT * FROM campaigns").fetchone())
        errs, berr = ads.remove_campaigns([c])
        assert errs == {"c1": None} and berr == {}
        assert not stub.ent["campaign"] and not stub.ent["budget"]
        stub.enable_fail = {c["campaign_res"]}
        errs, _ = ads.remove_campaigns([c])
        assert "cannot remove" in errs["c1"]


def test_sync_adds_up_daily_rows():
    with tempfile.TemporaryDirectory() as tmp:
        con, ads, stub, logos = setup(tmp)
        drive(ads, logos)
        ad = con.execute("SELECT ad_resource FROM articles WHERE id='a0'").fetchone()[0]

        def search(q, kind="search"):
            if "time_zone" in q:
                return [NS(customer=NS(time_zone="Asia/Kolkata"))]
            if "metrics" in q:          # one row per day
                return [NS(ad_group_ad=NS(resource_name=ad), metrics=NS(impressions=100, clicks=2)),
                        NS(ad_group_ad=NS(resource_name=ad), metrics=NS(impressions=50, clicks=1))]
            cs = NS(enable_asset_enhancements=False, enable_autogen_video=False)
            return [NS(ad_group_ad=NS(resource_name=ad, policy_summary=NS(
                approval_status=NS(name="APPROVED"), policy_topic_entries=[]),
                ad=NS(responsive_display_ad=NS(control_spec=cs))))]
        ads.search = search
        ads.sync("2026-10-24")
        r = con.execute("SELECT impressions, clicks, approval FROM articles WHERE id='a0'").fetchone()
        assert (r["impressions"], r["clicks"], r["approval"]) == (150, 3, "APPROVED")
