"""Google Ads upload logic against a stub API: resume, no duplicates, partial failures."""
import json
import os
import tempfile
import types

from google.ads.googleads.client import GoogleAdsClient
from PIL import Image

from rda import config, db
from rda.gads import API_VERSION, Ads


class StubBatch:
    def __init__(self, fail_types=()):
        self.jobs, self.n, self.fail_types = {}, 0, set(fail_types)
        self.created_counts = {}

    def mutate_batch_job(self, customer_id, operation):
        self.n += 1
        name = f"customers/{customer_id}/batchJobs/{self.n}"
        self.jobs[name] = {"ops": [], "status": "PENDING"}
        return types.SimpleNamespace(result=types.SimpleNamespace(resource_name=name))

    def add_batch_job_operations(self, resource_name, mutate_operations, sequence_token=None):
        self.jobs[resource_name]["ops"].extend(mutate_operations)
        return types.SimpleNamespace(next_sequence_token="t")

    def run_batch_job(self, resource_name):
        self.jobs[resource_name]["status"] = "DONE"

    def list_batch_job_results(self, resource_name, page_size):
        out = []
        for i, op in enumerate(self.jobs[resource_name]["ops"]):
            field = op._pb.WhichOneof("operation")
            fail = field in self.fail_types
            self.fail_types.discard(field) if fail else None
            if not fail:
                self.created_counts[field] = self.created_counts.get(field, 0) + 1
            resp = types.SimpleNamespace(_pb=types.SimpleNamespace(WhichOneof=lambda _: "x"),
                                         x=types.SimpleNamespace(
                                             resource_name=f"customers/1/real/{field}/{i}~{i}"))
            out.append(types.SimpleNamespace(
                operation_index=i, mutate_operation_response=resp,
                status=types.SimpleNamespace(code=3 if fail else 0,
                                             message="boom" if fail else "")))
        return out


def setup(tmp, fail_types=()):
    config.KEEP_IMAGES = False
    config.IMG_DIR = os.path.join(tmp, "img")
    os.makedirs(config.IMG_DIR, exist_ok=True)
    con = db.connect(os.path.join(tmp, "t.sqlite"))
    client = GoogleAdsClient(credentials=None, developer_token="x", use_proto_plus=True,
                             version=API_VERSION)
    stub = StubBatch(fail_types)

    class TAds(Ads):
        def search(self, query, kind="search"):
            db.log_ops(self.con, 1, kind)
            res = query.split("'")[1]
            st = types.SimpleNamespace(name=stub.jobs[res]["status"])
            return [types.SimpleNamespace(batch_job=types.SimpleNamespace(status=st))]

    ads = TAds(con, client)
    ads.cid = "1"
    client.get_service = lambda name, **k: stub
    copy = {"headlines": ["a", "b", "c", "d", "e"], "long_headline": "l",
            "descriptions": ["1", "2", "3", "4", "5"], "image_prompt": "p"}
    con.execute("INSERT INTO campaigns(key,wave,pool,nn,theme,name,n) VALUES('c1',1,'P',1,'T','C1',4)")
    con.execute("INSERT INTO adgroups(key,campaign_key,nn,theme,name,n,alcohol) "
                "VALUES('g1','c1',1,'T','G1',4,1)")
    for i in range(4):
        lp, sp = os.path.join(tmp, f"{i}l.jpg"), os.path.join(tmp, f"{i}s.jpg")
        Image.new("RGB", (1200, 628)).save(lp)
        Image.new("RGB", (720, 720)).save(sp)
        con.execute("INSERT INTO articles(id,url,final_url,title,category,status,copy_json,"
                    "landscape_path,square_path,adgroup_key) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (f"a{i}", "https://ponly.com/x/", "https://ponly.com/x/", "T", "P",
                     "planned", json.dumps(copy), lp, sp, "g1"))
    logos = {"landscape": "customers/1/assets/91", "square": "customers/1/assets/92"}
    return con, ads, stub, logos


def drive(ads, logos, rounds=6):
    for _ in range(rounds):
        ads.run_upload(1, logos, 10000)


def test_full_upload_no_duplicates():
    with tempfile.TemporaryDirectory() as tmp:
        con, ads, stub, logos = setup(tmp)
        drive(ads, logos)
        assert stub.created_counts["campaign_operation"] == 1
        assert stub.created_counts["campaign_budget_operation"] == 1
        assert stub.created_counts["ad_group_ad_operation"] == 4
        assert stub.created_counts["asset_operation"] == 8
        assert stub.created_counts["ad_group_criterion_operation"] == 1  # 18-24 exclusion
        assert con.execute("SELECT COUNT(*) n FROM articles WHERE status='uploaded'").fetchone()["n"] == 4
        assert ads.ops_for_wave(1, logos) == []
        # images deleted once both assets exist
        assert not any(os.path.exists(os.path.join(tmp, f"{i}l.jpg")) for i in range(4))


def test_crash_before_run_abandons_and_redoes_once():
    with tempfile.TemporaryDirectory() as tmp:
        con, ads, stub, logos = setup(tmp)
        orig = stub.run_batch_job
        stub.run_batch_job = lambda resource_name: (_ for _ in ()).throw(RuntimeError("crash"))
        try:
            ads.run_upload(1, logos, 10000)
        except RuntimeError:
            pass
        stub.run_batch_job = orig
        drive(ads, logos)
        assert stub.created_counts["campaign_operation"] == 1
        assert stub.created_counts["ad_group_ad_operation"] == 4
        states = [r["state"] for r in con.execute("SELECT state FROM jobs")]
        assert "abandoned" in states


def test_partial_failure_retries_only_missing():
    with tempfile.TemporaryDirectory() as tmp:
        con, ads, stub, logos = setup(tmp, fail_types=["ad_group_ad_operation"])
        drive(ads, logos)
        assert stub.created_counts["campaign_operation"] == 1
        assert stub.created_counts["asset_operation"] == 8
        assert stub.created_counts["ad_group_ad_operation"] == 4
        assert con.execute("SELECT COUNT(*) n FROM articles WHERE status='uploaded'").fetchone()["n"] == 4


def test_quota_respected():
    with tempfile.TemporaryDirectory() as tmp:
        con, ads, stub, logos = setup(tmp)
        assert ads.run_upload(1, logos, 10) == 0   # campaign unit (~20 ops) does not fit
        assert stub.n == 0


def test_campaign_failure_reported_and_stops_after_3():
    with tempfile.TemporaryDirectory() as tmp:
        con, ads, stub, logos = setup(tmp)
        orig = stub.list_batch_job_results

        def always_fail_campaign(resource_name, page_size):
            # campaign fails; everything that references it fails too (as in Google Ads)
            out = []
            for r in orig(resource_name, page_size):
                op = stub.jobs[resource_name]["ops"][r.operation_index]
                if op._pb.WhichOneof("operation") not in ("asset_operation",
                                                           "campaign_budget_operation"):
                    r.status.code, r.status.message = 3, "RESOURCE_LIMIT: too many campaigns"
                out.append(r)
            return out
        stub.list_batch_job_results = always_fail_campaign
        drive(ads, logos, rounds=12)
        c = con.execute("SELECT * FROM campaigns WHERE key='c1'").fetchone()
        assert c["status"] == "failed" and c["attempts"] == 3 and "campaign" in c["error"]
        errs = [r["error"] for r in con.execute("SELECT error FROM articles WHERE status='error'")]
        assert len(errs) == 4 and all("campaign not created" in e for e in errs)
        assert ads.ops_for_wave(1, logos) == []   # no endless retries
