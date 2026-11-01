"""One-off placement quota test against a stub that charges 1 operation per mutate operation
and returns Google's quota error past a limit. Calls bind to the real client signatures."""
import json
import os
import re
import tempfile
import types

from google.ads.googleads.client import GoogleAdsClient

from rda import config, db, placement_test as pt
from rda.gads import API_VERSION, Ads

NS = types.SimpleNamespace


class GoogleAdsException(Exception):           # same class name the pipeline checks for
    def __init__(self):
        super().__init__("RESOURCE_EXHAUSTED: Too many requests")
        self.error = NS(code=lambda: NS(name="RESOURCE_EXHAUSTED"))


class Google:
    """Charges every mutate operation and every search 1 operation against `limit`."""

    def __init__(self, client, limit, reject=()):
        self.client, self.limit, self.used = client, limit, 0
        self.reject = set(reject)
        self.n = 100
        self.campaigns = {}          # name -> resource
        self.adgroups = {}           # campaign resource -> ad group resource
        self.placements = {}         # ad group -> [urls]
        self.targeting = {}          # ad group -> [(dimension, bid_only)]
        self.optimized = {}
        self.creates = 0

    def charge(self, n):
        if self.used + n > self.limit:
            raise GoogleAdsException()
        self.used += n

    def _res(self, coll):
        self.n += 1
        return f"customers/1/{coll}/{self.n}"

    # GoogleAdsService.mutate
    def mutate(self, customer_id, mutate_operations):
        self.charge(len(mutate_operations))
        self.creates += 1
        temp, out = {}, []
        for op in mutate_operations:
            f = op._pb.WhichOneof("operation")
            obj = getattr(op, f).create
            coll = {"campaign_budget_operation": "campaignBudgets",
                    "campaign_operation": "campaigns", "ad_group_operation": "adGroups",
                    "campaign_criterion_operation": "campaignCriteria",
                    "ad_group_ad_operation": "adGroupAds"}[f]
            res = self._res(coll)
            if getattr(obj, "resource_name", ""):
                temp[obj.resource_name] = res
            if f == "campaign_operation":
                assert obj.status.name == "PAUSED"
                self.campaigns[obj.name] = res
            if f == "ad_group_operation":
                camp = temp[obj.campaign]
                self.adgroups[camp] = res
                self.placements[res] = []
                self.targeting[res] = [(t.targeting_dimension.name, t.bid_only)
                                       for t in obj.targeting_setting.target_restrictions]
                self.optimized[res] = obj.optimized_targeting_enabled
            r = self.client.get_type("MutateOperationResponse")
            getattr(r, f.replace("_operation", "_result")).resource_name = res
            out.append(r)
        return NS(mutate_operation_responses=out)

    # AdGroupCriterionService.mutate_ad_group_criteria
    def mutate_ad_group_criteria(self, request):
        ops = request["operations"]
        assert request["partial_failure"] is True and len(ops) <= 10000
        self.charge(len(ops))
        bad, results = [], []
        for i, op in enumerate(ops):
            url = op.create.placement.url
            if url in self.reject:
                bad.append(i)
                results.append(NS(resource_name=""))
            else:
                self.placements[op.create.ad_group].append(url)
                results.append(NS(resource_name=self._res("adGroupCriteria")))
        pfe = None
        if bad:
            f = self.client.get_type("GoogleAdsFailure")
            for i in bad:
                e = self.client.get_type("GoogleAdsError")
                e.message = "not a valid placement"
                el = type(e.location).FieldPathElement(field_name="operations", index=i)
                e.location.field_path_elements.append(el)
                f.errors.append(e)
            pfe = NS(code=3, message="x", details=[NS(value=type(f).serialize(f))])
        return NS(results=results, partial_failure_error=pfe)

    def search(self, q):
        self.charge(1)
        m = re.search(r"campaign\.name = '([^']+)'", q)
        if m:
            r = self.campaigns.get(m.group(1))
            return [NS(campaign=NS(resource_name=r))] if r else []
        m = re.search(r"FROM ad_group WHERE campaign\.resource_name = '([^']+)'", q)
        if m:
            return [NS(ad_group=NS(resource_name=self.adgroups[m.group(1)]))]
        m = re.search(r"FROM ad_group_criterion WHERE ad_group\.resource_name = '([^']+)'", q)
        if m:
            return [NS(ad_group_criterion=NS(placement=NS(url=u)))
                    for u in self.placements[m.group(1)]]
        m = re.search(r"FROM ad_group WHERE ad_group\.resource_name = '([^']+)'", q)
        if m:
            ag = m.group(1)
            trs = [NS(targeting_dimension=NS(name=d), bid_only=b) for d, b in self.targeting[ag]]
            return [NS(ad_group=NS(optimized_targeting_enabled=self.optimized[ag],
                                   targeting_setting=NS(target_restrictions=trs)))]
        raise AssertionError(q)


class Strict:
    def __init__(self, g, service):
        import importlib
        mod = importlib.import_module("google.ads.googleads.v26.services.services." +
                                      re.sub(r"(?<!^)(?=[A-Z])", "_", service).lower())
        self.real, self.g = getattr(mod, service + "Client"), g

    def __getattr__(self, name):
        import inspect
        sig = inspect.signature(getattr(self.real, name))

        def call(*a, **k):
            sig.bind(None, *a, **k)
            return getattr(self.g, name)(*a, **k)
        return call


class FakeSheet:
    def __init__(self, domains):
        self.tabs = {"Placements": [["Domain"]] + [[d] for d in domains]}

    def read(self, tab):
        return self.tabs.get(tab, [])

    def write(self, tab, rows, force=False):
        self.tabs[tab] = rows
        return True


def setup(tmp, limit, n_domains=9405, reject=()):
    config.CUSTOMER_ID = "1"
    con = db.connect(os.path.join(tmp, "t.sqlite"))
    client = GoogleAdsClient(credentials=None, developer_token="x", use_proto_plus=True,
                             version=API_VERSION)
    g = Google(client, limit, reject)

    class TAds(Ads):
        def search(self, query, kind="search"):
            db.log_ops(self.con, 1, kind)
            try:
                return g.search(query)
            except GoogleAdsException as e:
                from rda.gads import QuotaExhausted
                raise QuotaExhausted(str(e))

    ads = TAds(con, client)
    ads.cid = "1"
    client.get_service = lambda name, **k: Strict(g, name)
    db.kv_set(con, "logos_1", {"landscape": "customers/1/assets/91",
                               "square": "customers/1/assets/92"})
    copy = {"headlines": ["a", "b", "c", "d", "e"], "long_headline": "l",
            "descriptions": ["1", "2", "3", "4", "5"], "image_prompt": "p"}
    for i in range(30):
        con.execute("INSERT INTO articles(id,url,final_url,title,status,wave,alcohol,copy_json,"
                    "landscape_asset,square_asset,ad_resource) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    (f"a{i}", f"https://ponly.com/x{i}/", f"https://ponly.com/x{i}/", "T",
                     "uploaded", 1, 1 if i >= 25 else 0, json.dumps(copy),
                     f"customers/1/assets/{1000 + i}", f"customers/1/assets/{2000 + i}",
                     f"customers/1/adGroupAds/{i}"))
    domains = [f"site{i}.com" for i in range(n_domains)]
    logs = []
    w = NS(con=con, ads=ads, sheet=FakeSheet(domains), settings={"Run placement test": True},
           s_bool=lambda k: True, log=logs.append)
    return w, g, domains, logs


def test_counts_one_per_placement_stops_campaign_2_and_reads_back_later():
    with tempfile.TemporaryDirectory() as tmp:
        w, g, domains, logs = setup(tmp, limit=15000, reject={"site7.com"})
        pt.run(w)
        st = db.kv_get(w.con, pt.KEY)
        c1, c2 = st["c"]
        assert c1["accepted"] == 9404 and list(c1["rejected"]) == ["site7.com"]
        assert c1["readback"] == {**c1["readback"], "placements_stored": 9404,
                                  "placement_setting": "Targeting",
                                  "optimized_targeting": False}
        assert c2["result"] == "stopped by Google's quota error"
        assert 4000 < c2["sent"] < 6000                  # 15,000 - c1 (~9,430) in 500s
        assert c2["readback"]["placements_stored"] == c2["sent"] - len(c2["rejected"])
        # read-back while the quota is still used up: waits, then reads later
        c2.pop("readback"); st.pop("done"); st.pop("finished")
        db.kv_set(w.con, pt.KEY, st)
        g.limit = g.used
        pt.run(w)
        assert not db.kv_get(w.con, pt.KEY).get("done")
        g.limit = 10 ** 9                                  # the 24 h window has moved on
        pt.run(w)
        st = db.kv_get(w.con, pt.KEY)
        assert st["done"] and st["c"][1]["readback"]["placements_stored"] == st["c"][1]["sent"] - 1
        rows = dict((r[0], r[1]) for r in w.sheet.tabs["Placement test"] if len(r) == 2)
        assert rows["Campaign 2: result"] == "stopped by Google's quota error"
        assert ["site7.com", "not a valid placement"] in w.sheet.tabs["Placement test"]
        assert g.creates == 2


def test_no_quota_error_means_all_placements_stored():
    with tempfile.TemporaryDirectory() as tmp:
        w, g, domains, logs = setup(tmp, limit=10 ** 9)
        pt.run(w)
        st = db.kv_get(w.con, pt.KEY)
        assert st["done"] and st["c"][1]["result"] == "all placements sent; no quota error"
        assert st["c"][1]["readback"]["placements_stored"] == 9405


def test_twenty_different_non_alcohol_pilot_articles():
    with tempfile.TemporaryDirectory() as tmp:
        w, g, domains, logs = setup(tmp, limit=10 ** 9, n_domains=50)
        pt.run(w)
        a1, a2 = db.kv_get(w.con, pt.KEY)["articles"]
        assert len(set(a1 + a2)) == 20 and len(a1) == len(a2) == 10
        assert not {"a25", "a26", "a27", "a28", "a29"} & set(a1 + a2)


def test_restart_never_creates_a_campaign_twice():
    with tempfile.TemporaryDirectory() as tmp:
        w, g, domains, logs = setup(tmp, limit=10 ** 9, n_domains=50)
        orig = pt._send
        pt._send = lambda *a: (_ for _ in ()).throw(RuntimeError("crash"))
        try:
            pt.run(w)
        except RuntimeError:
            pass
        finally:
            pt._send = orig
        st = db.kv_get(w.con, pt.KEY)
        st["c"][0].pop("campaign")                  # as if the crash came before saving it
        st["c"][0]["in_flight"] = False
        db.kv_set(w.con, pt.KEY, st)
        pt.run(w)
        assert g.creates == 2 and len(g.campaigns) == 2
        assert db.kv_get(w.con, pt.KEY)["done"]


def test_bad_placements_tab_stops_before_anything_is_created():
    with tempfile.TemporaryDirectory() as tmp:
        w, g, domains, logs = setup(tmp, limit=10 ** 9, n_domains=5)
        w.sheet.tabs["Placements"].append(["https://not-a-domain/"])
        try:
            pt.run(w)
            assert False
        except RuntimeError as e:
            assert "plain domain" in str(e)
        assert g.creates == 0 and g.used == 0


def test_runs_only_when_ticked_and_not_done():
    w = NS(settings={"Run placement test": False}, s_bool=lambda k: False, con=None)
    assert not pt.wanted(w)
