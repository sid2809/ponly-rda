"""The whole worker driven against the REAL Google Ads code (rda/gads.py: batch jobs, resume,
labels, exclusions, pre-enable check, enable, sync) on the strict stub from test_gads, so the
worker-level 'nothing created twice / every article final' checks cover the real upload path."""
from google.ads.googleads.client import GoogleAdsClient

from rda import config, db
from rda.gads import API_VERSION, Ads
from tests.test_gads import Strict, Stub
from tests.test_worker_faults import drive, make, tmp  # noqa: F401


def make_real(every=40, fail_types=()):
    w = make(every=every)
    config.CUSTOMER_ID = "1"
    client = GoogleAdsClient(credentials=None, developer_token="x", use_proto_plus=True,
                             version=API_VERSION)
    stub = Stub(client, fail_types)
    client.get_service = lambda name, **k: Strict(stub, name)

    class TAds(Ads):
        def search(self, query, kind="search"):
            db.log_ops(self.con, 1, kind)
            return stub.search(query)

        def upload_logos(self):
            return {"landscape": "customers/1/assets/91", "square": "customers/1/assets/92"}

    def factory(con):
        a = TAds(con, client)
        a.cid = "1"
        return a
    w._ads_factory, w._ads = factory, None
    return w, stub


def check_final(w, stub):
    con = w.con
    st = dict(con.execute("SELECT status, COUNT(*) FROM articles GROUP BY status").fetchall())
    assert set(st) <= {"uploaded", "skipped", "error"}, st
    up = st.get("uploaded", 0)
    assert stub.created_counts.get("ad_group_ad_operation", 0) == up         # nothing twice
    camps = con.execute("SELECT COUNT(*) FROM campaigns WHERE campaign_res IS NOT NULL").fetchone()[0]
    assert stub.created_counts.get("campaign_operation", 0) == camps
    names = [n for n, _ in stub.ent["campaign"]]
    assert len(names) == len(set(names))
    alc = con.execute("SELECT COUNT(*) FROM adgroups WHERE alcohol=1 AND adgroup_res IS NOT NULL"
                      ).fetchone()[0]
    assert len(stub.ent["agc"]) == alc                                    # every exclusion once
    enabled = con.execute("SELECT COUNT(*) FROM campaigns WHERE status='enabled'").fetchone()[0]
    assert enabled == camps                                               # nothing left off
    return up


def test_full_run_on_real_google_ads_code(tmp):
    w, stub = make_real()
    assert drive(w, 500)
    assert check_final(w, stub) > 100
    assert w.con.execute("SELECT SUM(cost) FROM articles").fetchone()[0] > 0   # cost synced


def test_full_run_with_google_ads_errors(tmp):
    w, stub = make_real(fail_types=["ad_group_ad_operation", "asset_operation",
                                    "campaign_operation", "ad_group_criterion_operation"])
    assert drive(w, 500)
    check_final(w, stub)
