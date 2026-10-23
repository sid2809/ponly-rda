"""Offline fakes for DRY_RUN and tests: nothing is paid, nothing is created in Google Ads.
The Google Ads fake still builds the real API operations, so their structure is tested."""
import csv
import glob
import io
import json
import os
import random
import uuid

from PIL import Image

from . import config, db
from .sheet import SETTINGS_DEFAULTS
from .split import pretty_category


class FakeSheet:
    def __init__(self, csv_dir=None):
        self.path = os.path.join(config.DATA_DIR, "fake_sheet.json")
        if os.path.exists(self.path):
            self.tabs = json.load(open(self.path))
        else:
            self.tabs = {"Settings": [list(r) for r in SETTINGS_DEFAULTS]}
            rows = [["URL", "Title", "Category"]]
            for f in sorted(glob.glob(os.path.join(csv_dir or "data_csv", "*.csv"))):
                cat = pretty_category(os.path.basename(f)[:-4])
                for r in csv.DictReader(open(f, encoding="utf-8-sig")):
                    rows.append([r["URL"].strip(), r["Title"].strip(), cat])
            self.tabs["Input"] = rows
            self._save()

    def _save(self):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        json.dump(self.tabs, open(self.path, "w"))

    def read(self, tab):
        return self.tabs.get(tab, [])

    def read_settings(self):
        return {str(r[0]): (r[1] if len(r) > 1 else "") for r in self.tabs["Settings"][1:]}

    def set_setting(self, key, value):
        for r in self.tabs["Settings"]:
            if r[0] == key:
                r[1] = value
        self._save()

    def write(self, tab, rows, force=False):
        self.tabs[tab] = rows
        self._save()
        return True


class FakeClaude:
    def __init__(self, fail_rate=0.05, seed=1):
        self.batches = {}
        self.rng = random.Random(seed)
        self.fail_rate = fail_rate

    def _answer(self, params):
        schema = params["output_config"]["format"]["schema"]["properties"]
        content = params["messages"][0]["content"]
        text = content if isinstance(content, str) else content[-1]["text"]
        if "headlines" in schema:
            t = text.split("Title: ", 1)[1].split("\n", 1)[0]
            short = t[:24]
            return {"headlines": [f"{short}{s}"[:30] for s in ("", " Recipe", " Ideas",
                                                                 " Tonight", " Guide")],
                    "long_headline": f"How to make {t}"[:90],
                    "descriptions": [f"{p} {t}."[:90] for p in
                                     ("Make", "Try", "Cook", "See how to make", "Learn to cook")],
                    "image_prompt": f"A plated {t} on a table."}
        if "pass" in schema:
            ok = self.rng.random() > self.fail_rate
            return {"pass": ok, "problems": [] if ok else ["visible text"]}
        if "alcohol" in schema:
            return {"alcohol": False, "reason": "no alcohol named"}
        if "themes" in schema:
            return {"themes": ["Classic Favorites", "Savory Mains", "Sweet Treats",
                               "Quick Bites", "Hearty Dishes"]}
        if "a" in schema and "pass" not in schema:
            lines = [l for l in text.split("\n\n")[-1].splitlines() if l[:1].isdigit() and ". " in l]
            return {"a": [[int(l.split(".")[0]), hash(l) % 5] for l in lines]}
        raise ValueError("unknown schema")

    def sync_json(self, params):
        return self._answer(params)

    def create_batch(self, requests):
        bid = "msgbatch_" + uuid.uuid4().hex[:10]
        self.batches[bid] = {cid: ("ok", self._answer(p)) for cid, p in requests}
        return bid

    def batch_ended(self, bid):
        return True

    def find_batch(self, created_after, created_before, n_requests):
        return []

    def batch_results(self, bid):
        return self.batches[bid]


class FakeReplicate:
    def __init__(self):
        self.preds = {}

    def schema_inputs(self, ref):
        return {"Input": {"properties": {"aspect_ratio": {"enum": ["1:1", "16:9"]}}}}

    def create(self, ref, inputs):
        pid = uuid.uuid4().hex[:12]
        self.preds[pid] = (1344, 768) if "banana" in ref else (1280, 720)
        return pid

    def get(self, pid):
        if pid in getattr(self, "cancelled", set()):
            return "canceled", None, None
        return "succeeded", f"fake://{pid}", None

    def cancel(self, pid):
        self.cancelled = getattr(self, "cancelled", set()) | {pid}

    def download(self, url):
        size = self.preds[url.split("//")[1]]
        im = Image.new("RGB", size, (random.randint(0, 255), 120, 60))
        buf = io.BytesIO()
        im.save(buf, "PNG")
        return buf.getvalue()


def fake_ads(con):
    from google.ads.googleads.client import GoogleAdsClient
    from .gads import Ads, API_VERSION

    class FakeAds(Ads):
        def __init__(self, con):
            client = GoogleAdsClient(credentials=None, developer_token="x",
                                     use_proto_plus=True, version=API_VERSION)
            super().__init__(con, client)
            self.cid = "1234567890"
            self.n = 1000
            self.submitted = []

        def search(self, query, kind="search"):
            db.log_ops(self.con, 1, kind)
            return []

        def ensure_labels(self, names):
            known = db.kv_get(self.con, f"labels_{self.cid}", {})
            for n in names:
                known.setdefault(n, f"customers/{self.cid}/labels/{len(known) + 1}")
            db.kv_set(self.con, f"labels_{self.cid}", known)
            return known

        def upload_logos(self):
            return {"landscape": f"customers/{self.cid}/assets/1",
                    "square": f"customers/{self.cid}/assets/2"}

        def run_upload(self, wave, logos, quota_left):
            """Same unit selection as the real upload; results applied directly."""
            if db.open_jobs(self.con, "gads"):
                return -1
            if not self.has_work(wave) or quota_left < 50:
                return 0
            names = []
            for c in self.con.execute("SELECT * FROM campaigns WHERE wave=? AND "
                                      "status!='failed'", (wave,)).fetchall():
                names += self.wanted_labels(c)
            label_res = self.ensure_labels(names)
            total = 0
            for key, ops, idx in self.iter_units(wave, logos, label_res):
                if total + len(ops) + 20 > quota_left:
                    break
                with db.tx(self.con):
                    for op, e in zip(ops, idx):
                        field = op._pb.WhichOneof("operation")
                        assert field, "empty operation"
                        self.submitted.append(field)
                        self.n += 1
                        res = f"customers/{self.cid}/{e['t']}/{self.n}"
                        if e["t"] == "ad":
                            res = f"customers/{self.cid}/adGroupAds/1~{self.n}"
                        self.apply_result(e, res, None)
                    db.log_ops(self.con, len(ops), "batch_ops")
                total += len(ops)
            return total

        def remove_campaigns(self, camps):
            db.log_ops(self.con, len(camps), "remove")
            self.removed = getattr(self, "removed", []) + [c["campaign_res"] for c in camps]
            return {c["key"]: None for c in camps}, {}

        def enable_wave(self, wave):
            n = self.con.execute("UPDATE campaigns SET status='enabled' WHERE wave=? AND "
                                 "status='created'", (wave,)).rowcount
            db.log_ops(self.con, n, "enable")
            return n

        def sync(self, since):
            db.log_ops(self.con, 2, "sync")
            for r in self.con.execute("SELECT id FROM articles WHERE status='uploaded'"):
                db.update_article(self.con, r["id"], approval="APPROVED",
                                  impressions=random.randint(100, 2000),
                                  clicks=random.randint(0, 20))
            return 0

    return FakeAds(con)
