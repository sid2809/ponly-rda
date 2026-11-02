"""Topic targeting (Siddharth, 2 Nov 2026): the 3 topics from his screenshot, mode Targeting,
on every ad group the pipeline creates except the live pilot (wave 1)."""
import json
import tempfile

from rda import db, livecheck
from tests import test_gads as tg
from tests.test_gads import TOPIC_RES, _live_worker, setup


def drive(ads, logos, rounds=6):
    wave = ads.con.execute("SELECT wave FROM campaigns").fetchone()[0]
    for _ in range(rounds):
        ads.run_upload(wave, logos, 10000)


def test_wave_2_ad_groups_get_the_three_topics_as_targeting_once():
    with tempfile.TemporaryDirectory() as tmp:
        con, ads, stub, logos = setup(tmp, wave=2)
        drive(ads, logos, rounds=4)
        assert {t for _, _, t in stub.ent["topic"]} == TOPIC_RES and len(stub.ent["topic"]) == 3
        ag = stub.ent["ag"][0][2]
        assert stub.ag_targeting[ag] == [("TOPIC", False)]          # Targeting, not Observation
        g = con.execute("SELECT * FROM adgroups").fetchone()
        assert set(json.loads(g["topics_res"])) == TOPIC_RES
        assert not ads.has_work(2)
        assert db.kv_get(con, "topic_constants") == {
            "Food & Drink / Beverages": "topicConstants/35",
            "Food & Drink / Food": "topicConstants/5",
            "Food & Drink / Cooking & Recipes": "topicConstants/122"}


def test_pilot_wave_1_is_left_without_topics():
    with tempfile.TemporaryDirectory() as tmp:
        con, ads, stub, logos = setup(tmp, wave=1)
        drive(ads, logos)
        assert stub.ent["topic"] == []
        assert stub.ag_targeting[stub.ent["ag"][0][2]] == []


def test_topic_not_found_in_google_stops_before_anything_is_uploaded():
    with tempfile.TemporaryDirectory() as tmp:
        con, ads, stub, logos = setup(tmp, wave=2)
        saved = dict(tg.TOPICS)
        tg.TOPICS.pop(122)                                           # Cooking & Recipes missing
        try:
            ads.run_upload(2, logos, 10000)
            assert False, "must stop"
        except RuntimeError as e:
            assert "Cooking & Recipes: 0 matches" in str(e)
        finally:
            tg.TOPICS.clear()
            tg.TOPICS.update(saved)
        assert stub.jobs == {} and stub.n == 0


def test_topic_matching_twice_stops_too():
    with tempfile.TemporaryDirectory() as tmp:
        con, ads, stub, logos = setup(tmp, wave=2)
        tg.TOPICS[999] = ["", "Food & Drink", "Food"]
        try:
            ads.run_upload(2, logos, 10000)
            assert False, "must stop"
        except RuntimeError as e:
            assert "Food & Drink / Food: 2 matches" in str(e)
        finally:
            tg.TOPICS.pop(999)
        assert stub.jobs == {}


def test_topics_failing_keep_the_campaign_paused():
    with tempfile.TemporaryDirectory() as tmp:
        con, ads, stub, logos = setup(tmp, wave=2,
                                      fail_types=["ad_group_criterion_operation"] * 15)
        con.execute("UPDATE adgroups SET alcohol=0")
        drive(ads, logos, rounds=12)
        g = con.execute("SELECT * FROM adgroups").fetchone()
        assert g["topic_attempts"] == 5 and not ads.has_work(2)
        assert con.execute("SELECT status FROM campaigns").fetchone()[0] == "held"
        assert ads.enable_wave(2) == 0


def test_enable_refused_when_topics_missing_in_account():
    with tempfile.TemporaryDirectory() as tmp:
        con, ads, stub, logos = setup(tmp, wave=2)
        drive(ads, logos)
        stub.ent["topic"].pop()                                      # one topic gone in Google Ads
        assert ads.enable_wave(2) == 0
        c = con.execute("SELECT * FROM campaigns").fetchone()
        assert c["status"] == "created" and "1 of 3 topics not found" in c["error"]


def test_enable_refused_for_an_ad_group_in_google_that_has_no_topics():
    with tempfile.TemporaryDirectory() as tmp:
        con, ads, stub, logos = setup(tmp, wave=2)
        drive(ads, logos)
        camp = stub.ent["campaign"][0][1]
        stub.ent["ag"].append((camp, "stray ad group", "customers/1/adGroups/7777"))
        assert ads.enable_wave(2) == 0
        assert "stray ad group" in con.execute("SELECT error FROM campaigns").fetchone()[0]


def test_enable_works_with_all_topics_in_place():
    with tempfile.TemporaryDirectory() as tmp:
        con, ads, stub, logos = setup(tmp, wave=2)
        drive(ads, logos)
        assert ads.enable_wave(2) == 1


def test_pilot_enable_does_not_need_topics():
    with tempfile.TemporaryDirectory() as tmp:
        con, ads, stub, logos = setup(tmp, wave=1)
        drive(ads, logos)
        assert ads.enable_wave(1) == 1


def test_lost_job_topics_are_adopted_not_added_again():
    with tempfile.TemporaryDirectory() as tmp:
        con, ads, stub, logos = setup(tmp, wave=2)
        drive(ads, logos)
        con.execute("UPDATE adgroups SET topics_res=NULL")          # results never recorded
        ads.reconcile(2)
        g = con.execute("SELECT * FROM adgroups").fetchone()
        assert set(json.loads(g["topics_res"])) == TOPIC_RES
        assert not ads.has_work(2) and len(stub.ent["topic"]) == 3


def test_missing_topic_on_an_existing_ad_group_is_added_alone():
    with tempfile.TemporaryDirectory() as tmp:
        con, ads, stub, logos = setup(tmp, wave=2)
        drive(ads, logos)
        g = con.execute("SELECT * FROM adgroups").fetchone()
        have = json.loads(g["topics_res"])
        gone = "topicConstants/5"
        stub.ent["topic"] = [x for x in stub.ent["topic"] if x[2] != gone]
        have.pop(gone)
        con.execute("UPDATE adgroups SET topics_res=?", (json.dumps(have),))
        n_ag = len(stub.ent["ag"])
        drive(ads, logos)
        assert {t for _, _, t in stub.ent["topic"]} == TOPIC_RES and len(stub.ent["topic"]) == 3
        assert len(stub.ent["ag"]) == n_ag                           # no new ad group


def test_live_check_reports_enabled_ad_group_missing_topics():
    with tempfile.TemporaryDirectory() as tmp:
        con, ads, stub, logos = setup(tmp, wave=2)
        drive(ads, logos)
        assert ads.enable_wave(2) == 1
        stub.ent["topic"].pop()
        w = _live_worker(con, ads)
        livecheck.run(w)
        line = next(x for x in w.logs if "duplicates / mismatches" in x)
        assert "FAIL" in line and "has 2 of 3 topics" in line
