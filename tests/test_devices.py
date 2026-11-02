"""Phones and tablets only (Siddharth, 2 Nov 2026): desktop and TV screens opted out
(bid_modifier 0) on every campaign, pilot included, confirmed by reading Google back."""
import tempfile

from rda import livecheck
from tests.test_gads import _live_worker, setup


def upload(ads, logos, rounds=4):
    for _ in range(rounds):
        ads.run_upload(1, logos, 10000)


def camp(stub):
    return stub.ent["campaign"][0][1]


def test_new_campaign_gets_desktop_and_tv_created_at_zero_phones_tablets_untouched():
    with tempfile.TemporaryDirectory() as tmp:
        con, ads, stub, logos = setup(tmp)
        upload(ads, logos)
        assert ads.ensure_devices() == 1
        c = camp(stub)
        assert stub.dev[(c, "DESKTOP")][1] == 0 and stub.dev[(c, "CONNECTED_TV")][1] == 0
        assert (c, "MOBILE") not in stub.dev and (c, "TABLET") not in stub.dev
        assert con.execute("SELECT devices_ok FROM campaigns").fetchone()[0]
        n = stub.dev_calls
        assert ads.ensure_devices() == 0 and stub.dev_calls == n      # done once, not again


def test_existing_device_criteria_are_updated_with_an_explicit_mask():
    with tempfile.TemporaryDirectory() as tmp:
        con, ads, stub, logos = setup(tmp)
        stub.dev_implicit = True
        upload(ads, logos)
        assert ads.ensure_devices() == 1
        c = camp(stub)
        assert stub.dev[(c, "DESKTOP")][1] == 0 and stub.dev[(c, "CONNECTED_TV")][1] == 0
        assert stub.dev[(c, "MOBILE")][1] is None and stub.dev[(c, "TABLET")][1] is None


def test_change_google_ignores_is_never_counted_and_blocks_enable():
    with tempfile.TemporaryDirectory() as tmp:
        con, ads, stub, logos = setup(tmp)
        stub.dev_implicit = True
        upload(ads, logos)
        stub.mutate_campaign_criteria = lambda request: type(
            "R", (), {"results": [], "partial_failure_error": None})()   # "success", no change
        for _ in range(5):
            ads.ensure_devices()
        c = dict(con.execute("SELECT * FROM campaigns").fetchone())
        assert c["devices_ok"] is None and c["device_attempts"] == 3
        assert "not 0 in Google Ads" in c["device_error"]
        assert ads.enable_wave(1) == 0
        assert "desktop / TV screens not opted out" in con.execute(
            "SELECT error FROM campaigns").fetchone()[0]


def test_enabled_pilot_campaign_gets_the_opt_out_too():
    with tempfile.TemporaryDirectory() as tmp:
        con, ads, stub, logos = setup(tmp)
        upload(ads, logos)
        con.execute("UPDATE campaigns SET status='enabled'")
        assert ads.ensure_devices() == 1
        assert stub.dev[(camp(stub), "DESKTOP")][1] == 0


def test_enable_works_once_devices_are_confirmed():
    with tempfile.TemporaryDirectory() as tmp:
        con, ads, stub, logos = setup(tmp)
        upload(ads, logos)
        assert ads.enable_wave(1) == 0                                # not opted out yet
        ads.ensure_devices()
        assert ads.enable_wave(1) == 1


def test_live_check_flags_a_campaign_changed_back_in_google_but_not_pending_ones():
    with tempfile.TemporaryDirectory() as tmp:
        con, ads, stub, logos = setup(tmp)
        upload(ads, logos)
        w = _live_worker(con, ads)
        livecheck.run(w)
        line = next(x for x in w.logs if "duplicates / mismatches" in x)
        assert "OK" in line and "devices" in line                    # pending, not a failure
        ads.ensure_devices()
        stub.dev[(camp(stub), "DESKTOP")][1] = 1.0                    # someone changed it back
        w = _live_worker(con, ads)
        livecheck.run(w)
        line = next(x for x in w.logs if "duplicates / mismatches" in x)
        assert "FAIL" in line and "no longer opted out" in line
