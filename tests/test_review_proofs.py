"""Proof tests from the code review (24 Oct). Each asserts the correct behaviour."""
import json
import time

import pytest
from rda import config, db, images, llm
from tests.test_worker_faults import tmp, make  # noqa: F401


def _wave_article(con, aid, status, **kw):
    cols = dict(id=aid, url="https://ponly.com/x", final_url="https://ponly.com/x", title="T",
                category="C", status=status, wave=2, image_model="minimax",
                copy_json=json.dumps({"image_prompt": "p"}))
    cols.update(kw)
    con.execute(f"INSERT INTO articles({','.join(cols)}) VALUES({','.join('?'*len(cols))})",
                tuple(cols.values()))


def test_crash_between_prediction_done_and_article_update_recovers(tmp):
    """poll_predictions: 'UPDATE predictions SET state=done' and the article update are two
    autocommit statements. A crash in between must not leave the article stuck."""
    w = make(every=200)
    con = w.con
    _wave_article(con, "aaaaaaaaaaaa", "copy_done")
    a = con.execute("SELECT * FROM articles WHERE id='aaaaaaaaaaaa'").fetchone()
    images.start_prediction(con, w.rep, a, "minimax")
    real = db.update_article
    def crash(con_, aid, **f):
        if f.get("status") == "image_done":
            raise SystemExit("killed")          # SIGKILL stand-in
        return real(con_, aid, **f)
    db.update_article = crash
    try:
        with pytest.raises(SystemExit):
            images.poll_predictions(con, w.rep)
    finally:
        db.update_article = real
    # restart: recovery + polling again
    w.recover()
    images.poll_predictions(con, w.rep)
    st = con.execute("SELECT status FROM articles WHERE id='aaaaaaaaaaaa'").fetchone()[0]
    open_preds = con.execute("SELECT COUNT(*) FROM predictions WHERE state NOT IN ('done','failed')").fetchone()[0]
    assert st != "image_pending" or open_preds, f"stuck: status={st}, open predictions={open_preds}"


def test_adopt_does_not_take_a_batch_owned_by_another_job(tmp):
    """Check chunks are 100 requests each, created seconds apart. A crash during the create of
    chunk 2 must not adopt chunk 1's batch (already owned by job 1)."""
    w = make(every=200)
    con = w.con
    ids = []
    for i in range(4):
        aid = f"{i}bbbbbbbbbbb"
        _wave_article(con, aid, "image_done")
        ids.append(aid)
    reqs1 = [(f"chk-{a}-1", llm.alcohol_params("x")) for a in ids[:2]]
    w._submit("check", 2, reqs1)                     # chunk 1: fine
    bid1 = con.execute("SELECT external_id FROM jobs WHERE state='running'").fetchone()[0]
    # chunk 2: process dies inside create_batch, before the request reached Anthropic
    db.new_job(con, "check", 2, payload={"ids": ids[2:], "cids": [f"chk-{a}-1" for a in ids[2:]]},
               state="creating")
    w.claude.find_batch = lambda after, before, n: [bid1]   # real find_batch: same window, same size
    w.recover()
    owners = con.execute("SELECT id, state FROM jobs WHERE external_id=?", (bid1,)).fetchall()
    assert len(owners) == 1, [tuple(o) for o in owners]


def test_create_batch_error_after_server_created_is_not_paid_twice(tmp):
    """create_batch raises (read timeout) although Anthropic created the batch. The job is
    marked 'abandoned' at once, so recovery never looks for it and the articles are
    submitted again: the same requests are paid twice."""
    w = make(every=200)
    con = w.con
    aid = "cccccccccccc"
    lp, sp = images.save_crops(aid, w.rep.download("fake://" + w.rep.create("minimax", {})))
    _wave_article(con, aid, "image_done", landscape_path=lp, square_path=sp)
    real = w.claude.create_batch
    def timeout_after_create(reqs):
        real(reqs)
        raise TimeoutError("read timeout")
    w.claude.create_batch = timeout_after_create
    with pytest.raises(TimeoutError):
        w._submit("check", 2, [(f"chk-{aid}-1", llm.check_params("T", b"x", b"y"))])
    w.claude.create_batch = real
    w.claude.find_batch = lambda after, before, n: list(w.claude.batches)  # it IS findable
    w.recover()
    w.step_check(2)
    assert len(w.claude.batches) == 1, f"{len(w.claude.batches)} paid batches for one article"


def test_replicate_create_error_does_not_block_wave_forever(tmp):
    """A create-time error that always happens for one article (e.g. 422 invalid input) is
    never charged to that article: it stays copy_done, every loop retries it first, and the
    step backs off. Images of the rest of the wave and the article's final state never come."""
    config.IMAGE_GAP_S = (0, 0)
    w = make(every=200)
    con = w.con
    _wave_article(con, "dddddddddddd", "copy_done")
    _wave_article(con, "eeeeeeeeeeee", "copy_done")
    real = w.rep.create
    def create(ref, inputs):
        if "BAD" in inputs["prompt"]:
            from replicate.exceptions import ReplicateError
            raise ReplicateError(status=422, detail="input.prompt invalid")
        return real(ref, inputs)
    con.execute("UPDATE articles SET copy_json=? WHERE id='dddddddddddd'",
                (json.dumps({"image_prompt": "BAD"}),))
    w.rep.create = create
    w.settings = {"Image model": "minimax", "Pause": False}
    for _ in range(10):
        w.run_step("images:w2", w.step_images, 2, backoff=False)
    st = dict(con.execute("SELECT id, status FROM articles").fetchall())
    assert st["dddddddddddd"] in ("skipped", "error"), st


def test_rate_limit_at_start_is_not_charged_to_the_article(tmp):
    """429 from Replicate stays a step-wide pause: no attempt is used up."""
    config.IMAGE_GAP_S = (0, 0)
    w = make(every=200)
    con = w.con
    _wave_article(con, "ffffffffffff", "copy_done")

    def create(ref, inputs):
        from replicate.exceptions import ReplicateError
        raise ReplicateError(status=429, detail="throttled")
    w.rep.create = create
    w.settings = {"Image model": "minimax", "Pause": False}
    w.run_step("images:w2", w.step_images, 2, backoff=False)
    a = con.execute("SELECT status, image_attempts FROM articles WHERE id='ffffffffffff'").fetchone()
    assert (a["status"], a["image_attempts"]) == ("copy_done", 0)


def test_finished_image_is_collected_even_after_30_minutes(tmp):
    """An image that already succeeded is never cancelled and paid again."""
    w = make(every=200)
    con = w.con
    _wave_article(con, "gggggggggggg", "copy_done")
    a = con.execute("SELECT * FROM articles WHERE id='gggggggggggg'").fetchone()
    pid = images.start_prediction(con, w.rep, a, "minimax")
    con.execute("UPDATE predictions SET created=? WHERE id=?", (time.time() - 7200, pid))
    images.poll_predictions(con, w.rep)
    assert con.execute("SELECT status FROM articles WHERE id='gggggggggggg'").fetchone()[0] == \
        "image_done"


def test_disk_nearly_full_starts_no_paid_image(tmp):
    config.IMAGE_GAP_S = (0, 0)
    w = make(every=200)
    con = w.con
    _wave_article(con, "hhhhhhhhhhhh", "copy_done")
    old = config.MIN_FREE_DISK_BYTES
    config.MIN_FREE_DISK_BYTES = 10 ** 18
    try:
        w.settings = {"Image model": "minimax", "Pause": False}
        w.run_step("images:w2", w.step_images, 2, backoff=False)
    finally:
        config.MIN_FREE_DISK_BYTES = old
    assert con.execute("SELECT COUNT(*) FROM predictions").fetchone()[0] == 0
    assert "disk nearly full" in db.kv_get(con, "step_errors")["images:w2"]["error"]


def test_pause_fails_closed(tmp):
    w = make(every=200)
    for v, paused in ((False, False), ("FALSE", False), (True, True), ("yes", True),
                      ("1", True), ("", True)):
        w.settings = {"Pause": v}
        assert w.paused() is paused, v


def test_skip_log_printed_only_after_commit(tmp, capsys):
    w = make(every=200)
    con = w.con
    _wave_article(con, "iiiiiiiiiiii", "copy_done")
    try:
        with db.tx(con):
            db.update_article(con, "iiiiiiiiiiii", status="skipped", skip_reason="x")
            raise RuntimeError("rolled back")
    except RuntimeError:
        pass
    assert "article SKIPPED" not in capsys.readouterr().out
    with db.tx(con):
        db.update_article(con, "iiiiiiiiiiii", status="skipped", skip_reason="y")
    assert "article SKIPPED" in capsys.readouterr().out


def test_account_wide_refusal_never_skips_the_wave(tmp):
    """401/403/404 (token, account, model) pause the step; no article loses an attempt."""
    config.IMAGE_GAP_S = (0, 0)
    w = make(every=200)
    con = w.con
    for i in range(5):
        _wave_article(con, f"{i}jjjjjjjjjjj", "copy_done")

    def create(ref, inputs):
        from replicate.exceptions import ReplicateError
        raise ReplicateError(status=401, detail="invalid token")
    w.rep.create = create
    w.settings = {"Image model": "minimax", "Pause": False}
    for _ in range(5):
        w.run_step("images:w2", w.step_images, 2, backoff=False)
    rows = con.execute("SELECT status, image_attempts FROM articles WHERE id LIKE '%jjj'").fetchall()
    assert all((r["status"], r["image_attempts"]) == ("copy_done", 0) for r in rows)


def test_many_per_request_refusals_stop_the_step(tmp):
    """Even 422s: 3 in a row with nothing started stops the step (protects the wave)."""
    config.IMAGE_GAP_S = (0, 0)
    w = make(every=200)
    con = w.con
    for i in range(6):
        _wave_article(con, f"{i}kkkkkkkkkkk", "copy_done")

    def create(ref, inputs):
        from replicate.exceptions import ReplicateError
        raise ReplicateError(status=422, detail="bad input")
    w.rep.create = create
    w.settings = {"Image model": "minimax", "Pause": False}
    w.run_step("images:w2", w.step_images, 2, backoff=False)
    used = con.execute("SELECT SUM(image_attempts) FROM articles WHERE id LIKE '%kkk'").fetchone()[0]
    assert used == 2 and "in a row refused" in db.kv_get(con, "step_errors")["images:w2"]["error"]


def test_sheets_429_is_not_a_google_ads_quota_wait(tmp):
    from rda.gads import is_quota_error
    assert not is_quota_error(RuntimeError("HttpError 429 RESOURCE_EXHAUSTED (Sheets)"))
