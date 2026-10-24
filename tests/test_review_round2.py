import json
from replicate.exceptions import ReplicateError
from rda import config, db
from tests.test_worker_faults import tmp, make  # noqa: F401


def test_revoked_token_does_not_skip_the_whole_wave(tmp):
    """401 (token revoked/rotated) or 404 (model moved) is account-wide, not this article's
    fault. It must never burn image attempts and skip articles."""
    config.IMAGE_GAP_S = (0, 0)
    w = make(every=200)
    con = w.con
    for i in range(5):
        con.execute("INSERT INTO articles(id,url,title,category,status,wave,image_model,copy_json)"
                    " VALUES(?,?,?,?,?,?,?,?)", (f"{i}fffffffffff", "u", "T", "C", "copy_done", 2,
                                                 "minimax", json.dumps({"image_prompt": "p"})))
    def create(ref, inputs):
        raise ReplicateError(status=401, detail="Invalid token")
    w.rep.create = create
    w.settings = {"Image model": "minimax", "Pause": False}
    for _ in range(5):
        w.run_step("images:w2", w.step_images, 2, backoff=False)
    st = {r[0]: r[1] for r in con.execute("SELECT status, COUNT(*) FROM articles GROUP BY status")}
    assert "skipped" not in st, st
