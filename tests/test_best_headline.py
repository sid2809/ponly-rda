"""'Best [dish] Recipe YEAR' headlines (Siddharth, 2 Nov 2026): allowed only in that exact
form, dish words from the title, current year, skipped by Haiku when it does not fit; wave 2
copy written again once without touching images or statuses."""
import json

from rda import db, fakes, lint, llm
from tests.test_worker_faults import tmp, make  # noqa: F401

GOOD = {"headlines": ["Spiced Pumpkin Gumbo", "Pumpkin Gumbo Recipe",
                      "Best Pumpkin Gumbo Recipe 2026", "Make Pumpkin Gumbo", "Gumbo With Spice"],
        "long_headline": "Spiced pumpkin gumbo recipe for your kitchen",
        "descriptions": ["Get the spiced pumpkin gumbo recipe.", "See how to make pumpkin gumbo.",
                         "You can make spiced pumpkin gumbo.", "Spiced pumpkin gumbo, step by step.",
                         "Pumpkin gumbo recipe."],
        "image_prompt": "p"}
T = "Spiced Pumpkin Gumbo"


def copy_with(**kw):
    c = json.loads(json.dumps(GOOD))
    for k, v in kw.items():
        idx, text = v
        if k == "long_headline":
            c[k] = text
        else:
            c[k][idx] = text
    return c


def test_pattern_headline_passes():
    assert lint.check_copy(GOOD, T, year=2026) == []


def test_pattern_needs_title_words_only():
    p = lint.check_copy(copy_with(headlines=(2, "Best Easy Gumbo Recipe 2026")), T, year=2026)
    assert any("'best' only" in x for x in p) and any("2026" in x for x in p)


def test_short_form_best_dish_year_passes():
    assert lint.check_copy(copy_with(headlines=(2, "Best Pumpkin Gumbo 2026")), T, year=2026) == []
    assert lint.check_copy(copy_with(headlines=(2, "Best Spiced Gumbo 2026")), T, year=2026) == []
    p = lint.check_copy(copy_with(headlines=(2, "Best Easy Gumbo 2026")), T, year=2026)
    assert any("'best' only" in x for x in p)                 # 'Easy' is not in the title
    assert lint.check_copy(copy_with(headlines=(2, "Best Pumpkin Gumbo 2025")), T, year=2026)


def test_both_forms_in_one_ad():
    c = copy_with(headlines=(3, "Best Pumpkin Gumbo 2026"))
    assert lint.check_copy(c, T, year=2026) == []


def test_best_or_year_anywhere_else_fails():
    assert lint.check_copy(copy_with(headlines=(2, "2026 Best Pumpkin Gumbo")), T, year=2026)
    assert lint.check_copy(copy_with(headlines=(2, "Best Pumpkin Gumbo Dish 2026")), T,
                           year=2026)                    # 'Dish' not in the title
    assert lint.check_copy(copy_with(headlines=(2, "Best Pumpkin Gumbo")), T, year=2026)
    assert lint.check_copy(copy_with(descriptions=(0, "The best pumpkin gumbo recipe.")), T,
                           year=2026)
    assert lint.check_copy(copy_with(long_headline=(0, "Best pumpkin gumbo recipe 2026")), T,
                           year=2026)


def test_only_the_current_year():
    p = lint.check_copy(copy_with(headlines=(2, "Best Pumpkin Gumbo Recipe 2025")), T, year=2026)
    assert p
    assert lint.check_copy(copy_with(headlines=(2, "Best Pumpkin Gumbo Recipe 2027")), T,
                           year=2027) == []


def test_other_rules_unchanged():
    assert lint.check_copy(copy_with(headlines=(2, "Best Ever Pumpkin Gumbo")), T, year=2026)
    assert lint.check_copy(copy_with(headlines=(2, "BEST Pumpkin Gumbo Recipe 2026")), T,
                           year=2026)                    # ALL-CAPS still blocked


def test_best_in_title_still_allowed_like_before():
    title = "Best Pumpkin Gumbo"
    c = copy_with(headlines=(0, "Best Pumpkin Gumbo"))
    assert lint.check_copy(c, title, year=2026) == []


def test_prompt_explains_the_pattern_with_the_year():
    text = llm.copy_params(T, "Soups", "Theme", year=2026)["messages"][0]["content"]
    assert '"Best [dish] Recipe 2026"' in text and '"Best [dish] 2026"' in text
    assert "fits in 30 characters" in text


def _wave2(w, statuses):
    old = json.dumps(GOOD | {"headlines": ["Old " + h for h in GOOD["headlines"][:5]]})
    for i, s in enumerate(statuses):
        w.con.execute("INSERT INTO articles(id,url,title,category,theme,status,wave,image_model,"
                      "copy_json) VALUES(?,?,?,?,?,?,?,?,?)",
                      (f"{i:012x}", "u", T, "Soups", "Th", s, 2, "minimax", old))
    return old


class PatternClaude(fakes.FakeClaude):
    """Answers every copy request with GOOD (which uses the pattern headline)."""

    def __init__(self):
        super().__init__(fail_rate=0)

    def _answer(self, params):
        if "headlines" in params["output_config"]["format"]["schema"]["properties"]:
            return json.loads(json.dumps(GOOD))
        return super()._answer(params)


def test_recopy_replaces_copy_and_never_touches_status(tmp, monkeypatch):
    monkeypatch.setattr(lint, "current_year", lambda: 2026)
    w = make(every=200, claude=PatternClaude())
    statuses = ["copy_done", "image_pending", "image_done", "check_pending", "checked"]
    old = _wave2(w, statuses)
    for _ in range(3):
        w.step_recopy(2)
    st = db.kv_get(w.con, "recopy_2")
    assert st["done"] and st["replaced"] == 5 and st["kept"] == 0
    rows = w.con.execute("SELECT status, copy_json FROM articles WHERE wave=2 ORDER BY id").fetchall()
    assert [r["status"] for r in rows] == statuses
    assert all(r["copy_json"] != old and "Best Pumpkin Gumbo Recipe 2026" in r["copy_json"]
               for r in rows)


def test_copy_failing_twice_keeps_the_old_copy(tmp, monkeypatch):
    monkeypatch.setattr(lint, "current_year", lambda: 2026)
    w = make(every=200, claude=PatternClaude())
    old = _wave2(w, ["image_done", "image_done"])
    real_create = w.claude.create_batch

    def create(reqs):                      # article 0 always gets copy that fails the checks
        bid = real_create(reqs)
        for cid, _ in reqs:
            if cid.endswith("-000000000000"):
                w.claude.batches[bid][cid] = ("ok", copy_with(descriptions=(0, "The best gumbo.")))
        return bid
    w.claude.create_batch = create
    for _ in range(4):
        w.step_recopy(2)
    st = db.kv_get(w.con, "recopy_2")
    assert st["done"] and st["replaced"] == 1 and st["kept"] == 1
    a = w.con.execute("SELECT copy_json, status FROM articles WHERE id='000000000000'").fetchone()
    assert a["copy_json"] == old and a["status"] == "image_done"


def test_plan_waits_for_recopy_and_recopy_waits_for_first_copy(tmp):
    w = make(every=200, claude=PatternClaude())
    _wave2(w, ["checked", "themed"])
    db.kv_set(w.con, "planned_2", False)
    w.step_recopy(2)
    assert db.kv_get(w.con, "recopy_2") is None          # first copy round still running
    w.step_plan(2)
    assert not db.kv_get(w.con, "planned_2")


def test_recopy_not_run_when_wave_2_already_planned(tmp):
    w = make(every=200, claude=PatternClaude())
    _wave2(w, ["planned"])
    db.kv_set(w.con, "planned_2", True)
    w.step_recopy(2)
    assert db.kv_get(w.con, "recopy_2")["done"] is True
    assert "skipped" in db.kv_get(w.con, "recopy_2")
