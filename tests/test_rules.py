import io

from PIL import Image

from rda import alcohol, images, lint, split


def test_alcohol_test_set_keywords():
    for t in alcohol.MUST_TAG:
        assert alcohol.keyword_check(t)[0], t
    for t in alcohol.MUST_NOT_TAG:
        assert not alcohol.keyword_check(t)[0], t


def test_alcohol_whole_words():
    for t in ["Pale Kale Chips", "Portuguese Kale Soup", "Rump Roast", "Gingerbread Cookies",
              "Sale Day Salad", "Prime Rib"]:
        assert not alcohol.keyword_check(t)[0], t
    for t in ["Ginger Ale Glazed Ham", "Port Wine Sauce", "Bourbon-Glazed Salmon"]:
        assert alcohol.keyword_check(t)[0], t


GOOD = {"headlines": ["Chicken Pot Pie", "Creamy Pot Pie", "Golden Crust Pie",
                      "Classic Comfort Food", "Homemade Pot Pie"],
        "long_headline": "A golden, flaky chicken pot pie with a creamy vegetable filling",
        "descriptions": ["Tender chicken and vegetables in a creamy sauce under a flaky crust.",
                         "A cozy family dinner baked until golden.",
                         "Step-by-step recipe for a classic pot pie.",
                         "Make a comforting pot pie at home.",
                         "Creamy filling, buttery crust, simple steps."],
        "image_prompt": "A golden chicken pot pie on a wooden table."}


def test_lint_good():
    assert lint.check_copy(GOOD, "Classic Chicken Pot Pie") == []


def test_lint_catches():
    bad = dict(GOOD, headlines=["THE BEST Pot Pie Ever Made Here!!", "x", "x", "y", "z"])
    p = " | ".join(lint.check_copy(bad, "Classic Chicken Pot Pie"))
    assert "headline length" in p and "ALL-CAPS" in p and "duplicate" in p
    assert "repeated punctuation" in p and "exclamation" in p
    num = dict(GOOD, long_headline="Ready in 30 minutes")
    assert any("number" in x for x in lint.check_copy(num, "Classic Chicken Pot Pie"))
    gf = dict(GOOD, long_headline="A gluten free fritter")
    assert lint.check_copy(gf, "Gluten Free Zucchini Fritters") == []
    caps = dict(GOOD, long_headline="Smoky BBQ meatballs in sauce")
    assert lint.check_copy(caps, "Mini BBQ Meatballs") == []


def test_campaign_sizes():
    assert split.campaign_sizes(5) == [5]
    assert split.campaign_sizes(9) == [9] and split.campaign_sizes(11) == [11]
    assert split.campaign_sizes(17) == [9, 8]
    assert split.campaign_sizes(12) == [6, 6]
    for n in list(range(6, 9)) + list(range(12, 17)) + list(range(18, 900)):
        s = split.campaign_sizes(n)
        assert sum(s) == n and all(6 <= x <= 8 for x in s), (n, s)


def test_adgroup_split():
    for m in range(2, 12):
        s = split.adgroup_sizes(m)
        assert sum(s) == m and all(2 <= x <= 4 for x in s), (m, s)
    assert split.adgroup_sizes(1) == [1]
    # keeps themes together: 3 A + 3 B -> 3 + 3, not 4 + 2
    assert split.adgroup_split(["A"] * 3 + ["B"] * 3) == [3, 3]
    assert split.adgroup_split(["A"] * 2 + ["B"] * 2 + ["C"] * 4) == [2, 2, 4]


def test_crops_any_size():
    for size in [(1280, 720), (1344, 768), (1024, 1024), (720, 1280)]:
        im = Image.new("RGB", size, (200, 100, 50))
        b = io.BytesIO(); im.save(b, "PNG")
        land, sq = images.crop_pair(b.getvalue())
        assert Image.open(io.BytesIO(land)).size == (1200, 628)
        assert Image.open(io.BytesIO(sq)).size == (720, 720)


def test_pretty_category():
    assert split.pretty_category("soups-stews-chili") == "Soups, Stews & Chili"
    assert split.pretty_category("recipes") == "Recipes"
    assert split.pretty_category("chicken-poultry") == "Chicken & Poultry"


def test_assign_waves_pilot_balanced():
    arts = []
    for c in ["A", "B", "C", "D", "E"]:
        for i in range(100):
            arts.append({"id": f"{c}{i:03d}", "category": c, "alcohol": 0, "theme": f"T{i % 7}"})
    for i in range(50):
        arts.append({"id": f"X{i:03d}", "category": "A", "alcohol": 1, "theme": "Drinks"})
    plan = split.assign_waves(arts, {"A", "B", "C", "D"})
    pilot = [k for k, v in plan.items() if v[0] == 1]
    assert len(pilot) == 100
    sides = [v[1] for v in plan.values() if v[0] == 1]
    assert sides.count("A") == sides.count("B") == 50
    w2 = sum(1 for v in plan.values() if v[0] == 2)
    assert 880 <= w2 <= 920 or w2 == len(arts) - 100


def test_small_pools_not_split_across_waves():
    arts = [{"id": f"R{i}", "category": "Recipes", "alcohol": 0, "theme": "T"} for i in range(9)]
    arts += [{"id": f"B{i:04d}", "category": "Big", "alcohol": 0, "theme": "T"} for i in range(3000)]
    plan = split.assign_waves(arts, set())
    waves = {plan[f"R{i}"][0] for i in range(9)}
    assert len(waves) == 1


def test_cocktail_dishes_decided_food():
    for t in ["Classic Shrimp Cocktail", "Avocado and Shrimp Cocktail Cups",
              "Tropical Breeze Fruit Cocktail Cake"]:
        hit, why = alcohol.keyword_check(t)
        assert hit is False and "decision" in why, t
    assert alcohol.keyword_check("Bourbon Shrimp Cocktail")[0] is True
    assert alcohol.keyword_check("Kale Salad")[0] is None
