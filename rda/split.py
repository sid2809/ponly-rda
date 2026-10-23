"""Wave assignment and campaign/ad-group split (pure functions, unit-tested)."""
import math
from collections import Counter

from . import config

ALCOHOL_POOL = "ALCOHOL"


CATEGORY_NAMES = {
    "vegetables-side-dishes": "Vegetables & Side Dishes",
    "dips-sauces-pantry-staples": "Dips, Sauces & Pantry Staples",
    "stir-fries-curries-bowls": "Stir-Fries, Curries & Bowls",
}


def pretty_category(slug):
    """'soups-stews-chili' -> 'Soups, Stews & Chili'."""
    if slug in CATEGORY_NAMES:
        return CATEGORY_NAMES[slug]
    parts = [w.capitalize() for w in slug.replace("_", "-").split("-") if w]
    if len(parts) <= 1:
        return "".join(parts)
    return ", ".join(parts[:-1]) + " & " + parts[-1]


def pool_of(a):
    return ALCOHOL_POOL if a["alcohol"] else a["category"]


def campaign_sizes(n):
    """8-12 per campaign; 13-15 -> one campaign; under 8 -> one campaign (Siddharth)."""
    if n <= 0:
        return []
    lo, hi = config.POOL_ONE_CAMPAIGN_RANGE
    if n < config.CAMPAIGN_MIN or lo <= n <= hi or n <= config.CAMPAIGN_MAX:
        return [n]
    k = math.ceil(n / config.CAMPAIGN_MAX)
    base, extra = divmod(n, k)
    sizes = [base + 1] * extra + [base] * (k - extra)
    assert all(config.CAMPAIGN_MIN <= s <= config.CAMPAIGN_MAX for s in sizes), (n, sizes)
    return sizes


def adgroup_sizes(m):
    """3-4 per ad group; 1, 2 or 5 articles -> one ad group (Siddharth, 22 Oct)."""
    if m in (1, 2, 5) or m <= 0:
        return [m] if m > 0 else []
    if m in (3, 4):
        return [m]
    g = math.ceil(m / 4)
    base, extra = divmod(m, g)
    sizes = [base + 1] * extra + [base] * (g - extra)
    assert all(3 <= s <= 4 for s in sizes), (m, sizes)
    return sizes


def chunk(items, sizes):
    out, i = [], 0
    for s in sizes:
        out.append(items[i:i + s])
        i += s
    return out


def assign_waves(articles, pilot_categories):
    """articles: dicts with id, category, alcohol, theme. Returns {id: (wave, side)}.
    Pilot: 20 per pilot category + 20 alcohol, alternating A/B so both image models
    get the same theme mix. Wave 2: ~900 spread over pools in proportion. Rest: wave 3."""
    pools = {}
    for a in articles:
        pools.setdefault(pool_of(a), []).append(a)
    for p in pools.values():
        p.sort(key=lambda a: ((a["theme"] or "").lower(), a["id"]))
    out = {}
    for pool, items in pools.items():
        take = 0
        if pool in pilot_categories:
            take = config.PILOT_PER_CATEGORY
        elif pool == ALCOHOL_POOL:
            take = config.PILOT_ALCOHOL
        for i, a in enumerate(items[:take]):
            out[a["id"]] = (1, "A" if i % 2 == 0 else "B")
        pools[pool] = items[take:]
    remaining = sum(len(v) for v in pools.values())
    for pool, items in pools.items():
        n2 = round(len(items) * config.WAVE2_SIZE / remaining) if remaining else 0
        # never leave a pool piece too small to make a normal campaign in either wave
        if n2 < config.CAMPAIGN_MIN:
            n2 = 0
        elif len(items) - n2 < config.CAMPAIGN_MIN:
            n2 = len(items)
        for i, a in enumerate(items):
            out[a["id"]] = (2 if i < n2 else 3, None)
    return out


def plan_campaigns(articles, start_nn):
    """articles: passed articles of ONE wave (dicts with id, category, alcohol, theme,
    image_model). start_nn: {pool: next number}. Returns list of campaigns:
    {pool, image_model, nn, theme, name, articles, adgroups:[{nn, theme, name, articles}]}"""
    groups = {}
    for a in articles:
        groups.setdefault((pool_of(a), a["image_model"]), []).append(a)
    camps = []
    for (pool, model) in sorted(groups, key=lambda k: (k[0], k[1] or "")):
        items = sorted(groups[(pool, model)], key=lambda a: ((a["theme"] or "").lower(), a["id"]))
        for c_items in chunk(items, campaign_sizes(len(items))):
            nn = start_nn.get(pool, 1)
            start_nn[pool] = nn + 1
            theme = Counter(a["theme"] for a in c_items).most_common(1)[0][0]
            name = f"{pool} | {theme} | {nn:02d} | {len(c_items)} articles"
            ags = []
            for j, g in enumerate(chunk(c_items, adgroup_sizes(len(c_items))), 1):
                gt = Counter(a["theme"] for a in g).most_common(1)[0][0]
                ags.append({"nn": j, "theme": gt, "articles": g,
                            "name": f"{gt} | AG{j:02d} | {len(g)} articles"})
            camps.append({"pool": pool, "image_model": model, "nn": nn, "theme": theme,
                          "name": name, "articles": c_items, "adgroups": ags})
    return camps
