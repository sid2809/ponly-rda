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


def _even(n, k):
    base, extra = divmod(n, k)
    return [base + 1] * extra + [base] * (k - extra)


def campaign_sizes(n, lo=None, hi=None):
    """Split n articles into campaigns of lo-hi (from Settings; default 6-8).
    Counts that cannot be split exactly use the fewest campaigns that keep every one at
    least lo, letting a campaign go slightly over hi. With 6-8 that gives Siddharth's rules:
    under 6 -> one campaign; 9-11 -> one campaign; 17 -> 9 + 8."""
    lo = lo or config.CAMPAIGN_MIN
    hi = hi or config.CAMPAIGN_MAX
    if n <= 0:
        return []
    if n <= hi or n < lo:
        return [n]
    k = math.ceil(n / hi)
    if n // k >= lo:
        return _even(n, k)
    return _even(n, max(1, n // lo))     # no exact split: allow slightly over hi


def adgroup_split(themes, lo=None, hi=None):
    """Split a campaign's articles (sorted by theme) into ad groups of lo-hi (default 2-4),
    keeping same-theme articles together where possible. If no exact split exists
    (e.g. 5 with 3-4), one ad group is used when there are fewer than 2*lo articles,
    otherwise groups may go slightly over hi."""
    lo = lo or config.ADGROUP_MIN
    hi = hi or config.ADGROUP_MAX
    m = len(themes)
    if m == 0:
        return []
    if m < 2 * lo and not (lo <= m <= hi):
        return [m]
    for top in (hi, hi + lo - 1):
        INF = float("inf")
        best = [(INF, None)] * (m + 1)
        best[0] = (0, None)
        for i in range(1, m + 1):
            for size in range(lo, top + 1):
                j = i - size
                if j < 0 or best[j][0] == INF:
                    continue
                mixed = len(set(themes[j:i])) - 1       # theme changes inside the group
                cost = best[j][0] + mixed * 10 + 1       # avoid mixing first, then fewer groups
                if cost < best[i][0]:
                    best[i] = (cost, j)
        if best[m][0] < INF:
            sizes, i = [], m
            while i > 0:
                j = best[i][1]
                sizes.append(i - j)
                i = j
            return sizes[::-1]
    return [m]


def adgroup_sizes(m, lo=None, hi=None):
    """Sizes for m articles of one theme (used in tests and estimates)."""
    return adgroup_split(["t"] * m, lo, hi)


def chunk(items, sizes):
    out, i = [], 0
    for s in sizes:
        out.append(items[i:i + s])
        i += s
    return out


def assign_waves(articles, pilot_categories, camp_min=None):
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
        cmin = camp_min or config.CAMPAIGN_MIN
        if n2 < cmin:
            n2 = 0
        elif len(items) - n2 < cmin:
            n2 = len(items)
        for i, a in enumerate(items):
            out[a["id"]] = (2 if i < n2 else 3, None)
    return out


def plan_campaigns(articles, start_nn, sizes=None):
    """articles: passed articles of ONE wave (dicts with id, category, alcohol, theme,
    image_model). start_nn: {pool: next number}. Returns list of campaigns:
    {pool, image_model, nn, theme, name, articles, adgroups:[{nn, theme, name, articles}]}"""
    groups = {}
    for a in articles:
        groups.setdefault((pool_of(a), a["image_model"]), []).append(a)
    camps = []
    for (pool, model) in sorted(groups, key=lambda k: (k[0], k[1] or "")):
        items = sorted(groups[(pool, model)], key=lambda a: ((a["theme"] or "").lower(), a["id"]))
        cl, ch, gl, gh = sizes or (None, None, None, None)
        for c_items in chunk(items, campaign_sizes(len(items), cl, ch)):
            nn = start_nn.get(pool, 1)
            start_nn[pool] = nn + 1
            theme = Counter(a["theme"] for a in c_items).most_common(1)[0][0]
            name = f"{pool} | {theme} | {nn:02d} | {len(c_items)} articles"
            ags = []
            ag_sizes = adgroup_split([a["theme"] for a in c_items], gl, gh)
            for j, g in enumerate(chunk(c_items, ag_sizes), 1):
                gt = Counter(a["theme"] for a in g).most_common(1)[0][0]
                ags.append({"nn": j, "theme": gt, "articles": g,
                            "name": f"{gt} | AG{j:02d} | {len(g)} articles"})
            camps.append({"pool": pool, "image_model": model, "nn": nn, "theme": theme,
                          "name": name, "articles": c_items, "adgroups": ags})
    return camps
