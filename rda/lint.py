"""Code checks on Haiku ad copy. Returns a list of problems (empty = pass)."""
import re

CLICKBAIT = [
    "you won't believe", "you wont believe", "shocking", "click here", "doctors hate",
    "one weird trick", "secret", "miracle", "guaranteed", "#1", "number one", "best ever",
    "life-changing", "mind-blowing", "jaw-dropping", "what happens next", "act now",
    "limited time", "free", "100%", "never seen", "insane", "unbelievable", "hack",
]
_CAPS = re.compile(r"\b[A-Z]{2,}\b")
_REPEAT_PUNCT = re.compile(r"([!?.,])\1|[!?]{2,}")
_NUM = re.compile(r"\d+")


def check_copy(copy, title):
    p = []
    heads = copy.get("headlines") or []
    descs = copy.get("descriptions") or []
    long_h = copy.get("long_headline") or ""
    prompt = copy.get("image_prompt") or ""
    if len(heads) != 5:
        p.append(f"{len(heads)} headlines (need 5)")
    if len(descs) != 5:
        p.append(f"{len(descs)} descriptions (need 5)")
    if not prompt.strip():
        p.append("empty image prompt")
    for h in heads:
        if not h.strip() or len(h) > 30:
            p.append(f"headline length {len(h)}: {h!r}")
        if "!" in h:
            p.append(f"exclamation in headline: {h!r}")
    if not long_h.strip() or len(long_h) > 90:
        p.append(f"long headline length {len(long_h)}")
    for d in descs:
        if not d.strip() or len(d) > 90:
            p.append(f"description length {len(d)}: {d!r}")
    if len({h.strip().lower() for h in heads}) != len(heads):
        p.append("duplicate headlines")
    if len({d.strip().lower() for d in descs}) != len(descs):
        p.append("duplicate descriptions")
    title_words = set(re.findall(r"[A-Za-z]+", title))
    title_nums = set(_NUM.findall(title))
    for text in heads + descs + [long_h]:
        low = text.lower()
        for c in CLICKBAIT:
            if re.search(r"(?<![a-z])" + re.escape(c) + r"(?![a-z])", low) and c not in title.lower():
                p.append(f"clickbait phrase {c!r}")
        for w in _CAPS.findall(text):
            if w not in title_words:
                p.append(f"ALL-CAPS word {w!r}")
        if _REPEAT_PUNCT.search(text):
            p.append(f"repeated punctuation: {text!r}")
        for n in _NUM.findall(text):
            if n not in title_nums:
                p.append(f"number not in title: {n!r}")
    return p
