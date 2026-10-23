"""Claude Haiku 5.5 calls: themes (sync), copy / image check / alcohol (Batch API, 50% off).
Haiku 5.5 rules respected: no temperature, no prefill; JSON via output_config.format."""
import base64
import json

from . import config

COPY_SCHEMA = {
    "type": "object",
    "properties": {
        "headlines": {"type": "array", "items": {"type": "string"}},
        "long_headline": {"type": "string"},
        "descriptions": {"type": "array", "items": {"type": "string"}},
        "image_prompt": {"type": "string"},
    },
    "required": ["headlines", "long_headline", "descriptions", "image_prompt"],
    "additionalProperties": False,
}
CHECK_SCHEMA = {
    "type": "object",
    "properties": {"pass": {"type": "boolean"},
                   "problems": {"type": "array", "items": {"type": "string"}}},
    "required": ["pass", "problems"], "additionalProperties": False,
}
ALCOHOL_SCHEMA = {
    "type": "object",
    "properties": {"alcohol": {"type": "boolean"}, "reason": {"type": "string"}},
    "required": ["alcohol", "reason"], "additionalProperties": False,
}
THEMES_SCHEMA = {
    "type": "object",
    "properties": {"themes": {"type": "array", "items": {"type": "string"}}},
    "required": ["themes"], "additionalProperties": False,
}
ASSIGN_SCHEMA = {   # compact: article number -> theme number (short output, no truncation)
    "type": "object",
    "properties": {"a": {"type": "array", "items": {
        "type": "array", "items": {"type": "integer"}}}},
    "required": ["a"], "additionalProperties": False,
}


def _params(content, schema, max_tokens):
    return {"model": config.HAIKU_MODEL, "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": content}],
            "output_config": {"format": {"type": "json_schema", "schema": schema}}}


def copy_params(title, category, theme, problems=None, big=False):
    fix = ""
    if problems:
        fix = ("\nYour previous attempt failed these checks; fix them:\n- "
               + "\n- ".join(problems[:10]))
    text = f"""Write Google responsive display ad copy (US English) for this recipe article.
Title: {title}
Category: {category}
Theme: {theme}

Rules:
- headlines: exactly 5, each at most 30 characters including spaces, all different.
- long_headline: at most 90 characters.
- descriptions: exactly 5, each at most 90 characters, all different.
- Describe only what the title says. No numbers unless they are in the title. No prices,
  times, ratings, health claims, superlatives (best, #1), urgency, or clickbait.
- No ALL-CAPS words, no exclamation marks in headlines, no repeated punctuation.
- image_prompt: one sentence describing a photorealistic photo of this exact dish,
  plated, natural light. No text, no logos, no people, no hands. Dish centred.{fix}"""
    return _params(text, COPY_SCHEMA, 16000 if big else 8000)


def check_params(title, landscape_bytes, square_bytes, big=False):
    def img(b):
        return {"type": "image", "source": {"type": "base64", "media_type": "image/jpeg",
                                            "data": base64.b64encode(b).decode()}}
    text = f"""These two images (wide and square crop) will be used in a display ad for the
recipe "{title}". Fail them if EITHER image has any of: visible text, letters or numbers;
a watermark; a logo or brand; a real person, face, body part or hands; adult, violent or
disturbing content; a collage or split image; heavy blur or distortion; or a dish that does
not match the title. Otherwise pass. List each problem briefly."""
    return _params([img(landscape_bytes), img(square_bytes), {"type": "text", "text": text}],
                   CHECK_SCHEMA, 8000 if big else 4000)


def alcohol_params(title, big=False):
    text = f"""Recipe title: "{title}"
Answer alcohol=true if the title names an alcoholic drink (including cocktail names such as
Mai Tai or Cosmopolitan), names an alcoholic ingredient, or is a drink whose alcohol content
is unclear (punch, cider, eggnog, toddy). Words used only as style descriptions (e.g.
"Old Fashioned" meaning traditional) do not count. Dishes count only if alcohol is named in
the title. If genuinely ambiguous, answer true."""
    return _params(text, ALCOHOL_SCHEMA, 8000 if big else 4000)


class LLMError(Exception):
    """kind: 'api' (transport, rate limit, overload, bad JSON, expired/errored batch entry),
    'refusal' (model refused: a content verdict), 'max_tokens' (answer cut off)."""

    def __init__(self, kind, msg):
        super().__init__(f"{kind}: {msg}")
        self.kind = kind
        self.msg = str(msg)[:300]


def _parse_message(msg):
    """Message -> parsed JSON, or raise LLMError by stop_reason (R1/P4)."""
    if msg.stop_reason == "refusal":
        raise LLMError("refusal", "model refused")
    if msg.stop_reason == "max_tokens":
        raise LLMError("max_tokens", "answer cut off at max_tokens")
    if msg.stop_reason != "end_turn":
        raise LLMError("api", f"stop_reason={msg.stop_reason}")
    try:
        text = next(b.text for b in msg.content if b.type == "text")
        return json.loads(text)
    except Exception as e:
        raise LLMError("api", f"bad JSON: {e}")


class Claude:
    def __init__(self):
        import anthropic
        # SDK retries 408/409/429/5xx (incl. 529 overloaded) with backoff
        self.c = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY, max_retries=6)

    def sync_json(self, params):
        """Raises LLMError. Streaming: the SDK refuses large max_tokens without it."""
        try:
            with self.c.messages.stream(**params) as stream:
                r = stream.get_final_message()
        except LLMError:
            raise
        except Exception as e:
            raise LLMError("api", f"{type(e).__name__}: {e}")
        return _parse_message(r)

    def create_batch(self, requests):
        """requests: list of (custom_id, params). Returns batch id."""
        b = self.c.messages.batches.create(
            requests=[{"custom_id": cid, "params": p} for cid, p in requests])
        return b.id

    def batch_ended(self, batch_id):
        return self.c.messages.batches.retrieve(batch_id).processing_status == "ended"

    def find_batch(self, created_after, created_before, n_requests):
        """Batches created in the window with exactly n requests (R5/R19/R21)."""
        out = []
        for b in self.c.messages.batches.list(limit=50):
            ts = b.created_at.timestamp()
            if ts < created_after - 3600:     # newest first: nothing older can match
                break
            rc = b.request_counts
            total = rc.processing + rc.succeeded + rc.errored + rc.canceled + rc.expired
            if created_after <= ts <= created_before and total == n_requests:
                out.append(b.id)
        return out

    def batch_results(self, batch_id):
        """{custom_id: ("ok", data) | (kind, message)} with kind api/refusal/max_tokens."""
        out = {}
        for e in self.c.messages.batches.results(batch_id):
            if e.result.type == "succeeded":
                try:
                    out[e.custom_id] = ("ok", _parse_message(e.result.message))
                except LLMError as le:
                    out[e.custom_id] = (le.kind, le.msg)
            else:   # errored / expired / canceled
                out[e.custom_id] = ("api", f"batch entry {e.result.type}")
        return out


def build_themes(claude, category, titles, state, save):
    """Theme list, then each title gets a theme. `state` holds progress for this pool
    ({"themes": [...], "assigned": {index: theme}}) and `save()` persists it after every
    paid call, so a failure never re-pays finished work (P6). Returns
    (themes, assigned_list, defaulted_indexes)."""
    n = len(titles)
    if not state.get("themes"):
        target = max(1, round(n / 10))
        listing = "\n".join(titles)
        res = claude.sync_json(_params(
            f"""Here are {n} recipe titles from the category "{category}".
Create about {target} themes that group similar recipes (by main ingredient, dish type or
flavour). Each theme name: 2-4 words, at most 28 characters, Title Case, no numbers,
no punctuation except '&'. Themes must not overlap.

{listing}""", THEMES_SCHEMA, 16000))
        state["themes"] = [t.strip()[:28] for t in res["themes"] if t.strip()] or ["Recipes"]
        save()
    themes = state["themes"]
    assigned = state.setdefault("assigned", {})
    numbered_themes = "\n".join(f"{k}. {t}" for k, t in enumerate(themes))

    def ask(indexes, depth=0):
        part = [titles[i] for i in indexes]
        lines = "\n".join(f"{j}. {t}" for j, t in enumerate(part))
        try:
            res = claude.sync_json(_params(
                f"""Themes:
{numbered_themes}

Assign each numbered recipe title below to exactly one theme. Answer with pairs
[title number, theme number] for every title, e.g. {{"a": [[0, 3], [1, 0]]}}.

{lines}""", ASSIGN_SCHEMA, 8000))
        except LLMError:
            if depth >= 3 or len(indexes) <= 10:
                raise
            half = len(indexes) // 2         # answer cut off or invalid: retry in halves
            ask(indexes[:half], depth + 1)
            ask(indexes[half:], depth + 1)
            return
        for pr in res.get("a", []):
            if len(pr) == 2 and 0 <= pr[0] < len(part) and 0 <= pr[1] < len(themes):
                assigned[str(indexes[pr[0]])] = themes[pr[1]]
        save()

    per = 150   # titles per call keeps each answer short
    for start in range(0, n, per):
        todo = [i for i in range(start, min(n, start + per)) if str(i) not in assigned]
        if todo:
            ask(todo)
    missing = [i for i in range(n) if str(i) not in assigned]
    if missing:          # titles left out of an answer: ask once more for just those
        ask(missing)
    defaulted = [i for i in range(n) if str(i) not in assigned]
    return themes, [assigned.get(str(i), themes[0]) for i in range(n)], defaulted
