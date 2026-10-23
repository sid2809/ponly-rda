"""Claude Haiku 5.5 calls: themes (sync), copy / image check / alcohol (Batch API, 50% off).
Haiku 5.5 rules respected: no temperature, no prefill; JSON via output_config.format."""
import base64
import json
import math

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
ASSIGN_SCHEMA = {
    "type": "object",
    "properties": {"assignments": {"type": "array", "items": {
        "type": "object",
        "properties": {"n": {"type": "integer"}, "theme": {"type": "string"}},
        "required": ["n", "theme"], "additionalProperties": False}}},
    "required": ["assignments"], "additionalProperties": False,
}


def _params(content, schema, max_tokens):
    return {"model": config.HAIKU_MODEL, "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": content}],
            "output_config": {"format": {"type": "json_schema", "schema": schema}}}


def copy_params(title, category, theme, problems=None):
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
    return _params(text, COPY_SCHEMA, 4000)


def check_params(title, landscape_bytes, square_bytes):
    def img(b):
        return {"type": "image", "source": {"type": "base64", "media_type": "image/jpeg",
                                            "data": base64.b64encode(b).decode()}}
    text = f"""These two images (wide and square crop) will be used in a display ad for the
recipe "{title}". Fail them if EITHER image has any of: visible text, letters or numbers;
a watermark; a logo or brand; a real person, face, body part or hands; adult, violent or
disturbing content; a collage or split image; heavy blur or distortion; or a dish that does
not match the title. Otherwise pass. List each problem briefly."""
    return _params([img(landscape_bytes), img(square_bytes), {"type": "text", "text": text}],
                   CHECK_SCHEMA, 2000)


def alcohol_params(title):
    text = f"""Recipe title: "{title}"
Answer alcohol=true if the title names an alcoholic drink (including cocktail names such as
Mai Tai or Cosmopolitan), names an alcoholic ingredient, or is a drink whose alcohol content
is unclear (punch, cider, eggnog, toddy). Words used only as style descriptions (e.g.
"Old Fashioned" meaning traditional) do not count. Dishes count only if alcohol is named in
the title. If genuinely ambiguous, answer true."""
    return _params(text, ALCOHOL_SCHEMA, 1000)


class Claude:
    def __init__(self):
        import anthropic
        self.c = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)

    def sync_json(self, params):
        # streaming: the SDK refuses large max_tokens on non-streaming calls
        with self.c.messages.stream(**params) as stream:
            r = stream.get_final_message()
        text = next(b.text for b in r.content if b.type == "text")
        return json.loads(text)

    def create_batch(self, requests):
        """requests: list of (custom_id, params). Returns batch id."""
        b = self.c.messages.batches.create(
            requests=[{"custom_id": cid, "params": p} for cid, p in requests])
        return b.id

    def batch_ended(self, batch_id):
        return self.c.messages.batches.retrieve(batch_id).processing_status == "ended"

    def batch_results(self, batch_id):
        out = {}
        for e in self.c.messages.batches.results(batch_id):
            if e.result.type == "succeeded":
                try:
                    text = next(b.text for b in e.result.message.content if b.type == "text")
                    out[e.custom_id] = (True, json.loads(text))
                except Exception as ex:
                    out[e.custom_id] = (False, f"bad JSON: {ex}")
            else:
                out[e.custom_id] = (False, e.result.type)
        return out


def build_themes(claude, category, titles, cache=None):
    """Two steps: fixed theme list from all titles, then each title gets a theme.
    Chunks stay well under the 100k-token long-prompt price tier."""
    n = len(titles)
    target = max(1, round(n / 10))
    listing = "\n".join(titles)
    cached = cache.get(category) if cache is not None else None
    themes = cached or claude.sync_json(_params(
        f"""Here are {n} recipe titles from the category "{category}".
Create about {target} themes that group similar recipes (by main ingredient, dish type or
flavour). Each theme name: 2-4 words, at most 28 characters, Title Case, no numbers,
no punctuation except '&'. Themes must not overlap.

{listing}""", THEMES_SCHEMA, 8000))["themes"]
    themes = [t.strip()[:28] for t in themes if t.strip()] or ["Recipes"]
    if cache is not None:
        cache[category] = themes
    assigned = {}
    per = 1500  # titles per assignment call (~20k tokens)
    for start in range(0, n, per):
        part = titles[start:start + per]
        lines = "\n".join(f"{i}. {t}" for i, t in enumerate(part))
        res = claude.sync_json(_params(
            f"""Assign each numbered recipe title to exactly one theme from this list
(use the theme text exactly):
{json.dumps(themes)}

{lines}""", ASSIGN_SCHEMA, 16000 + 12 * len(part)))
        for item in res["assignments"]:
            i = item["n"]
            if 0 <= i < len(part):
                t = item["theme"] if item["theme"] in themes else themes[0]
                assigned[start + i] = t
    return themes, [assigned.get(i, themes[0]) for i in range(n)]
