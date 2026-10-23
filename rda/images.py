"""Image generation (Replicate) and crops. Prediction IDs are stored before waiting."""
import io
import os
import time

import requests
from PIL import Image

from . import config, db
from .log import log, short

LANDSCAPE = (1200, 628)
SQUARE = (720, 720)
MAX_BYTES = 5_120 * 1024


def crop_pair(raw_bytes):
    """Return (landscape_jpeg_bytes, square_jpeg_bytes) from any output size."""
    im = Image.open(io.BytesIO(raw_bytes)).convert("RGB")
    w, h = im.size
    target = LANDSCAPE[0] / LANDSCAPE[1]
    if w / h > target:            # too wide: trim sides
        nw = round(h * target)
        box = ((w - nw) // 2, 0, (w - nw) // 2 + nw, h)
    else:                          # too tall: trim top/bottom
        nh = round(w / target)
        box = (0, (h - nh) // 2, w, (h - nh) // 2 + nh)
    land = im.crop(box).resize(LANDSCAPE, Image.LANCZOS)
    s = min(w, h)
    sq = im.crop(((w - s) // 2, (h - s) // 2, (w - s) // 2 + s, (h - s) // 2 + s)).resize(
        SQUARE, Image.LANCZOS)
    return _jpeg(land), _jpeg(sq)


def _jpeg(im):
    for q in (85, 75, 65):
        buf = io.BytesIO()
        im.save(buf, "JPEG", quality=q, optimize=True)
        if buf.tell() <= MAX_BYTES:
            return buf.getvalue()
    raise ValueError("image too large after compression")


def save_crops(article_id, raw_bytes):
    land, sq = crop_pair(raw_bytes)
    os.makedirs(config.IMG_DIR, exist_ok=True)
    lp = os.path.join(config.IMG_DIR, f"{article_id}_l.jpg")
    sp = os.path.join(config.IMG_DIR, f"{article_id}_s.jpg")
    with open(lp, "wb") as f:
        f.write(land)
    with open(sp, "wb") as f:
        f.write(sq)
    return lp, sp


class ReplicateClient:
    """Thin wrapper so tests/dry-run can swap in a fake."""

    def __init__(self):
        import replicate
        self.r = replicate.Client(api_token=config.REPLICATE_API_TOKEN)

    def schema_inputs(self, model_ref):
        m = self.r.models.get(model_ref)
        schema = m.latest_version.openapi_schema
        return schema["components"]["schemas"]

    def create(self, model_ref, inputs):
        p = self.r.models.predictions.create(model=model_ref, input=inputs)
        return p.id

    def get(self, pid):
        p = self.r.predictions.get(pid)
        out = p.output
        if isinstance(out, list):
            out = out[0] if out else None
        return p.status, (str(out) if out else None), p.error

    def cancel(self, pid):
        self.r.predictions.cancel(pid)

    def download(self, url):
        r = requests.get(url, timeout=120)
        r.raise_for_status()
        return r.content


def start_prediction(con, client, article, model_key, extra_rule=""):
    """Create one prediction and record its ID immediately."""
    m = config.IMAGE_MODELS[model_key]
    copy = __import__("json").loads(article["copy_json"])
    prompt = copy["image_prompt"].strip()
    prompt += (" Photorealistic food photography. No text, letters, numbers, labels, logos,"
               " watermarks, people or hands anywhere in the image. Keep the dish centred"
               " with space around it so it also works as a square crop." + extra_rule)
    pid = client.create(m["ref"], {"prompt": prompt, **m["input"]})
    con.execute("INSERT INTO predictions(id,article_id,model,state,created) VALUES(?,?,?,?,?)",
                (pid, article["id"], model_key, "starting", time.time()))
    db.update_article(con, article["id"], status="image_pending", image_model=model_key,
                      image_attempts=article["image_attempts"] + 1)
    log(f"image started: {article['id']} '{short(article['title'], 60)}' model={model_key} "
        f"attempt={article['image_attempts'] + 1} prediction={pid}")
    return pid


PREDICTION_TIMEOUT_S = 1800   # 30 min (R9): older unfinished predictions are cancelled


def _fail_prediction(con, p, reason):
    con.execute("UPDATE predictions SET state='failed' WHERE id=?", (p["id"],))
    a = con.execute("SELECT image_attempts, status FROM articles WHERE id=?",
                    (p["article_id"],)).fetchone()
    if not a or a["status"] != "image_pending":
        return
    if a["image_attempts"] >= 3:
        db.update_article(con, p["article_id"], status="skipped",
                          skip_reason=f"image could not be produced: {reason}"[:300])
    else:
        db.update_article(con, p["article_id"], status="copy_done",
                          error=f"image failed, will retry: {reason}"[:300])
        log(f"image failed (will retry): {p['article_id']} prediction={p['id']}: "
            f"{short(reason, 200)}")


def poll_predictions(con, client):
    """Collect finished predictions. Never raises for one bad prediction (P5): corrupt
    images, missing output, lost predictions and timeouts all fail just that prediction."""
    rows = con.execute("SELECT * FROM predictions WHERE state NOT IN ('done','failed')").fetchall()
    done = 0
    for p in rows:
        age = time.time() - (p["created"] or 0)
        if age > PREDICTION_TIMEOUT_S:
            try:
                client.cancel(p["id"])
                log(f"image cancelled (no result after {int(age // 60)} min): prediction={p['id']}")
            except Exception as e:
                log(f"image cancel failed: prediction={p['id']}: {type(e).__name__}: {short(e)}")
            _fail_prediction(con, p, f"no result after {int(age // 60)} min")
            continue
        try:
            status, url, err = client.get(p["id"])
        except Exception as e:      # network / lost prediction: the timeout above handles it
            log(f"image status could not be read (will retry): prediction={p['id']}: "
                f"{type(e).__name__}: {short(e)}")
            continue
        if status == "succeeded":
            if not url:
                _fail_prediction(con, p, "succeeded with no output")
                continue
            try:
                raw = client.download(url)
                lp, sp = save_crops(p["article_id"], raw)
            except Exception as e:  # corrupt / truncated / not an image
                _fail_prediction(con, p, f"bad image: {type(e).__name__}: {e}")
                continue
            con.execute("UPDATE predictions SET state='done' WHERE id=?", (p["id"],))
            log(f"image done: {p['article_id']} model={p['model']} prediction={p['id']}")
            # new image: any Google Ads image asset stored for an older image must not be used
            db.update_article(con, p["article_id"], status="image_done", landscape_path=lp,
                              square_path=sp, error=None, landscape_asset=None,
                              square_asset=None)
            done += 1
        elif status in ("failed", "canceled"):
            _fail_prediction(con, p, f"replicate {status}: {err}")
    return done
