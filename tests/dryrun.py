"""Full offline dry run: python -m tests.dryrun  (uses fakes, nothing real)."""
import os, tempfile, time
os.environ["DRY_RUN"] = "1"
os.environ.setdefault("DATA_DIR", tempfile.mkdtemp(prefix="rda_dry_"))
from rda import config, db, worker

w = worker.build()
w.sheet.set_setting("Auto-apply recommendations off", True)
w.sheet.set_setting("Pause", False)
config.SHEETS_MIN_WRITE_INTERVAL_S = 0
config.IMAGE_GAP_S = (0, 0)
t0 = time.time()
for i in range(400):
    w.tick()
    con = w.con
    # simulate Siddharth: enable boxes once uploads finish; choose model after pilot
    for n in (1, 2, 3):
        if db.kv_get(con, f"uploaded_{n}"):
            w.sheet.set_setting(f"W{n} enable", True)
    if db.kv_get(con, "enabled_1"):
        db.kv_set(con, "sync_now", True) if not db.kv_get(con, "synced_once_1") else None
        db.kv_set(con, "synced_once_1", True)
        w.sheet.set_setting("Image model", "minimax")
    if db.kv_get(con, "enabled_2") and not db.kv_get(con, "synced_once_2"):
        db.kv_set(con, "sync_now", True); db.kv_set(con, "synced_once_2", True)
    if db.kv_get(con, "enabled_3") and db.kv_get(con, "final_sync_done"):
        break
print("ticks", i + 1, "seconds", round(time.time() - t0))
for r in w.summary_rows():
    print(r)
print("ops submitted by type:", {k: w.ads.submitted.count(k) for k in set(w.ads.submitted)})
print("DATA_DIR", os.environ["DATA_DIR"])
