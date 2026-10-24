"""Run the live check by hand: python -m scripts.live_check (see rda/livecheck.py).
Refuses to run (before touching anything) while the worker holds the database lease."""
import sys
import time

from rda import config, db, livecheck, worker

con = db.connect()
lease = db.kv_get(con, "lease")
if lease and time.time() - lease["ts"] < config.LEASE_STALE_S:
    sys.exit("The worker is running (lease held). Its live check runs at every start; "
             "see the Railway logs instead.")
w = worker.build()
if not w.acquire_lease():
    sys.exit("The worker started meanwhile; not running.")
try:
    livecheck.run(w)
finally:
    w.release_lease()
