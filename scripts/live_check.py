"""Run the live check by hand: python -m scripts.live_check (see rda/livecheck.py).
Refuses to run while the worker holds the database lease."""
import sys

from rda import livecheck, worker

w = worker.build()
if not w.acquire_lease():
    sys.exit("The worker is running (lease held). Its live check runs at every start; "
             "see the Railway logs instead.")
try:
    livecheck.run(w)
finally:
    w.release_lease()
