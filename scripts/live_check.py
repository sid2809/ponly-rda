"""Run the live check by hand: python -m scripts.live_check (read-only, see rda/livecheck.py)."""
from rda import livecheck, worker

w = worker.build()
livecheck.run(w)
