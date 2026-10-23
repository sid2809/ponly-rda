"""Phase 1 test: send one wave's operations to Google Ads in validate-only mode.
Nothing is created. Usage: python -m scripts.validate_only 1"""
import sys
from rda import db
from rda.gads import Ads

wave = int(sys.argv[1]) if len(sys.argv) > 1 else 1
con = db.connect()
lease = db.kv_get(con, "lease")
if lease:
    import time
    if time.time() - lease["ts"] < 900:
        sys.exit("The worker is running (lease held); stop it first.")
a = Ads(con)
errs = a.validate_wave(wave)
print("validate-only:", "no errors" if not errs else "\n\n".join(errs))
