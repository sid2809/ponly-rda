"""Phase 1 test: send one wave's operations to Google Ads in validate-only mode.
Nothing is created. Usage: python -m scripts.validate_only 1"""
import sys
from rda import db
from rda.gads import Ads

wave = int(sys.argv[1]) if len(sys.argv) > 1 else 1
con = db.connect()
a = Ads(con)
errs = a.validate_wave(wave)
print("validate-only:", "no errors" if not errs else "\n\n".join(errs))
