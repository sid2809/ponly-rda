"""One-off: load the category CSVs into the Sheet's Input tab.
Usage (needs SHEET_ID + GOOGLE_SERVICE_ACCOUNT_JSON): python -m scripts.load_input data_csv"""
import csv
import glob
import os
import sys

from rda.sheet import Sheet
from rda.split import pretty_category

folder = sys.argv[1] if len(sys.argv) > 1 else "data_csv"
rows = [["URL", "Title", "Category"]]
for f in sorted(glob.glob(os.path.join(folder, "*.csv"))):
    cat = pretty_category(os.path.basename(f)[:-4])
    for r in csv.DictReader(open(f, encoding="utf-8-sig")):
        rows.append([r["URL"].strip(), r["Title"].strip(), cat])
s = Sheet()
s.ensure_tabs()
s.write("Input", rows, force=True)
print(f"Input tab: {len(rows) - 1} articles")
