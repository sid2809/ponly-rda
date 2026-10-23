"""Google Sheet I/O via the service account. Writes are throttled and use RAW values,
so titles starting with = or + are never run as formulas."""
import json
import time

from . import config

TABS = ["Input", "Settings", "Output", "Campaign map", "Summary"]
SETTINGS_DEFAULTS = [
    ["Setting", "Value", "Notes"],
    ["W1 enable", False, "Tick to enable Wave 1 (pilot) campaigns"],
    ["W2 enable", False, "Tick to enable Wave 2 campaigns"],
    ["W3 enable", False, "Tick to enable Wave 3 campaigns"],
    ["Image model", "", "Set after the pilot: nano-banana or minimax. Wave 2 images start "
                        "only once this is set. Change any time; applies to new images."],
    ["Daily API quota cap", config.DEFAULT_QUOTA_CAP, "14000 at Basic; 2700 at Explorer; "
                                                      "lower it if other tools share the project"],
    ["Access level", "Basic", "Google Ads API level of the Cloud project (Basic/Explorer)"],
    ["Auto-apply recommendations off", False, "Tick after checking Recommendations > "
                                              "Auto-apply in Google Ads"],
    ["Pilot categories", "", "4 food categories, separated by ;  (blank = 4 largest)"],
    ["Pause", False, "Tick to pause the worker (nothing new is started)"],
]
OUTPUT_HEADER = ["ID", "Category", "Title", "Final URL", "Alcohol", "Alcohol reason", "Wave",
                 "Image model", "Campaign", "Ad group", "Status", "Error / skip reason",
                 "Approval", "Policy note", "Campaign ID", "Ad ID", "Impressions", "Clicks",
                 "Regeneration cost ($)"]


class Sheet:
    def __init__(self):
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
        creds = service_account.Credentials.from_service_account_info(
            json.loads(config.SA_JSON),
            scopes=["https://www.googleapis.com/auth/spreadsheets"])
        self.svc = build("sheets", "v4", credentials=creds, cache_discovery=False)
        self.sid = config.SHEET_ID
        self._last_write = {}

    def _call(self, req):
        for attempt in range(6):
            try:
                return req.execute()
            except Exception as e:  # 429 / 5xx: back off
                code = getattr(getattr(e, "resp", None), "status", None)
                if code in (429, 500, 502, 503) and attempt < 5:
                    time.sleep(2 ** attempt * 5)
                    continue
                raise

    def ensure_tabs(self):
        meta = self._call(self.svc.spreadsheets().get(spreadsheetId=self.sid))
        have = {s["properties"]["title"]: s["properties"]["sheetId"] for s in meta["sheets"]}
        reqs = [{"addSheet": {"properties": {"title": t}}} for t in TABS if t not in have]
        if reqs:
            self._call(self.svc.spreadsheets().batchUpdate(
                spreadsheetId=self.sid, body={"requests": reqs}))
        vals = self.read("Settings")
        if len(vals) < 2:
            self.write("Settings", SETTINGS_DEFAULTS, force=True)
            meta = self._call(self.svc.spreadsheets().get(spreadsheetId=self.sid))
            sid = next(s["properties"]["sheetId"] for s in meta["sheets"]
                       if s["properties"]["title"] == "Settings")
            boxes = [1, 2, 3, 7, 9]  # rows with tick boxes (0-based)
            self._call(self.svc.spreadsheets().batchUpdate(spreadsheetId=self.sid, body={
                "requests": [{"setDataValidation": {
                    "range": {"sheetId": sid, "startRowIndex": r, "endRowIndex": r + 1,
                              "startColumnIndex": 1, "endColumnIndex": 2},
                    "rule": {"condition": {"type": "BOOLEAN"}}}} for r in boxes]}))
        if not self.read("Input"):
            self.write("Input", [["URL", "Title", "Category"]], force=True)

    def read(self, tab):
        r = self._call(self.svc.spreadsheets().values().get(
            spreadsheetId=self.sid, range=f"'{tab}'", valueRenderOption="UNFORMATTED_VALUE"))
        return r.get("values", [])

    def read_settings(self):
        out = {}
        for row in self.read("Settings")[1:]:
            if row:
                out[str(row[0]).strip()] = row[1] if len(row) > 1 else ""
        return out

    def write(self, tab, rows, force=False):
        """Overwrite a whole tab. Throttled to one write per tab per minute."""
        now = time.time()
        if not force and now - self._last_write.get(tab, 0) < config.SHEETS_MIN_WRITE_INTERVAL_S:
            return False
        self._call(self.svc.spreadsheets().values().clear(spreadsheetId=self.sid,
                                                          range=f"'{tab}'"))
        self._call(self.svc.spreadsheets().values().update(
            spreadsheetId=self.sid, range=f"'{tab}'!A1", valueInputOption="RAW",
            body={"values": rows}))
        self._last_write[tab] = now
        return True

    def append(self, tab, rows):
        self._call(self.svc.spreadsheets().values().append(
            spreadsheetId=self.sid, range=f"'{tab}'!A1", valueInputOption="RAW",
            insertDataOption="INSERT_ROWS", body={"values": rows}))
