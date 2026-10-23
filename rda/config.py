"""All settings. Secrets come only from environment variables (Railway Variables)."""
import os

DATA_DIR = os.environ.get("DATA_DIR", "/data")
DB_PATH = os.path.join(DATA_DIR, "rda.sqlite")
IMG_DIR = os.path.join(DATA_DIR, "images")
ASSETS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "assets")
LOGO_LANDSCAPE = os.path.join(ASSETS_DIR, "logo_landscape.png")
LOGO_SQUARE = os.path.join(ASSETS_DIR, "logo_square.png")

DRY_RUN = os.environ.get("DRY_RUN", "0") == "1"  # fake APIs, nothing paid or created
# Keep every generated image on the volume after upload (so nothing is ever paid for twice)
KEEP_IMAGES = os.environ.get("KEEP_IMAGES", "1") == "1"

# --- Google ---
SHEET_ID = os.environ.get("SHEET_ID", "")
SA_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")  # full JSON text
CUSTOMER_ID = os.environ.get("GOOGLE_ADS_CUSTOMER_ID", "").replace("-", "")
LOGIN_CUSTOMER_ID = os.environ.get("GOOGLE_ADS_LOGIN_CUSTOMER_ID", "").replace("-", "")
DEVELOPER_TOKEN = os.environ.get("GOOGLE_ADS_DEVELOPER_TOKEN", "")
OAUTH_CLIENT_ID = os.environ.get("GOOGLE_ADS_CLIENT_ID", "")
OAUTH_CLIENT_SECRET = os.environ.get("GOOGLE_ADS_CLIENT_SECRET", "")
OAUTH_REFRESH_TOKEN = os.environ.get("GOOGLE_ADS_REFRESH_TOKEN", "")
ADS_USE_SERVICE_ACCOUNT = os.environ.get("GOOGLE_ADS_USE_SERVICE_ACCOUNT", "0") == "1"

# --- AI / images ---
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
REPLICATE_API_TOKEN = os.environ.get("REPLICATE_API_TOKEN", "")
HAIKU_MODEL = "claude-haiku-5-5"

# Image models selectable in Settings. Inputs are checked against each model's
# published schema in preflight before anything is generated.
IMAGE_MODELS = {
    "minimax": {"ref": "minimax/image-01", "input": {"aspect_ratio": "16:9"},
                "est_usd": 0.01},   # [R] third-party price, confirm from billing
    "nano-banana": {"ref": "google/nano-banana", "input": {"aspect_ratio": "16:9"},
                    "est_usd": 0.039},  # [R]
}

# --- Business rules (Siddharth's decisions) ---
BUSINESS_NAME = "Leapfour Media Pvt Ltd"
BRAND_LABEL = "Ponly Cook"   # on every campaign (Siddharth, 23 Oct) + a category label
DOMAIN_PREFIX = "https://ponly.com/"
BUDGET_PER_ARTICLE_INR = 300
MAX_CPC_INR = 2
# Siddharth 23 Oct: 6-8 articles per campaign, 2-4 articles per ad group.
# Leftovers: under 6 -> one campaign; 9-11 -> one campaign; 17 -> 9 + 8.
CAMPAIGN_MIN, CAMPAIGN_MAX = 6, 8
ADGROUP_MIN, ADGROUP_MAX = 2, 4
GEO_US = "geoTargetConstants/2840"
LANG_EN = "languageConstants/1000"
UTM_SUFFIX = ("utm_source=google&utm_medium=display&utm_campaign={campaignid}"
              "&adgroup={adgroupid}&utm_content={creative}&utm_term={placement}"
              "&device={device}")
PILOT_PER_CATEGORY = 20      # 2 campaigns x 10 (one per image model)
PILOT_FOOD_CATEGORIES = 4
PILOT_ALCOHOL = 20
WAVE2_SIZE = 900

# Pass rules (confirmed by Siddharth 22 Oct)
PASS_APPROVED_MIN = 0.95
PASS_IMAGE_FAIL_MAX = 0.10

# --- Quotas / limits ---
DEFAULT_QUOTA_CAP = 14000
OPS_PER_ADD_REQUEST = 1000
BYTES_PER_ADD_REQUEST = 9_000_000   # v26 REQUEST_TOO_LARGE limit is 10,484,504 bytes
EXPLORER_QUOTA_CAP = 2700
LEASE_STALE_S = 900
SHEETS_MIN_WRITE_INTERVAL_S = 120
LOOP_SLEEP_S = 60
# Siddharth 23 Oct: wait 20-30 s between Replicate image requests (rate limit)
IMAGE_GAP_S = (20, 30)
IMAGE_STEP_BUDGET_S = 60   # max time one loop spends starting images
