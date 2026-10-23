"""Phase 0: one harmless test call to each service. Creates nothing, costs ~nothing.
Usage: python -m scripts.check_connections"""
from rda import config, db

ok = True
def report(name, fn):
    global ok
    try:
        print(f"OK   {name}: {fn()}")
    except Exception as e:
        ok = False
        print(f"FAIL {name}: {type(e).__name__}: {str(e)[:300]}")

def sheets():
    from rda.sheet import Sheet
    s = Sheet(); s.ensure_tabs()
    return f"tabs ready, Input rows: {len(s.read('Input'))}"

def ads():
    from rda.gads import Ads
    a = Ads(db.connect())
    r = a.search("SELECT customer.id, customer.descriptive_name, customer.currency_code "
                 "FROM customer LIMIT 1", "check")
    c = r[0].customer
    return f"customer {c.id} '{c.descriptive_name}' currency {c.currency_code}"

def claude():
    import anthropic
    m = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY).models.retrieve(config.HAIKU_MODEL)
    return f"model {m.id} available"

def replicate_():
    from rda.images import ReplicateClient
    r = ReplicateClient()
    return ", ".join(f"{k}: schema read" for k, m in config.IMAGE_MODELS.items()
                     if r.schema_inputs(m["ref"]))

for n, f in (("Google Sheet", sheets), ("Google Ads", ads), ("Anthropic", claude),
             ("Replicate", replicate_)):
    report(n, f)
print("ALL OK" if ok else "Some checks failed (see above)")
