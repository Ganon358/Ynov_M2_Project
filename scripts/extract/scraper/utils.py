import os, time, csv, json, re
from datetime import datetime
from dateutil import parser as dateparser

def polite_sleep(delay):
    if delay and delay > 0:
        time.sleep(delay)

def now_stamp():
    return datetime.utcnow().strftime("%Y%m%d_%H%M%S")

def ensure_outdir():
    os.makedirs("out", exist_ok=True)

def write_jsonl(records, path):
    with open(path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def write_csv(records, path, field_order=None):
    if not records:
        open(path, "w", encoding="utf-8").close()
        return
    fields = field_order or sorted(set().union(*[r.keys() for r in records]))
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in records:
            w.writerow({k: (json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v) for k, v in r.items()})

def slugify(text):
    text = text or ""
    text = re.sub(r"[^a-zA-Z0-9_-]+", "-", text.strip())
    return re.sub(r"-+", "-", text).strip("-").lower()

def parse_iso_date(s):
    if not s:
        return None
    try:
        return dateparser.parse(s).date().isoformat()
    except Exception:
        return None
