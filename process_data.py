"""
Process all scraped Property Finder data into dashboard_data.json.
Handles Dubai + Abu Dhabi, multi-bed unit types, tracks price/rent drops in SQLite.

Usage: python process_data.py
"""
import json
import re
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_PATH = DATA_DIR / "dashboard_data.json"
DB_PATH = DATA_DIR / "listings.db"

from config.area_tiers import (
    AREA_TIERS, DEFAULT_TIER,
    SERVICE_CHARGE_BY_AREA, SERVICE_CHARGE_BY_TIER, DEFAULT_SC_PER_SQFT,
)

PF_BASE = "https://www.propertyfinder.ae"

# Legacy names kept for backward compatibility
LEGACY_SALES_FILES = {
    "sales.json",
    "dubai_studio_sales.json",
    "ad_1br_sales.json",
    "ad_studio_sales.json",
}
LEGACY_RENTAL_FILES = {
    "rentals.json",
    "dubai_studio_rentals.json",
    "ad_1br_rentals.json",
    "ad_studio_rentals.json",
}

# ---- Dubai area slug map ----
DUBAI_SLUG_TO_AREA = {
    "downtown-dubai": "Downtown Dubai", "business-bay": "Business Bay",
    "dubai-marina": "Dubai Marina", "jumeirah-lake-towers": "Jumeirah Lake Towers",
    "jumeirah-village-circle": "Jumeirah Village Circle",
    "jumeirah-village-triangle": "Jumeirah Village Triangle",
    "jumeirah-beach-residence": "JBR", "palm-jumeirah": "Palm Jumeirah",
    "dubai-hills-estate": "Dubai Hills Estate", "dubai-creek-harbour": "Dubai Creek Harbour",
    "mohammed-bin-rashid-city": "Mohammed Bin Rashid City",
    "dubai-harbour": "Dubai Harbour", "dubai-silicon-oasis": "Dubai Silicon Oasis",
    "dubai-sports-city": "Dubai Sports City", "dubai-investment-park": "Dubai Investment Park",
    "dubai-south": "Dubai South", "dubai-production-city": "Dubai Production City",
    "dubai-science-park": "Dubai Science Park", "dubai-studio-city": "Dubai Studio City",
    "dubai-land": "Dubai Land", "dubai-festival-city": "Dubai Festival City",
    "dubai-design-district": "DIFC", "international-city": "International City",
    "discovery-gardens": "Discovery Gardens", "damac-hills-2": "Damac Hills 2",
    "damac-hills": "DAMAC Hills", "city-walk": "City Walk",
    "culture-village": "Culture Village", "barsha-heights-tecom": "Barsha Heights (Tecom)",
    "arjan": "Arjan", "al-furjan": "Al Furjan", "al-barsha": "Al Barsha",
    "al-jaddaf": "Al Jaddaf", "al-quoz": "Al Quoz", "al-satwa": "Al Satwa",
    "al-sufouh": "Al Sufouh", "al-warsan": "Al Warsan", "al-muhaisnah": "Al Muhaisnah",
    "bluewaters": "Bluewaters", "bur-dubai": "Bur Dubai", "difc": "DIFC",
    "greens": "Greens", "motor-city": "Motor City", "mudon": "Mudon",
    "meydan": "Meydan", "town-square": "Town Square", "the-views": "The Views",
    "living-legends": "Living Legends", "maritime-city": "Maritime City",
    "downtown-jebel-ali": "Downtown Jebel Ali", "jebel-ali": "Jebel Ali",
    "wadi-al-safa": "Wadi Al Safa", "wasl-gate": "Wasl Gate",
    "nadd-al-hammar": "Nadd Al Hammar", "jumeirah": "Jumeirah",
    "hartland-greens": "Hartland Greens", "sobha-hartland": "Mohammed Bin Rashid City",
    "ras-al-khor": "Dubai Land", "dubai-internet-city": "Al Sufouh",
    "dubai-media-city": "Al Sufouh", "al-quoz-industrial": "Al Quoz",
    "umm-suqeim": "Umm Suqeim", "madinat-jumeirah-living": "Umm Suqeim",
}

# ---- Abu Dhabi area slug map ----
AD_SLUG_TO_AREA = {
    "al-reem-island": "Al Reem Island", "al-reem": "Al Reem Island",
    "yas-island": "Yas Island", "saadiyat-island": "Saadiyat Island",
    "al-maryah-island": "Al Maryah Island", "al-maryah": "Al Maryah Island",
    "al-raha-beach": "Al Raha Beach", "al-raha": "Al Raha Beach",
    "al-ghadeer": "Al Ghadeer", "al-reef": "Al Reef",
    "masdar-city": "Masdar City", "masdar": "Masdar City",
    "khalifa-city": "Khalifa City", "al-jubail-island": "Al Jubail Island",
    "al-jubail": "Al Jubail Island",
    "mohamed-bin-zayed-city": "Mohamed Bin Zayed City",
    "mohamed-bin": "Mohamed Bin Zayed City",
    "al-khalidiya": "Al Khalidiya", "corniche-road": "Corniche Road",
    "al-bateen": "Al Bateen", "baniyas": "Baniyas",
    "al-shamkha": "Al Shamkha", "al-mushrif": "Al Mushrif",
    "madinat-al": "Madinat Al Riyad", "al-danah": "Al Danah",
    "al-nahyan": "Al Nahyan", "hamdan-street": "Hamdan Street",
    "muroor-area": "Muroor Area", "muroor": "Muroor Area",
    "airport-road": "Airport Road", "airport": "Airport Road",
    "tourist-club": "Tourist Club", "the-marina": "The Marina",
    "city-downtown": "City Downtown", "mussafah": "Mussafah",
    "mussafah-shabiya": "Mussafah", "al-wahda": "Al Wahda",
    "rawdhat": "Rawdhat", "al-zahiyah": "Al Zahiyah",
    "electra-street": "Electra Street", "capital-centre": "Capital Centre",
    "al-falah-city": "Al Falah City", "al-falah": "Al Falah City",
    "rabdan": "Rabdan",
}

_DUBAI_SLUGS = sorted(DUBAI_SLUG_TO_AREA.keys(), key=len, reverse=True)
_AD_SLUGS = sorted(AD_SLUG_TO_AREA.keys(), key=len, reverse=True)


def detect_city(path):
    if not path:
        return None
    p = str(path).lower()
    if "-dubai-" in p or "/dubai/" in p:
        return "Dubai"
    if "-abu-dhabi-" in p or "/abu-dhabi/" in p:
        return "Abu Dhabi"
    return None


def detect_city_from_listing(listing):
    explicit_city = (listing.get("city") or "").strip().lower()
    if explicit_city == "dubai":
        return "Dubai"
    if explicit_city in {"abu dhabi", "abu-dhabi"}:
        return "Abu Dhabi"
    return detect_city(listing.get("path"))


def extract_area(path):
    if not path:
        return None, None
    city = detect_city(path)
    if city == "Dubai":
        m = re.search(r"-dubai-(.+?)(?:-\d{5,}\.html|\.html|-[A-Za-z0-9]{10,}\.html)", path)
        if not m:
            return city, None
        slug = m.group(1)
        for prefix in _DUBAI_SLUGS:
            if slug.startswith(prefix + "-") or slug == prefix:
                return city, DUBAI_SLUG_TO_AREA[prefix]
    elif city == "Abu Dhabi":
        m = re.search(r"-abu-dhabi-(.+?)(?:-\d{5,}\.html|\.html|-[A-Za-z0-9]{10,}\.html)", path)
        if not m:
            return city, None
        slug = m.group(1)
        for prefix in _AD_SLUGS:
            if slug.startswith(prefix + "-") or slug == prefix:
                return city, AD_SLUG_TO_AREA[prefix]
    return city, None


def norm_beds(v):
    s = str(v).strip().lower()
    if s in ("0", "studio"):
        return "Studio"
    if re.match(r"^\d+$", s):
        return f"{int(s)}BR"
    return None


def norm_beds_tracking(v, listing=None):
    unit_type = ""
    if listing:
        unit_type = str(listing.get("unit_type", "")).strip().lower()
    if unit_type == "studio":
        return "Studio"
    m = re.match(r"^(\d+)\s*br$", unit_type)
    if m:
        return f"{m.group(1)}BR"

    s = str(v).strip().lower()
    if s in ("0", "studio"):
        return "Studio"
    if re.match(r"^\d+$", s):
        return f"{int(s)}BR"
    return "Other"


def sc_for_area(area):
    if area in SERVICE_CHARGE_BY_AREA:
        return SERVICE_CHARGE_BY_AREA[area]
    tier = AREA_TIERS.get(area, DEFAULT_TIER)
    return SERVICE_CHARGE_BY_TIER.get(tier, DEFAULT_SC_PER_SQFT)


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def infer_file_kind(path, payload):
    """Classify input JSON file as sales or rentals."""
    f_name = path.name.lower()
    p_type = str(payload.get("type", "")).lower()
    listing_type = str(payload.get("listing_type", "")).lower()

    if "sale" in p_type or "buy" in p_type or "sale" in listing_type:
        return "sales"
    if "rent" in p_type or "rent" in listing_type:
        return "rentals"

    if (
        "sales data" in f_name
        or "_sales" in f_name
        or f_name in LEGACY_SALES_FILES
        or f_name.startswith("sales")
    ):
        return "sales"
    if (
        "rental data" in f_name
        or "rentals" in f_name
        or "_rental" in f_name
        or f_name in LEGACY_RENTAL_FILES
        or f_name.startswith("rental")
    ):
        return "rentals"

    listings = payload.get("listings", [])
    if listings:
        sample = listings[: min(50, len(listings))]
        rent_votes = 0
        sales_votes = 0
        for row in sample:
            period = str(row.get("period", "")).lower()
            path_val = str(row.get("path", "")).lower()
            if any(k in period for k in ("month", "year", "annual", "week", "day")):
                rent_votes += 1
            if "/rent/" in path_val:
                rent_votes += 1
            if "/buy/" in path_val or "/sale/" in path_val:
                sales_votes += 1
        if rent_votes > sales_votes:
            return "rentals"
        if sales_votes > rent_votes:
            return "sales"

    return None


def discover_input_files(data_dir):
    """Load all valid listing JSON files from data directory."""
    sales_sources = []
    rental_sources = []
    skip_names = {"dashboard_data.json"}

    for fp in sorted(data_dir.glob("*.json")):
        if fp.name in skip_names:
            continue
        try:
            payload = load_json(fp)
        except Exception as exc:
            print(f"  Skipped {fp.name}: could not parse JSON ({exc})")
            continue

        if not isinstance(payload, dict):
            continue
        listings = payload.get("listings", [])
        if not isinstance(listings, list):
            continue

        kind = infer_file_kind(fp, payload)
        if kind == "sales":
            sales_sources.append((fp, payload))
        elif kind == "rentals":
            rental_sources.append((fp, payload))
        else:
            print(f"  Skipped {fp.name}: unable to infer sales/rentals type")

    return sales_sources, rental_sources


def norm_sqft(v):
    if v is None:
        return None
    try:
        x = int(float(v))
        return x if 100 <= x <= 2500 else None
    except (TypeError, ValueError):
        return None


def norm_furnished(v):
    if v is None:
        return 0
    s = (v or "").upper()
    if "YES" in s or "FURNISHED" in s:
        return 1
    if "PARTLY" in s or "PARTIAL" in s:
        return 2
    return 0


def annual_rent(rent_val, period):
    if rent_val is None or rent_val <= 0:
        return None
    try:
        r = float(rent_val)
    except (TypeError, ValueError):
        return None
    p = (period or "").lower()
    if "year" in p or "annual" in p:
        return r
    if "month" in p or "monthly" in p or not p:
        return r * 12
    return r * 12


def median(xs):
    if not xs:
        return None
    s = sorted(xs)
    m = len(s) // 2
    return (s[m] + s[m - 1]) / 2 if len(s) % 2 == 0 else s[m]


def build_url(listing):
    """Build full Property Finder URL from listing data."""
    url = listing.get("url") or ""
    if url and url.startswith("http"):
        return url
    path = listing.get("path") or ""
    if path:
        return PF_BASE + (path if path.startswith("/") else "/" + path)
    return ""


# ---------------------------------------------------------------------------
# SQLite
# ---------------------------------------------------------------------------
def init_db(db_path):
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""CREATE TABLE IF NOT EXISTS listing_history (
        listing_id TEXT NOT NULL, snapshot_date TEXT NOT NULL,
        price REAL NOT NULL, sqft INTEGER, beds TEXT, building TEXT,
        area TEXT, city TEXT, path TEXT, furnished TEXT,
        PRIMARY KEY (listing_id, snapshot_date))""")
    conn.execute("""CREATE TABLE IF NOT EXISTS price_drops (
        listing_id TEXT PRIMARY KEY, building TEXT, area TEXT, city TEXT,
        sqft INTEGER, beds TEXT, furnished TEXT,
        first_price REAL, previous_price REAL, current_price REAL,
        drop_from_prev REAL, drop_pct_from_prev REAL,
        total_drop REAL, total_drop_pct REAL,
        drop_count INTEGER, first_seen TEXT, last_drop_date TEXT)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS rental_history (
        listing_id TEXT NOT NULL, snapshot_date TEXT NOT NULL,
        annual_rent REAL NOT NULL, rent_raw REAL, period TEXT,
        sqft INTEGER, beds TEXT, building TEXT,
        area TEXT, city TEXT, path TEXT, furnished TEXT,
        PRIMARY KEY (listing_id, snapshot_date))""")
    conn.execute("""CREATE TABLE IF NOT EXISTS rental_drops (
        listing_id TEXT PRIMARY KEY, building TEXT, area TEXT, city TEXT,
        sqft INTEGER, beds TEXT, furnished TEXT,
        first_rent REAL, previous_rent REAL, current_rent REAL,
        drop_from_prev REAL, drop_pct_from_prev REAL,
        total_drop REAL, total_drop_pct REAL,
        drop_count INTEGER, first_seen TEXT, last_drop_date TEXT)""")
    # Add city column if missing (migration for existing DBs)
    try:
        conn.execute("SELECT city FROM listing_history LIMIT 1")
    except sqlite3.OperationalError:
        conn.execute("ALTER TABLE listing_history ADD COLUMN city TEXT DEFAULT 'Dubai'")
    try:
        conn.execute("SELECT city FROM price_drops LIMIT 1")
    except sqlite3.OperationalError:
        conn.execute("ALTER TABLE price_drops ADD COLUMN city TEXT DEFAULT 'Dubai'")
    try:
        conn.execute("SELECT period FROM rental_history LIMIT 1")
    except sqlite3.OperationalError:
        conn.execute("ALTER TABLE rental_history ADD COLUMN period TEXT DEFAULT ''")
    conn.commit()
    return conn


def store_snapshot(conn, sales_filtered, snapshot_date):
    rows = []
    seen = set()
    for s in sales_filtered:
        lid = str(s.get("id", ""))
        if not lid or lid in seen:
            continue
        seen.add(lid)
        city, area = extract_area(s.get("path"))
        city = city or detect_city_from_listing(s)
        rows.append((
            lid, snapshot_date, float(s.get("price", 0)),
            norm_sqft(s.get("sqft")), norm_beds_tracking(s.get("beds"), s),
            (s.get("building") or "").strip(),
            area or "Other", city or "Dubai",
            s.get("path", ""), (s.get("furnished") or "").strip(),
        ))
    conn.executemany("""INSERT OR REPLACE INTO listing_history
        (listing_id, snapshot_date, price, sqft, beds, building, area, city, path, furnished)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", rows)
    conn.commit()
    return len(rows)


def store_rental_snapshot(conn, rentals_filtered, snapshot_date):
    rows = []
    seen = set()
    for r in rentals_filtered:
        lid = str(r.get("id", ""))
        if not lid or lid in seen:
            continue
        seen.add(lid)
        city, area = extract_area(r.get("path"))
        city = city or detect_city_from_listing(r)
        ann = annual_rent(r.get("rent"), r.get("period"))
        if ann is None or ann <= 0:
            continue
        rows.append((
            lid, snapshot_date, float(ann),
            float(r.get("rent") or 0), (r.get("period") or "").strip(),
            norm_sqft(r.get("sqft")), norm_beds_tracking(r.get("beds"), r),
            (r.get("building") or "").strip(),
            area or "Other", city or "Dubai",
            r.get("path", ""), (r.get("furnished") or "").strip(),
        ))
    conn.executemany("""INSERT OR REPLACE INTO rental_history
        (listing_id, snapshot_date, annual_rent, rent_raw, period, sqft, beds, building, area, city, path, furnished)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", rows)
    conn.commit()
    return len(rows)


def compute_drops(conn, snapshot_date):
    cur = conn.execute(
        "SELECT listing_id, price, sqft, beds, building, area, city, furnished "
        "FROM listing_history WHERE snapshot_date = ?", (snapshot_date,))
    current = {r[0]: r for r in cur.fetchall()}
    drops_found = 0
    for lid, row in current.items():
        cur_price, sqft, beds, building, area, city, furnished = row[1:]
        prior = conn.execute(
            "SELECT price, snapshot_date FROM listing_history "
            "WHERE listing_id = ? AND snapshot_date < ? ORDER BY snapshot_date ASC",
            (lid, snapshot_date)).fetchall()
        if not prior:
            continue
        first_price = prior[0][0]
        prev_price = prior[-1][0]
        if cur_price >= prev_price:
            existing = conn.execute(
                "SELECT current_price FROM price_drops WHERE listing_id = ?", (lid,)
            ).fetchone()
            if existing and cur_price < first_price:
                conn.execute("""UPDATE price_drops SET current_price=?, previous_price=?,
                    total_drop=?, total_drop_pct=?, drop_from_prev=0, drop_pct_from_prev=0
                    WHERE listing_id=?""", (
                    cur_price, prev_price, first_price - cur_price,
                    round((first_price - cur_price) / first_price * 100, 2), lid))
            continue
        drop_from_prev = prev_price - cur_price
        drop_pct_from_prev = round(drop_from_prev / prev_price * 100, 2) if prev_price > 0 else 0
        total_drop = first_price - cur_price
        total_drop_pct = round(total_drop / first_price * 100, 2) if first_price > 0 else 0
        price_chain = [first_price] + [p for p, _ in prior[1:]] + [cur_price]
        drop_count = sum(1 for i in range(1, len(price_chain)) if price_chain[i] < price_chain[i - 1])
        first_seen = prior[0][1]
        conn.execute("""INSERT OR REPLACE INTO price_drops
            (listing_id, building, area, city, sqft, beds, furnished,
             first_price, previous_price, current_price,
             drop_from_prev, drop_pct_from_prev, total_drop, total_drop_pct,
             drop_count, first_seen, last_drop_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", (
            lid, building, area, city, sqft, beds, furnished,
            first_price, prev_price, cur_price,
            drop_from_prev, drop_pct_from_prev, total_drop, total_drop_pct,
            drop_count, first_seen, snapshot_date))
        drops_found += 1
    conn.commit()
    return drops_found


def compute_rental_drops(conn, snapshot_date):
    cur = conn.execute(
        "SELECT listing_id, annual_rent, sqft, beds, building, area, city, furnished "
        "FROM rental_history WHERE snapshot_date = ?", (snapshot_date,))
    current = {r[0]: r for r in cur.fetchall()}
    drops_found = 0
    for lid, row in current.items():
        cur_rent, sqft, beds, building, area, city, furnished = row[1:]
        prior = conn.execute(
            "SELECT annual_rent, snapshot_date FROM rental_history "
            "WHERE listing_id = ? AND snapshot_date < ? ORDER BY snapshot_date ASC",
            (lid, snapshot_date)).fetchall()
        if not prior:
            continue
        first_rent = prior[0][0]
        prev_rent = prior[-1][0]
        if cur_rent >= prev_rent:
            existing = conn.execute(
                "SELECT current_rent FROM rental_drops WHERE listing_id = ?", (lid,)
            ).fetchone()
            if existing and cur_rent < first_rent:
                conn.execute("""UPDATE rental_drops SET current_rent=?, previous_rent=?,
                    total_drop=?, total_drop_pct=?, drop_from_prev=0, drop_pct_from_prev=0
                    WHERE listing_id=?""", (
                    cur_rent, prev_rent, first_rent - cur_rent,
                    round((first_rent - cur_rent) / first_rent * 100, 2), lid))
            continue
        drop_from_prev = prev_rent - cur_rent
        drop_pct_from_prev = round(drop_from_prev / prev_rent * 100, 2) if prev_rent > 0 else 0
        total_drop = first_rent - cur_rent
        total_drop_pct = round(total_drop / first_rent * 100, 2) if first_rent > 0 else 0
        rent_chain = [first_rent] + [p for p, _ in prior[1:]] + [cur_rent]
        drop_count = sum(1 for i in range(1, len(rent_chain)) if rent_chain[i] < rent_chain[i - 1])
        first_seen = prior[0][1]
        conn.execute("""INSERT OR REPLACE INTO rental_drops
            (listing_id, building, area, city, sqft, beds, furnished,
             first_rent, previous_rent, current_rent,
             drop_from_prev, drop_pct_from_prev, total_drop, total_drop_pct,
             drop_count, first_seen, last_drop_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", (
            lid, building, area, city, sqft, beds, furnished,
            first_rent, prev_rent, cur_rent,
            drop_from_prev, drop_pct_from_prev, total_drop, total_drop_pct,
            drop_count, first_seen, snapshot_date))
        drops_found += 1
    conn.commit()
    return drops_found


def load_all_drops(conn):
    return conn.execute("""SELECT listing_id, building, area, city, sqft, beds, furnished,
        first_price, previous_price, current_price,
        drop_from_prev, drop_pct_from_prev, total_drop, total_drop_pct,
        drop_count, first_seen, last_drop_date
        FROM price_drops WHERE total_drop > 0 ORDER BY total_drop_pct DESC""").fetchall()


def load_all_rental_drops(conn):
    return conn.execute("""SELECT listing_id, building, area, city, sqft, beds, furnished,
        first_rent, previous_rent, current_rent,
        drop_from_prev, drop_pct_from_prev, total_drop, total_drop_pct,
        drop_count, first_seen, last_drop_date
        FROM rental_drops WHERE total_drop > 0 ORDER BY total_drop_pct DESC""").fetchall()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Load ALL sales and rentals from all valid JSON files, dedup by id
    all_sales_raw = []
    all_rentals_raw = []
    sales_sources, rental_sources = discover_input_files(DATA_DIR)
    for fp, payload in sales_sources:
        all_sales_raw.extend(payload.get("listings", []))
        print(f"  Loaded {len(payload.get('listings', []))} sales from {fp.name}")
    for fp, payload in rental_sources:
        all_rentals_raw.extend(payload.get("listings", []))
        print(f"  Loaded {len(payload.get('listings', []))} rentals from {fp.name}")

    # Dedup by id
    seen_ids = set()
    sales_dedup = []
    for s in all_sales_raw:
        lid = str(s.get("id", ""))
        if lid and lid not in seen_ids:
            seen_ids.add(lid)
            sales_dedup.append(s)
    seen_ids = set()
    rentals_dedup = []
    for r in all_rentals_raw:
        lid = str(r.get("id", ""))
        if lid and lid not in seen_ids:
            seen_ids.add(lid)
            rentals_dedup.append(r)

    print(f"\nAfter dedup: {len(sales_dedup)} sales, {len(rentals_dedup)} rentals")

    # Filter: completed, valid sqft, any supported unit type
    sales = []
    for s in sales_dedup:
        beds = norm_beds(s.get("beds"))
        if beds is None:
            continue
        sqft = norm_sqft(s.get("sqft"))
        if sqft is None:
            continue
        comp = (s.get("completion") or "").lower()
        if comp and comp != "completed":
            continue
        price = float(s.get("price") or 0)
        if price <= 0:
            continue
        city = detect_city_from_listing(s)
        if city is None:
            continue
        sales.append(s)

    print(f"Filtered sales (completed, valid sqft): {len(sales)}")
    rentals_for_tracking = []
    for r in rentals_dedup:
        beds = norm_beds(r.get("beds"))
        if beds is None:
            continue
        sqft = norm_sqft(r.get("sqft"))
        if sqft is None:
            continue
        ann = annual_rent(r.get("rent"), r.get("period"))
        if ann is None or ann <= 0:
            continue
        city = detect_city_from_listing(r)
        if city is None:
            continue
        rentals_for_tracking.append(r)

    print(f"Filtered rentals (valid sqft): {len(rentals_for_tracking)}")

    # --- SQLite ---
    snapshot_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    conn = init_db(DB_PATH)
    stored = store_snapshot(conn, sales, snapshot_date)
    print(f"Stored {stored} in snapshot {snapshot_date}")
    n_snap = conn.execute("SELECT COUNT(DISTINCT snapshot_date) FROM listing_history").fetchone()[0]
    print(f"Total snapshots: {n_snap}")
    rental_stored = store_rental_snapshot(conn, rentals_for_tracking, snapshot_date)
    print(f"Stored {rental_stored} rentals in snapshot {snapshot_date}")
    n_rental_snap = conn.execute("SELECT COUNT(DISTINCT snapshot_date) FROM rental_history").fetchone()[0]
    print(f"Total rental snapshots: {n_rental_snap}")
    if n_snap >= 2:
        drops_found = compute_drops(conn, snapshot_date)
        print(f"Price drops detected: {drops_found}")
    if n_rental_snap >= 2:
        rental_drops_found = compute_rental_drops(conn, snapshot_date)
        print(f"Rental drops detected: {rental_drops_found}")

    # --- Build rental indexes ---
    by_building = defaultdict(list)
    by_area = defaultdict(list)
    for r in rentals_dedup:
        sqft = norm_sqft(r.get("sqft"))
        if sqft is None or sqft <= 0:
            continue
        ann = annual_rent(r.get("rent"), r.get("period"))
        if ann is None or ann <= 0:
            continue
        rpsf = ann / sqft
        if rpsf < 30 or rpsf > 300:
            continue
        b = (r.get("building") or "").strip()
        _, area = extract_area(r.get("path"))
        area = area or "Other"
        if b:
            by_building[b].append(rpsf)
        by_area[area].append(rpsf)

    # --- Areas list ---
    area_set = set()
    for s in sales:
        _, area = extract_area(s.get("path"))
        area_set.add(area or "Other")
    areas = ["Other"] + sorted(a for a in area_set if a != "Other")

    # --- Listings output ---
    # [building, area_idx, price, sqft, furnished, grossYield, netYield, tier, conf, scPsf, city, beds, url]
    listings_out = []
    for s in sales:
        building = (s.get("building") or "").strip() or "Unknown"
        city, area = extract_area(s.get("path"))
        area = area or "Other"
        city = city or "Dubai"
        price = float(s.get("price") or 0)
        sqft = norm_sqft(s.get("sqft"))
        if not sqft or price <= 0:
            continue
        beds = norm_beds(s.get("beds"))
        if beds is None:
            continue

        rpsf = None
        conf = "A"
        if building in by_building:
            rpsf = median(by_building[building])
            conf = "B"
        if rpsf is None and area in by_area:
            rpsf = median(by_area[area])
        if rpsf is None:
            continue

        rent_annual = rpsf * sqft
        gross_yield = (rent_annual / price) * 100
        sc_psf = sc_for_area(area)
        net_yield = ((rent_annual - sc_psf * sqft) / price) * 100 if price > 0 else 0
        tier = AREA_TIERS.get(area, DEFAULT_TIER)
        area_idx = areas.index(area) if area in areas else 0
        furnished = norm_furnished(s.get("furnished"))
        url = build_url(s)

        listings_out.append([
            building, area_idx, round(price), sqft, furnished,
            round(gross_yield, 1), round(net_yield, 1), tier, conf, sc_psf,
            city, beds, url,
        ])

    # --- Drops with yields ---
    all_drops = load_all_drops(conn)
    drops_out = []
    for row in all_drops:
        (lid, building, area, city, sqft, beds, furnished,
         first_price, prev_price, cur_price,
         drop_prev, drop_pct_prev, total_drop, total_drop_pct,
         drop_count, first_seen, last_drop_date) = row
        building = building or "Unknown"
        area = area or "Other"
        city = city or "Dubai"
        area_idx = areas.index(area) if area in areas else 0
        tier = AREA_TIERS.get(area, DEFAULT_TIER)
        sc_psf = sc_for_area(area)
        furn_code = norm_furnished(furnished)
        rpsf = None
        if building in by_building:
            rpsf = median(by_building[building])
        if rpsf is None and area in by_area:
            rpsf = median(by_area[area])
        if rpsf and sqft and cur_price > 0:
            rent_annual = rpsf * sqft
            new_gy = round((rent_annual / cur_price) * 100, 1)
            new_ny = round(((rent_annual - sc_psf * sqft) / cur_price) * 100, 1)
            old_gy = round((rent_annual / first_price) * 100, 1) if first_price > 0 else 0
            old_ny = round(((rent_annual - sc_psf * sqft) / first_price) * 100, 1) if first_price > 0 else 0
        else:
            new_gy = new_ny = old_gy = old_ny = 0
        # Find URL from listing_history
        url_row = conn.execute(
            "SELECT path FROM listing_history WHERE listing_id = ? ORDER BY snapshot_date DESC LIMIT 1",
            (lid,)).fetchone()
        url = PF_BASE + (url_row[0] if url_row and url_row[0] else "")

        drops_out.append([
            building, area_idx, sqft or 0, beds or "Studio", furn_code,
            round(first_price), round(prev_price), round(cur_price),
            round(drop_prev), round(drop_pct_prev, 1),
            round(total_drop), round(total_drop_pct, 1), drop_count,
            old_gy, old_ny, new_gy, new_ny, tier, sc_psf,
            first_seen or "", last_drop_date or "", city or "Dubai", url,
        ])
    all_rental_drops = load_all_rental_drops(conn)
    rental_drops_out = []
    for row in all_rental_drops:
        (lid, building, area, city, sqft, beds, furnished,
         first_rent, prev_rent, cur_rent,
         drop_prev, drop_pct_prev, total_drop, total_drop_pct,
         drop_count, first_seen, last_drop_date) = row
        building = building or "Unknown"
        area = area or "Other"
        city = city or "Dubai"
        area_idx = areas.index(area) if area in areas else 0
        furn_code = norm_furnished(furnished)
        tier = AREA_TIERS.get(area, DEFAULT_TIER)
        url_row = conn.execute(
            "SELECT path FROM rental_history WHERE listing_id = ? ORDER BY snapshot_date DESC LIMIT 1",
            (lid,)).fetchone()
        url = PF_BASE + (url_row[0] if url_row and url_row[0] else "")

        rental_drops_out.append([
            building, area_idx, sqft or 0, beds or "Studio", furn_code,
            round(first_rent), round(prev_rent), round(cur_rent),
            round(drop_prev), round(drop_pct_prev, 1),
            round(total_drop), round(total_drop_pct, 1), drop_count,
            first_seen or "", last_drop_date or "", city, url, tier,
        ])
    conn.close()

    # --- Area summaries ---
    area_stats = defaultdict(lambda: {
        "prices": [], "rentals": 0, "bldg": 0,
        "gy": [], "ny": [], "rpsf": [], "rent_annuals": [],
    })
    for row in listings_out:
        name = areas[row[1]]
        area_stats[name]["prices"].append(row[2])
        area_stats[name]["gy"].append(row[5])
        area_stats[name]["ny"].append(row[6])
        if row[8] == "B":
            area_stats[name]["bldg"] += 1
        area_stats[name]["rent_annuals"].append(row[2] * (row[5] / 100))
    for a in by_area:
        area_stats[a]["rentals"] = len(by_area[a])
        area_stats[a]["rpsf"] = by_area[a]

    summaries_out = []
    for name in areas:
        st = area_stats[name]
        count = len(st["prices"])
        if count == 0:
            continue
        prices = st["prices"]
        bldg = st["bldg"]
        avg_price = sum(prices) / count
        med_price = sorted(prices)[len(prices) // 2]
        avg_rent = sum(st["rent_annuals"]) / count if st["rent_annuals"] else 0
        avg_gy = sum(st["gy"]) / count if st["gy"] else 0
        avg_ny = sum(st["ny"]) / count if st["ny"] else 0
        sc_psf = sc_for_area(name)
        tier = AREA_TIERS.get(name, DEFAULT_TIER)
        med_rpsf = round(median(st["rpsf"]), 1) if st["rpsf"] else 0
        # Determine city for this area
        area_city = "Dubai"
        if name in AD_SLUG_TO_AREA.values() or name in [
            "Al Reem Island", "Yas Island", "Saadiyat Island", "Al Maryah Island",
            "Al Raha Beach", "Al Ghadeer", "Al Reef", "Masdar City", "Khalifa City",
            "Al Jubail Island", "Mohamed Bin Zayed City", "Al Khalidiya", "Corniche Road",
            "Al Bateen", "Baniyas", "Al Shamkha", "Al Mushrif", "Madinat Al Riyad",
            "Al Danah", "Al Nahyan", "Hamdan Street", "Muroor Area", "Airport Road",
            "Tourist Club", "The Marina", "City Downtown", "Mussafah", "Al Wahda",
            "Rawdhat", "Al Zahiyah", "Electra Street", "Capital Centre", "Al Falah City", "Rabdan",
        ]:
            area_city = "Abu Dhabi"
        summaries_out.append([
            name, count, st["rentals"], bldg, count - bldg,
            round(avg_price), round(med_price), min(prices), max(prices), round(avg_rent),
            round(avg_gy, 1), round(avg_ny, 1), sc_psf, tier, med_rpsf, area_city,
        ])

    out = {"a": areas, "l": listings_out, "s": summaries_out, "d": drops_out, "rd": rental_drops_out}
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(out, f, separators=(",", ":"))

    # Stats
    city_counts = defaultdict(int)
    beds_counts = defaultdict(int)
    tier_counts = defaultdict(int)
    for row in listings_out:
        city_counts[row[10]] += 1
        beds_counts[row[11]] += 1
        tier_counts[row[7]] += 1
    print(
        f"\nOutput: {len(listings_out)} listings, {len(summaries_out)} areas, "
        f"{len(drops_out)} price drops, {len(rental_drops_out)} rental drops"
    )
    print(f"Cities: {dict(city_counts)}")
    print(f"Unit types: {dict(beds_counts)}")
    print(f"Tiers: {dict(tier_counts)}")


if __name__ == "__main__":
    main()
