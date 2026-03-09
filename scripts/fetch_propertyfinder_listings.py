"""
Fetch Property Finder listings from configured sales/rental search URLs.

Outputs JSON files directly into data/ using:
  Multiple Cities - multiple unit types - sales data.json
  Multiple Cities - multiple unit types - rental data.json
"""

import argparse
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
from urllib.request import Request, urlopen


DEFAULT_SALES_URL = (
    "https://www.propertyfinder.ae/en/search?l=6-1&c=1&t=1&bdr[]=0&bdr[]=1"
    "&bdr[]=2&pt=3000000&fu=0&cs=completed&ob=mr"
)
DEFAULT_RENTAL_URL = (
    "https://www.propertyfinder.ae/en/search?l=6-1&c=2&t=1&bdr[]=0&bdr[]=1"
    "&bdr[]=2&fu=0&rp=y&ob=mr"
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


def fetch_html(url, retries=3, delay_s=2):
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            req = Request(url, headers=HEADERS)
            with urlopen(req, timeout=45) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except Exception as exc:
            last_exc = exc
            if attempt < retries:
                time.sleep(delay_s)
    raise RuntimeError(f"Failed to fetch URL after {retries} attempts: {url}") from last_exc


def extract_next_data(html):
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.S)
    if not m:
        raise RuntimeError("Could not find __NEXT_DATA__ payload in HTML")
    return json.loads(m.group(1))


def page_url(base_url, page):
    p = urlparse(base_url)
    query_pairs = [(k, v) for (k, v) in parse_qsl(p.query, keep_blank_values=True) if k != "page"]
    if page > 1:
        query_pairs.append(("page", str(page)))
    new_q = urlencode(query_pairs, doseq=True)
    return urlunparse((p.scheme, p.netloc, p.path, p.params, new_q, p.fragment))


def city_from_path(path):
    s = str(path or "").lower()
    if "-dubai-" in s or "/dubai/" in s:
        return "Dubai"
    if "-abu-dhabi-" in s or "/abu-dhabi/" in s:
        return "Abu Dhabi"
    return ""


def unit_type_from_beds(value):
    s = str(value or "").strip().lower()
    if s in ("studio", "0"):
        return "Studio"
    if s == "1":
        return "1BR"
    if s == "2":
        return "2BR"
    return "Other"


def extract_listing(p):
    path = p.get("details_path") or ""
    location_tree = p.get("location", {}).get("location_tree") or []
    community = next((n.get("name") for n in location_tree if n.get("type") == "COMMUNITY"), "") or ""
    subcommunity = next((n.get("name") for n in location_tree if n.get("type") == "SUBCOMMUNITY"), "") or ""
    out = {
        "id": p.get("id"),
        "beds": p.get("bedrooms"),
        "unit_type": unit_type_from_beds(p.get("bedrooms")),
        "sqft": (p.get("size") or {}).get("value"),
        "furnished": p.get("furnished"),
        "building": (p.get("location") or {}).get("name"),
        "area": community,
        "sub": subcommunity,
        "city": city_from_path(path),
        "path": path,
        "url": f"https://www.propertyfinder.ae{path}" if path else "",
        "completion": p.get("completion_status"),
        "price": (p.get("price") or {}).get("value"),
        "period": (p.get("price") or {}).get("period"),
    }
    return out


def infer_unit_type(all_rows):
    counts = {}
    for row in all_rows:
        u = row.get("unit_type") or "Other"
        counts[u] = counts.get(u, 0) + 1
    if len(counts) > 1:
        return "multiple unit types"
    return next(iter(counts), "unknown unit type")


def infer_city(all_rows):
    counts = {}
    for row in all_rows:
        c = row.get("city") or ""
        if c:
            counts[c] = counts.get(c, 0) + 1
    if len(counts) > 1:
        return "Multiple Cities"
    return next(iter(counts), "Unknown City")


def run_query(kind, url, out_dir, max_pages=0):
    html = fetch_html(url)
    first = extract_next_data(html)
    first_props = (((first.get("props") or {}).get("pageProps") or {}).get("searchResult") or {}).get("properties") or []
    if not first_props:
        raise RuntimeError(f"No properties returned on first page for {kind}")

    per_page = len(first_props)
    total_match = re.search(r"([\d,]+)\s*properties", html, re.I)
    total_expected = int(total_match.group(1).replace(",", "")) if total_match else 0
    total_pages = max(1, (total_expected + per_page - 1) // per_page) if total_expected else 1
    if max_pages and max_pages > 0:
        total_pages = min(total_pages, max_pages)

    rows = [extract_listing(x) for x in first_props]
    failures = 0

    for page in range(2, total_pages + 1):
        try:
            page_html = fetch_html(page_url(url, page))
            page_data = extract_next_data(page_html)
            page_props = (
                (((page_data.get("props") or {}).get("pageProps") or {}).get("searchResult") or {}).get("properties")
                or []
            )
            if not page_props:
                failures += 1
                if failures >= 10:
                    break
                continue
            rows.extend(extract_listing(x) for x in page_props)
            if page % 20 == 0:
                print(f"{kind}: page {page}/{total_pages}, rows={len(rows)}")
            time.sleep(1)
        except Exception:
            failures += 1
            if failures >= 10:
                break
            time.sleep(2)

    # Keep only apartment studios/1BR/2BR to match tracking scope.
    tracked = [r for r in rows if r.get("unit_type") in {"Studio", "1BR", "2BR"}]
    city = infer_city(tracked)
    unit_type = infer_unit_type(tracked)
    listing_type = "sales" if kind == "sales" else "rental"
    out_name = f"{city} - {unit_type} - {listing_type} data.json"
    payload = {
        "type": "sales" if kind == "sales" else "rentals",
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        "city": city,
        "unit_type": unit_type,
        "listing_type": listing_type,
        "source_url": url,
        "total": len(tracked),
        "expected": total_expected,
        "listings": [
            (
                {
                    **r,
                    "price": r["price"],
                    "completion": r["completion"],
                }
                if kind == "sales"
                else {
                    **r,
                    "rent": r["price"],
                    "period": r["period"],
                }
            )
            for r in tracked
        ],
    }

    # Remove fields not needed per listing type
    for row in payload["listings"]:
        if kind == "sales":
            row.pop("period", None)
        else:
            row.pop("completion", None)
            row.pop("price", None)

    out_path = out_dir / out_name
    out_path.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    print(f"{kind}: wrote {len(payload['listings'])} listings to {out_path}")
    return out_path


def main():
    parser = argparse.ArgumentParser(description="Fetch listings from Property Finder search URLs.")
    parser.add_argument("--sales-url", default=DEFAULT_SALES_URL)
    parser.add_argument("--rental-url", default=DEFAULT_RENTAL_URL)
    parser.add_argument("--only", choices=["sales", "rental", "both"], default="both")
    parser.add_argument("--max-pages", type=int, default=0, help="For testing; 0 means all pages.")
    parser.add_argument(
        "--out-dir",
        default=str(Path(__file__).resolve().parents[1] / "data"),
        help="Output directory for JSON files.",
    )
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.only in ("sales", "both"):
        run_query("sales", args.sales_url, out_dir, max_pages=args.max_pages)
    if args.only in ("rental", "both"):
        run_query("rental", args.rental_url, out_dir, max_pages=args.max_pages)


if __name__ == "__main__":
    main()
