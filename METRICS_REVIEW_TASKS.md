# Metrics & Calculation Review — Task List

Prioritized remediation tasks from the review of all sale, rent, service-charge, and yield
calculations across the pipeline (`scripts/fetch_propertyfinder_listings.py` →
`process_data.py` → `config/area_tiers.py` → `index.html`).

Status legend: `[ ]` todo · `[~]` in progress · `[x]` done

---

## High priority — correctness of the critical sale / rent / yield numbers

- [x] **H1. Fix area assignment (root cause for comps, tiers, service charge)**
  - Slug matching now tries a prefix match first, then matches a known area
    anywhere in the slug on hyphen boundaries (handles building-name-first URLs).
  - Each run logs the count/percentage of sales unresolved to a known area.
  - Files: `process_data.py` (`_bounded_index`, `_match_area_slug`, `extract_area`).

- [x] **H2. Make "building match" confidence reflect sample size**
  - `MIN_BUILDING_COMPS = 3`: building median is used only with ≥3 same-building
    comps, otherwise falls back to the area pool.
  - Rent comp count `n` is carried into `dashboard_data.json` (listing index 17)
    and surfaced in the UI (dot colour + `n=` label + tooltips).
  - Files: `process_data.py` (`pick_rent_psf`), `index.html`.

- [x] **H3. Stop pooling furnished + unfurnished rents**
  - `build_rent_comps` builds furnished/unfurnished × building/area pools;
    `pick_rent_psf` prefers same-furnishing comps before mixed fallbacks.
  - Files: `process_data.py` (`furn_class`, `build_rent_comps`, `pick_rent_psf`).

- [x] **H4. Redefine / relabel "Net yield"**
  - `compute_net_yield` deducts service charge plus configurable `VACANCY_RATE`
    and `MANAGEMENT_RATE` (default 0.0, so current numbers are unchanged until
    opted in). UI/README/footer updated to state what is deducted and that
    figures are asking-side.
  - Files: `config/area_tiers.py`, `process_data.py`, `index.html`, `README.md`.

---

## Medium priority

- [x] **M1. Tighten yield / PSF guardrails and "best" KPI**
  - Plausibility bands tightened: gross ≤ 15%, net ≤ 13% (was 35% / 30%).
  - "Top Gross Yield" KPI now uses only listings backed by ≥3 same-building rent
    comps, so a single thin/mispriced comp cannot headline the dashboard.
  - Files: `process_data.py` (guardrail constants), `index.html` (`mgy`/`mgyUrl`, label).

- [x] **M3. Align sqft filter with documentation**
  - `norm_sqft` floor raised from 100 → 300 sqft; README states 300–2,500.
  - Files: `process_data.py`, `README.md`.

- ~~**M2. Source real per-building service charges**~~ — *Removed (not doing now).*

---

## Low priority / hygiene

- [x] **L1. Safer unknown rent-period handling**
  - Unknown/blank periods now treated as yearly (was ×12); month/week/day handled
    explicitly. Files: `process_data.py` (`annual_rent`).

- [x] **L2. Reconcile "avg" vs "median" wording**
  - README headline/area-table wording aligned to median (and "top" gross yield);
    KPI labels and column headers already use median. Files: `README.md`, `index.html`.

- [x] **L3. Clarify estimated/synthetic metrics in the UI**
  - Info tooltips added on the area "Median Rent" column (implied, not observed),
    the "Bldg%" column, and the Price Indices header (median-of-pool, rebased).
  - Files: `index.html`.

- [x] **L4. Re-listing / duplicate-unit awareness**
  - `build_rent_comps` collapses exact duplicate units
    (city, area, beds, building, sqft, annual rent) before building comp pools.
  - Files: `process_data.py`.

- [x] **L5. Asking-side disclaimer**
  - Global note added under the dashboard subtitle and in the footer.
  - Files: `index.html` (also noted in `README.md`).

---

## Reference: evidence from original review (1,000 sales / 1,251 rentals sample)

- 100% of listings had an empty `area` field (area is slug-derived).
- 3% of sales resolved to "Other"; failures included clearly-identifiable areas (Arjan, DIP).
- Building comp groups: median size 1, 73% singletons.
- Rent/sqft: furnished median 135.8 vs unfurnished 116.8 (~16% gap).
- Gross yield distribution: p25 6.1% · median 7.0% · p75 8.1% · p95 10.6% · max 22.0%.
- Original guardrails (35% gross cap, etc.) caught 0 listings — too loose to protect KPIs.
