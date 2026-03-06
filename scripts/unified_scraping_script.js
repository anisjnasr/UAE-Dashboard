(async () => {
  const PF_BASE = window.location.origin || "https://www.propertyfinder.ae";
  const doc = document.querySelector("#__NEXT_DATA__");
  if (!doc) {
    console.error("Could not find __NEXT_DATA__. Open a Property Finder search results page first.");
    return;
  }

  const firstPage = JSON.parse(doc.textContent);
  const firstProps = firstPage?.props?.pageProps?.searchResult?.properties || [];
  if (!Array.isArray(firstProps) || firstProps.length === 0) {
    console.error("No listings found on the current page.");
    return;
  }

  const perPage = firstProps.length;

  const toFullUrl = (path) => {
    if (!path) return "";
    if (String(path).startsWith("http")) return path;
    return PF_BASE + (String(path).startsWith("/") ? path : `/${path}`);
  };

  const cityFromPath = (path) => {
    const p = String(path || "").toLowerCase();
    if (p.includes("-dubai-") || p.includes("/dubai/")) return "Dubai";
    if (p.includes("-abu-dhabi-") || p.includes("/abu-dhabi/")) return "Abu Dhabi";
    return null;
  };

  const unitTypeFromBeds = (value) => {
    const s = String(value || "").trim().toLowerCase();
    if (s === "studio" || s === "0") return "Studio";
    if (/^\d+$/.test(s)) return `${s}BR`;
    return "Unknown";
  };

  const detectListingType = (listings) => {
    const pathName = String(window.location.pathname || "").toLowerCase();
    if (pathName.includes("/rent")) return "rental";
    if (pathName.includes("/buy") || pathName.includes("/sale")) return "sales";

    const pageTitle = String(firstPage?.props?.pageProps?.meta?.title || "").toLowerCase();
    if (pageTitle.includes("rent")) return "rental";
    if (pageTitle.includes("buy") || pageTitle.includes("sale")) return "sales";

    let rentalVotes = 0;
    let salesVotes = 0;
    for (const listing of listings.slice(0, 100)) {
      const period = String(listing?.price?.period || "").toLowerCase();
      const p = String(listing?.details_path || "").toLowerCase();
      if (period.includes("month") || period.includes("year") || period.includes("annual")) rentalVotes += 1;
      if (p.includes("/rent/")) rentalVotes += 1;
      if (p.includes("/buy/") || p.includes("/sale/")) salesVotes += 1;
    }
    return rentalVotes > salesVotes ? "rental" : "sales";
  };

  const inferCity = (rows) => {
    const counts = {};
    for (const row of rows) {
      const city = cityFromPath(row.path);
      if (city) counts[city] = (counts[city] || 0) + 1;
    }
    const entries = Object.entries(counts).sort((a, b) => b[1] - a[1]);
    if (entries.length > 1) return "Multiple Cities";
    const best = entries[0];
    return best ? best[0] : "Unknown City";
  };

  const inferPropertyType = (rows) => {
    const rx = /(apartment|townhouse|villa|penthouse|duplex)-for-(sale|rent)/i;
    const counts = {};
    for (const row of rows.slice(0, 300)) {
      const match = String(row.path || "").match(rx);
      if (!match) continue;
      const key = match[1].toLowerCase();
      counts[key] = (counts[key] || 0) + 1;
    }
    const best = Object.entries(counts).sort((a, b) => b[1] - a[1])[0];
    return best ? best[0] : "unit";
  };

  const normalizeBedLabel = (value) => {
    const s = String(value || "").trim().toLowerCase();
    if (s === "studio" || s === "0") return "studio";
    if (/^\d+$/.test(s)) return `${s} bedroom`;
    return "unit";
  };

  const inferUnitType = (rows) => {
    const bedCounts = {};
    for (const row of rows) {
      const label = normalizeBedLabel(row.beds);
      bedCounts[label] = (bedCounts[label] || 0) + 1;
    }
    const bedEntries = Object.entries(bedCounts).sort((a, b) => b[1] - a[1]);
    if (bedEntries.length > 1) return "multiple unit types";
    const topBed = bedEntries[0]?.[0] || "unit";
    const propertyType = inferPropertyType(rows);
    if (propertyType === "apartment" || propertyType === "unit") return topBed;
    if (topBed.endsWith("bedroom") || topBed === "studio") return `${topBed} ${propertyType}`;
    return propertyType;
  };

  const sanitizeFilename = (name) =>
    name
      .replace(/[<>:"/\\|?*]+/g, "")
      .replace(/\s+/g, " ")
      .trim();

  const listingType = detectListingType(firstProps);
  console.log(`Detected listing type: ${listingType}`);

  const extract = (p) => {
    const path = p.details_path;
    const base = {
      id: p.id,
      beds: p.bedrooms,
      unit_type: unitTypeFromBeds(p.bedrooms),
      sqft: p.size?.value,
      furnished: p.furnished,
      building: p.location?.name,
      area: (p.location?.location_tree?.find((n) => n.type === "COMMUNITY") || {}).name || "",
      sub: (p.location?.location_tree?.find((n) => n.type === "SUBCOMMUNITY") || {}).name || "",
      city: cityFromPath(path) || "",
      path,
      url: toFullUrl(path),
    };

    if (listingType === "rental") {
      return {
        ...base,
        rent: p.price?.value,
        period: p.price?.period,
      };
    }

    return {
      ...base,
      price: p.price?.value,
      completion: p.completion_status,
    };
  };

  let all = firstProps.map(extract);
  const totalText = document.body.innerText.match(/([\d,]+)\s*properties/);
  const totalListings = totalText ? parseInt(totalText[1].replace(/,/g, ""), 10) : 0;
  const totalPages = Math.ceil(totalListings / perPage);

  console.log(`📊 ${totalListings} listings, ${totalPages} pages`);
  console.log(`⏱️ ~${Math.round((totalPages * 1.2) / 60)} minutes`);

  const baseUrl = window.location.href.split("?")[0];
  const urlParams = new URLSearchParams(window.location.search);
  let failures = 0;

  const buildPayload = (listings) => {
    const city = inferCity(listings);
    const unitType = inferUnitType(listings);
    const fileKind = listingType === "rental" ? "rental" : "sales";
    return {
      type: listingType === "rental" ? "rentals" : "sales",
      extracted_at: new Date().toISOString(),
      city,
      unit_type: unitType,
      listing_type: fileKind,
      source_url: window.location.href,
      total: listings.length,
      expected: totalListings,
      listings,
    };
  };

  const buildFilename = (listings) => {
    const city = inferCity(listings);
    const unitType = inferUnitType(listings);
    const fileKind = listingType === "rental" ? "rental" : "sales";
    return sanitizeFilename(`${city} - ${unitType} - ${fileKind} data.json`);
  };

  const downloadJson = (payload, filename) => {
    const blob = new Blob([JSON.stringify(payload)], { type: "application/json" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = filename;
    a.click();
    setTimeout(() => URL.revokeObjectURL(a.href), 5000);
  };

  for (let page = 2; page <= totalPages; page++) {
    let success = false;
    for (let attempt = 1; attempt <= 3 && !success; attempt++) {
      try {
        urlParams.set("page", page);
        const resp = await fetch(`${baseUrl}?${urlParams.toString()}`);
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const html = await resp.text();
        const match = html.match(/<script id="__NEXT_DATA__"[^>]*>(.*?)<\/script>/s);
        if (match) {
          const props = JSON.parse(match[1])?.props?.pageProps?.searchResult?.properties;
          if (props?.length) {
            all.push(...props.map(extract));
            success = true;
          }
        }
        if (!success && attempt < 3) await new Promise((r) => setTimeout(r, 3000));
      } catch (e) {
        if (attempt < 3) await new Promise((r) => setTimeout(r, 3000));
      }
    }
    if (!success) {
      failures++;
      if (failures >= 10) {
        console.log("🛑 Stopping after too many page failures.");
        break;
      }
    }
    if (page % 20 === 0) {
      console.log(`📦 ${page}/${totalPages} (${Math.round((page / totalPages) * 100)}%) — ${all.length} listings`);
    }
    await new Promise((r) => setTimeout(r, page % 50 === 0 ? 3000 : 1000));
  }
  const finalPayload = buildPayload(all);
  const finalFilename = buildFilename(all);
  downloadJson(finalPayload, finalFilename);
  console.log(`✅ Done! ${all.length} listings. Final file: ${finalFilename}`);
})();
