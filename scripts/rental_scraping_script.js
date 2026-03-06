(async () => {
  const PF_BASE = window.location.origin || 'https://www.propertyfinder.ae';
  const doc = document.querySelector('#__NEXT_DATA__');
  const firstPage = JSON.parse(doc.textContent);
  const perPage = firstPage.props.pageProps.searchResult.properties.length;
  
  const toFullUrl = (path) => {
    if (!path) return '';
    if (String(path).startsWith('http')) return path;
    return PF_BASE + (String(path).startsWith('/') ? path : `/${path}`);
  };

  const extract = (p) => ({
    id: p.id,
    rent: p.price?.value,
    period: p.price?.period,
    beds: p.bedrooms,
    sqft: p.size?.value,
    furnished: p.furnished,
    building: p.location?.name,
    area: (p.location?.location_tree?.find(n => n.type === 'COMMUNITY') || {}).name || '',
    sub: (p.location?.location_tree?.find(n => n.type === 'SUBCOMMUNITY') || {}).name || '',
    path: p.details_path,
    url: toFullUrl(p.details_path),
  });

  const inferCity = (listings) => {
    const counts = {};
    for (const listing of listings) {
      const p = String(listing.path || '').toLowerCase();
      let city = null;
      if (p.includes('-dubai-')) city = 'Dubai';
      if (p.includes('-abu-dhabi-')) city = 'Abu Dhabi';
      if (city) counts[city] = (counts[city] || 0) + 1;
    }
    const best = Object.entries(counts).sort((a, b) => b[1] - a[1])[0];
    return best ? best[0] : 'Unknown City';
  };

  const inferPropertyType = (listings) => {
    const rx = /(apartment|townhouse|villa|penthouse|duplex)-for-(sale|rent)/i;
    const counts = {};
    for (const listing of listings.slice(0, 300)) {
      const match = String(listing.path || '').match(rx);
      if (!match) continue;
      const key = match[1].toLowerCase();
      counts[key] = (counts[key] || 0) + 1;
    }
    const best = Object.entries(counts).sort((a, b) => b[1] - a[1])[0];
    return best ? best[0] : 'unit';
  };

  const normalizeBedLabel = (value) => {
    const s = String(value || '').trim().toLowerCase();
    if (s === 'studio' || s === '0') return 'studio';
    if (/^\d+$/.test(s)) return `${s} bedroom`;
    return 'unit';
  };

  const inferUnitType = (listings) => {
    const bedCounts = {};
    for (const listing of listings) {
      const label = normalizeBedLabel(listing.beds);
      bedCounts[label] = (bedCounts[label] || 0) + 1;
    }
    const topBed = Object.entries(bedCounts).sort((a, b) => b[1] - a[1])[0]?.[0] || 'unit';
    const propertyType = inferPropertyType(listings);
    if (propertyType === 'apartment' || propertyType === 'unit') return topBed;
    if (topBed.endsWith('bedroom') || topBed === 'studio') return `${topBed} ${propertyType}`;
    return propertyType;
  };

  const sanitizeFilename = (name) =>
    name
      .replace(/[<>:"/\\|?*]+/g, '')
      .replace(/\s+/g, ' ')
      .trim();

  const buildOutputFilename = (listings) => {
    const city = inferCity(listings);
    const unitType = inferUnitType(listings);
    const listingType = 'rental';
    return sanitizeFilename(`${city} - ${unitType} - ${listingType} data.json`);
  };
  
  let all = firstPage.props.pageProps.searchResult.properties.map(extract);
  const totalText = document.body.innerText.match(/([\d,]+)\s*properties/);
  const totalListings = totalText ? parseInt(totalText[1].replace(/,/g, '')) : 0;
  const totalPages = Math.ceil(totalListings / perPage);
  
  console.log(`🏠 RENTALS — SLIM EXTRACT`);
  console.log(`📊 ${totalListings} rentals, ${totalPages} pages`);
  console.log(`⏱️ ~${Math.round(totalPages * 1.2 / 60)} minutes`);
  
  const baseUrl = window.location.href.split('?')[0];
  const urlParams = new URLSearchParams(window.location.search);
  let failures = 0;
  
  for (let page = 2; page <= totalPages; page++) {
    let success = false;
    for (let attempt = 1; attempt <= 3 && !success; attempt++) {
      try {
        urlParams.set('page', page);
        const resp = await fetch(baseUrl + '?' + urlParams.toString());
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const html = await resp.text();
        const match = html.match(/<script id="__NEXT_DATA__"[^>]*>(.*?)<\/script>/s);
        if (match) {
          const props = JSON.parse(match[1]).props.pageProps.searchResult.properties;
          if (props?.length) { all.push(...props.map(extract)); success = true; }
        }
        if (!success && attempt < 3) await new Promise(r => setTimeout(r, 3000));
      } catch (e) {
        if (attempt < 3) await new Promise(r => setTimeout(r, 3000));
      }
    }
    if (!success) { failures++; if (failures >= 10) { console.log(`🛑 Stopping.`); break; } }
    if (page % 20 === 0) console.log(`📦 ${page}/${totalPages} (${Math.round(page/totalPages*100)}%) — ${all.length} rentals`);
    await new Promise(r => setTimeout(r, page % 50 === 0 ? 3000 : 1000));
  }
  
  const city = inferCity(all);
  const unitType = inferUnitType(all);
  const filename = buildOutputFilename(all);
  const blob = new Blob([JSON.stringify({
    type: "rentals", extracted_at: new Date().toISOString(),
    city, unit_type: unitType, listing_type: "rental",
    source_url: window.location.href,
    total: all.length, expected: totalListings, listings: all
  })], { type: 'application/json' });
  const a = document.createElement('a'); a.href = URL.createObjectURL(blob);
  a.download = filename; a.click();
  console.log(`✅ Done! ${all.length} rental listings. Saved as: ${filename}`);
})();
