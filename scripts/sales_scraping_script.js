(async () => {
  const doc = document.querySelector('#__NEXT_DATA__');
  const firstPage = JSON.parse(doc.textContent);
  const perPage = firstPage.props.pageProps.searchResult.properties.length;
  
  const extract = (p) => ({
    id: p.id,
    price: p.price?.value,
    beds: p.bedrooms,
    sqft: p.size?.value,
    furnished: p.furnished,
    building: p.location?.name,
    area: (p.location?.location_tree?.find(n => n.type === 'COMMUNITY') || {}).name || '',
    sub: (p.location?.location_tree?.find(n => n.type === 'SUBCOMMUNITY') || {}).name || '',
    completion: p.completion_status,
    path: p.details_path,
  });
  
  let all = firstPage.props.pageProps.searchResult.properties.map(extract);
  const totalText = document.body.innerText.match(/([\d,]+)\s*properties/);
  const totalListings = totalText ? parseInt(totalText[1].replace(/,/g, '')) : 0;
  const totalPages = Math.ceil(totalListings / perPage);
  
  console.log(`🏷️ SALES — SLIM EXTRACT`);
  console.log(`📊 ${totalListings} listings, ${totalPages} pages`);
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
    if (page % 20 === 0) console.log(`📦 ${page}/${totalPages} (${Math.round(page/totalPages*100)}%) — ${all.length} listings`);
    await new Promise(r => setTimeout(r, page % 50 === 0 ? 3000 : 1000));
  }
  
  const blob = new Blob([JSON.stringify({
    type: "sales", extracted_at: new Date().toISOString(),
    total: all.length, expected: totalListings, listings: all
  })], { type: 'application/json' });
  const a = document.createElement('a'); a.href = URL.createObjectURL(blob);
  a.download = `pf_sales_slim_${new Date().toISOString().slice(0,10)}.json`; a.click();
  console.log(`✅ Done! ${all.length} sales listings. File should be ~3-5 MB.`);
})();
