from __future__ import annotations

UI_HTML = """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>SCEI Investigation Console</title>
<style>
body{font-family:system-ui;margin:2rem;max-width:1100px;color:#18212b} main{display:grid;gap:1rem} form,.card{border:1px solid #d8dee6;border-radius:10px;padding:1rem;background:#fff} label{display:grid;gap:.25rem;margin:.5rem 0} input,select,button{font:inherit;padding:.5rem;border:1px solid #aab4c0;border-radius:6px}button{background:#135d8c;color:white;cursor:pointer}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:.75rem}pre{white-space:pre-wrap;overflow:auto;background:#f5f7fa;padding:1rem;border-radius:6px}.muted{color:#657384}body{background:#f0f3f6}
</style></head><body><main>
<header><h1>SCEI Investigation Console</h1><p class="muted">Context-aware pharmaceutical batch genealogy and contamination investigation.</p></header>
<form id="query"><div class="grid"><label>Organization ID<input id="org" required placeholder="UUID"></label><label>Context ID<input id="context" placeholder="Optional projection context UUID"></label><label>Batch code<input id="batch" required placeholder="BATCH-BD-00000111"></label><label>Maximum hops<input id="hops" type="number" min="1" max="100" value="5"></label></div><button>Investigate batch</button></form>
<form id="incident"><div class="grid"><label>Organization ID<input id="incidentOrg" required placeholder="UUID"></label><label>Incident ID<input id="incidentId" required placeholder="INC-BD-20261031-0002"></label><label>Context ID<input id="incidentContext" placeholder="Optional projection context UUID"></label></div><button>Run incident report</button></form>
<section class="card"><h2>Result</h2><div id="status" class="muted">No investigation run yet.</div><pre id="result">{}</pre></section>
</main><script>
const $=id=>document.getElementById(id); const show=(status,data)=>{$('status').textContent=status;$('result').textContent=JSON.stringify(data,null,2)};
async function call(url,options){const r=await fetch(url,options);const d=await r.json();if(!r.ok)throw new Error(d.detail||r.statusText);return d}
$('query').onsubmit=async e=>{e.preventDefault();try{show('Running…',{});const q={organization_id:$('org').value,batch_query:$('batch').value,max_hops:+$('hops').value,max_paths:500,include_possible:false};if($('context').value)q.context_id=$('context').value;show('Complete',await call('/v1/investigations/batch',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(q)}));}catch(x){show('Error',{error:x.message})}};
$('incident').onsubmit=async e=>{e.preventDefault();try{show('Running…',{});let q={organization_id:$('incidentOrg').value,incident_id:$('incidentId').value,max_hops:+$('hops').value,max_paths:500,include_possible:false};if($('incidentContext').value)q.context_id=$('incidentContext').value;show('Complete',await call('/v1/investigations/incident',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(q)}));}catch(x){show('Error',{error:x.message})}};
</script></body></html>"""
