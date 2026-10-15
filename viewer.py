"""Local evidence viewer for the BeSafe assessment.

    python viewer.py            # then open http://127.0.0.1:8765

Reads evidence/ written by run_demo.py. The "Re-run demo" and "Run tests"
buttons execute the real scripts and show their raw output. Standard library
only; binds to localhost.
"""

import json
import os
import subprocess
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
EVIDENCE = os.path.join(HERE, "evidence")
PORT = int(os.environ.get("PORT", "8765"))


def load(name):
    with open(os.path.join(EVIDENCE, name), encoding="utf-8") as f:
        return json.load(f)


def evidence():
    if not os.path.exists(os.path.join(EVIDENCE, "results.json")):
        run([sys.executable, "run_demo.py"])
    return dict(results=load("results.json"), steps=load("steps.json"), snapshots=load("snapshots.json"),
                trace=load("trace-evt-B-201.json"), variants=load("variants.json"))


def run(cmd):
    p = subprocess.run(cmd, cwd=HERE, capture_output=True, text=True, encoding="utf-8", errors="replace")
    return dict(cmd=" ".join(cmd[1:]), exit=p.returncode, output=(p.stdout + p.stderr)[-60000:])


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, body, ctype):
        data = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path == "/":
            self._send(200, PAGE, "text/html; charset=utf-8")
        elif self.path == "/api/evidence":
            self._send(200, json.dumps(evidence(), default=str), "application/json")
        else:
            self._send(404, "not found", "text/plain")

    def do_POST(self):
        if self.path == "/api/run-demo":
            self._send(200, json.dumps(run([sys.executable, "run_demo.py"])), "application/json")
        elif self.path == "/api/run-tests":
            self._send(200, json.dumps(run([sys.executable, "-m", "unittest", "discover", "-s", "tests", "-t", ".",
                                            "-v"])), "application/json")
        else:
            self._send(404, "not found", "text/plain")

    def log_message(self, fmt, *args):
        pass


PAGE = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>BeSafe Assessment Evidence</title>
<style>
:root{--bg:#f6f7f9;--panel:#fff;--ink:#16181d;--muted:#5d6472;--line:#e3e6eb;--accent:#1f5eff;
--pass:#0f7a3d;--passbg:#e6f6ec;--fail:#b42318;--failbg:#fdecea;--chg:#fff4c2;--new:#dff5e6;--warn:#8a5a00;--warnbg:#fff3d6;
--mono:ui-monospace,SFMono-Regular,Consolas,monospace}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:15px/1.5 system-ui,-apple-system,Segoe UI,Roboto,sans-serif}
header{background:#101828;color:#fff;padding:18px 28px;display:flex;gap:20px;align-items:center;flex-wrap:wrap}
header h1{font-size:19px;margin:0;font-weight:650}header .sub{color:#b9c2d3;font-size:13px}
header .sp{flex:1}button{font:inherit;font-size:14px;border:1px solid #3b4558;background:#1d2738;color:#fff;padding:7px 14px;border-radius:7px;cursor:pointer}
button:hover{background:#28344a}button.primary{background:var(--accent);border-color:var(--accent)}button:disabled{opacity:.5;cursor:wait}
nav{display:flex;gap:2px;padding:0 28px;background:var(--panel);border-bottom:1px solid var(--line);overflow-x:auto}
nav a{padding:12px 16px;color:var(--muted);text-decoration:none;border-bottom:2px solid transparent;white-space:nowrap;cursor:pointer}
nav a.on{color:var(--ink);border-color:var(--accent);font-weight:600}
main{padding:22px 28px;max-width:1500px}section{display:none}section.on{display:block}
.card{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:16px 18px;margin-bottom:16px}
.card h2{font-size:16px;margin:0 0 10px}.card h3{font-size:14px;margin:14px 0 6px;color:var(--muted);font-weight:600}
table{border-collapse:collapse;width:100%;font-size:13px}th,td{text-align:left;padding:6px 9px;border-bottom:1px solid var(--line);vertical-align:top}
th{color:var(--muted);font-weight:600;background:#fafbfc;position:sticky;top:0}td.m,.m{font-family:var(--mono);font-size:12.5px}
td{max-width:420px;overflow-wrap:anywhere}td:first-child,th:first-child{white-space:nowrap}tr.new td{background:var(--new)}td.chg{background:var(--chg)}
.chip{white-space:nowrap;display:inline-block;padding:2px 9px;border-radius:999px;font-size:12px;font-weight:650}
.pass{background:var(--passbg);color:var(--pass)}.fail{background:var(--failbg);color:var(--fail)}.warn{background:var(--warnbg);color:var(--warn)}
.big{font-size:30px;font-weight:700}.row{display:flex;gap:16px;flex-wrap:wrap}.row>.card{flex:1;min-width:230px}
.tl{border-left:2px solid var(--line);margin-left:8px}.tl .e{position:relative;padding:6px 0 10px 18px}
.tl .e:before{content:"";position:absolute;left:-6px;top:12px;width:10px;height:10px;border-radius:50%;background:var(--accent)}
.tl .e.bad:before{background:var(--fail)}.tl .e.good:before{background:var(--pass)}.tl .e.hold:before{background:#d29200}
.tl .t{font-family:var(--mono);font-size:12px;color:var(--muted)}.tl .s{font-weight:600}.tl pre{margin:4px 0 0}
pre{font-family:var(--mono);font-size:12.5px;background:#0f1522;color:#d7deea;padding:12px 14px;border-radius:8px;overflow:auto;white-space:pre-wrap;max-height:70vh}
.stages{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:14px}.stages button{background:#fff;color:var(--ink);border-color:var(--line)}
.stages button.on{background:var(--accent);color:#fff;border-color:var(--accent)}
.legend{font-size:12.5px;color:var(--muted)}.legend span{padding:1px 6px;border-radius:4px;margin:0 4px}
.chain .n{border:1px solid var(--line);border-radius:8px;padding:10px 12px;margin:0 0 6px;background:#fff}
.chain .arrow{color:var(--muted);text-align:center;margin:-2px 0 4px}.k{color:var(--muted);font-size:12px;text-transform:uppercase;letter-spacing:.04em}
.lab td:first-child{font-weight:600;white-space:nowrap}
</style></head><body>
<header><div><h1>BeSafe integration assessment: failure &amp; recovery evidence</h1>
<div class="sub">Synthetic data · worker logic real (SQLite, real threads) · ServiceM8 / GHL / Graph / AI simulated · deterministic clock</div></div>
<div class="sp"></div><button id="btnTests">Run tests</button><button class="primary" id="btnDemo">Re-run demo</button></header>
<nav id="nav"></nav><main id="main"></main>
<script>
const TABS=[["overview","Overview & results"],["timeline","Timeline"],["snapshots","Before / after snapshots"],["trace","Event trace"],["variants","Crash points & limits"],["console","Console"]];
const STAGES={"1_before":"1 · Before processing","2_after_failures_delivery":"2 · After timeout + crash","3_after_failures_all":"3 · After all failures (outage)","4_after_automated_recovery":"4 · After automated recovery","5_after_human_review":"5 · After human review"};
const KEYS={servicem8_jobs:"job_number",store_jobs:"job_number",inbound_events:"event_id",outbox:"id",followups:"id",mailbox_cursor:"mailbox",inbound_emails:"graph_id",ai_extractions:"graph_id",review_tasks:"id",contacts:"id",opportunities:"id",messages:"id",outlook_dropped_notifications:"graph_id"};
let D=null,stage="1_before",consoleOut="Press “Run tests” or “Re-run demo”.";
const $=s=>document.querySelector(s),esc=v=>String(v??"").replace(/[&<>"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));
function nav(){$("#nav").innerHTML=TABS.map(([k,l])=>`<a data-k="${k}">${l}</a>`).join("");
 document.querySelectorAll("#nav a").forEach(a=>a.onclick=()=>show(a.dataset.k))}
function show(k){document.querySelectorAll("#nav a").forEach(a=>a.classList.toggle("on",a.dataset.k===k));
 document.querySelectorAll("section").forEach(s=>s.classList.toggle("on",s.id===k));history.replaceState(null,"","#"+k)}
function fmt(v){if(v===null||v===undefined)return '<span style="color:#aab">—</span>';if(typeof v==="object")return esc(JSON.stringify(v));return esc(v)}
function tbl(rows,cols,key,prev){if(!rows||!rows.length)return '<div class="legend">(no rows)</div>';
 cols=cols||Object.keys(rows[0]);const pm={};(prev||[]).forEach((r,i)=>pm[key?r[key]:i]=r);
 return `<table><tr>${cols.map(c=>`<th>${esc(c)}</th>`).join("")}</tr>${rows.map((r,i)=>{const k=key?r[key]:i,p=prev?pm[k]:undefined,isNew=prev&&!p;
 return `<tr class="${isNew?"new":""}">${cols.map(c=>{const ch=p&&JSON.stringify(p[c])!==JSON.stringify(r[c]);return `<td class="m ${ch?"chg":""}" title="${esc(typeof r[c]==="object"?JSON.stringify(r[c]):r[c])}">${fmt(r[c])}</td>`}).join("")}</tr>`}).join("")}</table>`}
function overview(){const r=D.results,p=r.filter(x=>x.passed).length;
 return `<div class="row"><div class="card"><div class="k">Checks (computed from final state)</div><div class="big">${p}/${r.length}</div><span class="chip ${p===r.length?"pass":"fail"}">${p===r.length?"ALL PASS":"FAILURES"}</span></div>
 <div class="card"><div class="k">Confirmation SMS delivered for #1042</div><div class="big">${D.snapshots["5_after_human_review"].ghl.messages.filter(m=>m.body.includes("is confirmed")).length}</div><span class="legend">despite timeout + crash + 2 racing workers</span></div>
 <div class="card"><div class="k">Final ServiceM8 state</div><div style="margin-top:6px">${D.snapshots["5_after_human_review"].servicem8_jobs.map(j=>`#${j.job_number} <span class="chip ${j.status==="Cancelled"?"warn":"pass"}">${j.status}</span>`).join("<br>")}</div></div></div>
 <div class="card"><h2>What is working, simulated, proposed</h2><table class="lab">
 <tr><td><span class="chip pass">Working</span></td><td>Worker logic: event reconciliation &amp; version guard, outbox with idempotency keys, atomic leases + fencing tokens, resend rule, mailbox delta recovery, follow-up gates, AI validation, human review. Real SQLite, real concurrent threads in tests.</td></tr>
 <tr><td><span class="chip warn">Simulated</span></td><td>ServiceM8, GHL, Microsoft Graph mailbox, AI model, and time (deterministic clock).</td></tr>
 <tr><td><span class="chip fail">Proposed</span></td><td>Production deployment on Postgres against live APIs. Integration writes to ServiceM8 are shown only against the simulator.</td></tr></table></div>
 <div class="card"><h2>Results</h2><table><tr><th>#</th><th>Check</th><th>Expected</th><th>Observed</th><th>Result</th><th>Status</th></tr>
 ${r.map((x,i)=>`<tr><td>${i+1}</td><td><b>${esc(x.check)}</b></td><td>${esc(x.expected)}</td><td class="m">${esc(x.observed)}</td><td><span class="chip ${x.passed?"pass":"fail"}">${x.passed?"PASS":"FAIL"}</span></td><td>${esc(x.status)}</td></tr>`).join("")}</table></div>`}
function cls(s){const t=JSON.stringify(s.result);if(/CRASH|DROPPED|OUTAGE starts|REJECTED/.test(s.step+t))return"bad";if(/held|hold/i.test(t))return"hold";if(/applied|reconciled|sent|recovered|approves/.test(t+s.step))return"good";return""}
function timeline(){return `<div class="card"><h2>Main scenario timeline (sim clock, UTC)</h2><div class="legend">red = failure injected · amber = deliberately held · green = applied / recovered</div><div class="tl">${D.steps.map(s=>`<div class="e ${cls(s)}"><div class="t">${esc(s.at)}</div><div class="s">${esc(s.step)}</div><pre>${esc(typeof s.result==="string"?s.result:JSON.stringify(s.result,null,1))}</pre></div>`).join("")}</div></div>`}
function snapshots(){const ks=Object.keys(STAGES),i=ks.indexOf(stage),s=D.snapshots[stage],pv=i>0?D.snapshots[ks[i-1]]:null;
 const T=(t,rows,key,prevRows,cols)=>`<h3>${t}</h3>${tbl(rows,cols,key,pv?prevRows:null)}`;
 return `<div class="card"><div class="stages">${ks.map(k=>`<button class="${k===stage?"on":""}" data-s="${k}">${STAGES[k]}</button>`).join("")}</div>
 <div class="legend">Sim time <b class="m">${esc(s.at)}</b>. Compared with previous stage: <span style="background:var(--chg)">changed cell</span><span style="background:var(--new)">new row</span></div></div>
 <div class="row"><div class="card"><h2>ServiceM8 (destination)</h2>${T("Jobs",s.servicem8_jobs,"job_number",pv&&pv.servicem8_jobs,["job_number","status","start_at","edit_date"])}</div>
 <div class="card"><h2>GHL (destination)</h2>${T("Contacts",s.ghl.contacts,"id",pv&&pv.ghl.contacts,["id","email","phone"])}${T("Opportunities",s.ghl.opportunities,"id",pv&&pv.ghl.opportunities,["id","name","stage"])}</div></div>
 <div class="card"><h2>GHL messages actually delivered</h2>${tbl(s.ghl.messages,["id","created_at","body"],"id",pv&&pv.ghl.messages)}</div>
 <div class="card"><h2>Integration store</h2>
 ${T("jobs",s.store_jobs,"job_number",pv&&pv.store_jobs)}${T("inbound_events",s.inbound_events,"event_id",pv&&pv.inbound_events)}
 ${T("outbox",s.outbox,"id",pv&&pv.outbox)}${T("followups",s.followups,"id",pv&&pv.followups)}
 ${T("inbound_emails (shared mailbox)",s.inbound_emails,"graph_id",pv&&pv.inbound_emails)}${T("mailbox_cursor",s.mailbox_cursor,"mailbox",pv&&pv.mailbox_cursor)}
 ${T("ai_extractions",s.ai_extractions,"graph_id",pv&&pv.ai_extractions)}${T("review_tasks",s.review_tasks,"id",pv&&pv.review_tasks)}
 ${T("Graph notifications dropped during outage",s.outlook_dropped_notifications,"graph_id",pv&&pv.outlook_dropped_notifications)}</div>`}
function trace(){const t=D.trace,ob=t.outbox_row,dm=t.destination.ghl_message;
 const n=(k,h)=>`<div class="n"><div class="k">${k}</div>${h}</div><div class="arrow">↓</div>`;
 return `<div class="card"><h2>evt-B-201 (reschedule of job #1042): source → store → destination</h2><div class="chain">
 ${n("1 · Source event",`<span class="m">${esc(t.source_event.event_id)} · job ${esc(t.source_event.sm8_uuid)} · edit_date ${esc(t.source_event.event_edit_date)}</span>`)}
 ${n("2 · Dedupe key → outcome",`<span class="m">${esc(t.source_event.dedupe_key)}</span> → <span class="chip pass">${esc(t.source_event.outcome)}</span><div class="m legend">${esc(t.source_event.detail)}</div>`)}
 ${n("3 · Redeliveries of the same event",tbl(t.duplicates_rejected,["event_id","received_at","outcome","detail"]))}
 ${n("4 · Stored job now",tbl([t.stored_job_now],["job_number","status","start_at","sm8_edit_date","content_hash"]))}
 ${n("5 · Outbox row",tbl([ob],["id","idempotency_key","status","sends","fencing_token","first_send_at","resend_eligible_at","provider_message_id","resolution"]))}
 ${n("6 · Every attempt (worker, fencing token, step)",tbl(t.attempts,["at","worker","fencing_token","step","result"]))}
 <div class="n"><div class="k">7 · Destination: GHL</div>${dm?`<span class="m">${esc(dm.id)} at ${esc(dm.created_at)}</span><div>${esc(dm.body)}</div>`:"none"}<div style="margin-top:6px">Messages with this body in GHL: <span class="chip ${t.destination.ghl_messages_for_body===1?"pass":"fail"}">${t.destination.ghl_messages_for_body}</span></div></div></div></div>`}
function variants(){const V=D.variants.variants;
 return Object.entries(V).map(([name,r])=>`<div class="card"><h2>${esc(name)}</h2>
 ${tbl(r.steps.map(([a,b])=>({step:a,result:typeof b==="string"?b:JSON.stringify(b)})),["step","result"])}
 <h3>Outcome</h3><div>Outbox <span class="chip ${r.outbox.status==="sent"?"pass":"warn"}">${esc(r.outbox.status)}</span> · sends=${r.outbox.sends} · GHL messages delivered: <span class="chip ${r.ghl_messages===1?"pass":"fail"}">${r.ghl_messages}</span> · ${esc(r.outbox.resolution)}</div>
 ${r.reviews.length?`<h3>Review task opened (evidence for the human)</h3><pre>${esc(r.reviews.map(x=>x.kind+"\n"+x.evidence).join("\n\n"))}</pre>`:""}
 <h3>Attempt log</h3>${tbl(r.attempts,["at","worker","fencing_token","step","result"])}</div>`).join("")+
 `<div class="card"><h2>V4 · Same reply with no thread reference</h2><div>Suggestion: <b>${esc(typeof D.variants.ambiguous.task_evidence.suggestion==="string"?D.variants.ambiguous.task_evidence.suggestion:JSON.stringify(D.variants.ambiguous.task_evidence.suggestion))}</b></div>
 <div>Follow-ups paused: ${D.variants.ambiguous.paused.length} · ServiceM8 writes: ${D.variants.ambiguous.sm8_writes.length}</div></div>
 <div class="card"><h2>V5 · 8 workers claim the same row simultaneously, 20 rounds</h2><div>Winners per round: <span class="m">${D.variants.stress.map(s=>s.winners.length).join(" ")}</span></div></div>`}
function con(){return `<div class="card"><h2>Console</h2><pre id="out">${esc(consoleOut)}</pre></div>`}
function render(){$("#main").innerHTML=`<section id="overview">${overview()}</section><section id="timeline">${timeline()}</section><section id="snapshots">${snapshots()}</section><section id="trace">${trace()}</section><section id="variants">${variants()}</section><section id="console">${con()}</section>`;
 document.querySelectorAll(".stages button").forEach(b=>b.onclick=()=>{stage=b.dataset.s;render();show("snapshots")});
 show(((location.hash||"#overview").slice(1)).split("/")[0])}
async function load(){D=await (await fetch("/api/evidence")).json();const h=location.hash.slice(1).split("/");if(h[0]==="snapshots"&&STAGES[h[1]])stage=h[1];render();if(h[1])show(h[0])}
async function act(path,btn){const bs=document.querySelectorAll("header button");bs.forEach(b=>b.disabled=true);consoleOut="Running "+path+" …";render();show("console");
 try{const r=await (await fetch(path,{method:"POST"})).json();consoleOut=`$ python ${r.cmd}\nexit code ${r.exit}\n\n${r.output}`;if(path==="/api/run-demo")D=await (await fetch("/api/evidence")).json()}
 catch(e){consoleOut="Error: "+e}bs.forEach(b=>b.disabled=false);render();show("console");const o=$("#out");o.scrollTop=o.scrollHeight}
$("#btnDemo").onclick=()=>act("/api/run-demo");$("#btnTests").onclick=()=>act("/api/run-tests");nav();load();
</script></body></html>"""


if __name__ == "__main__":
    print(f"BeSafe evidence viewer: http://127.0.0.1:{PORT}")
    ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
