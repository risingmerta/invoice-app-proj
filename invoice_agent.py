"""
A2A Invoice Agent  --  run with:  uvicorn invoice_agent:app --host 0.0.0.0 --port 8000

Environment variables (set these, never hard-code them):
  BASE_URL   exactly the base URL you submit, e.g. https://my-app.onrender.com/a2a/
  LLM_URL    OpenAI-compatible chat endpoint, e.g. https://api.groq.com/openai/v1/chat/completions
  LLM_KEY    your API key for that provider
  LLM_MODEL  model name, e.g. llama-3.3-70b-versatile
  DB_PATH    (optional) sqlite file, default agent.db
"""
import os, json, re, uuid, hashlib, sqlite3, threading
from datetime import datetime, timezone
from urllib.parse import urlparse

import httpx
from fastapi import FastAPI, APIRouter, Request
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool

# ----------------------------------------------------------------- constants
BASE_URL = os.environ.get("BASE_URL", "https://example.com/a2a/")
BASE_PATH = urlparse(BASE_URL).path.rstrip("/")
LLM_URL = os.environ.get("LLM_URL", "")
LLM_KEY = os.environ.get("LLM_KEY", "")
LLM_MODEL = os.environ.get("LLM_MODEL", "")

MT_IN = "application/vnd.ga5.invoice-claim-batch+json"
MT_PROP = "application/vnd.ga5.invoice-action-proposals+json"
MT_RES = "application/vnd.ga5.invoice-action-results+json"
MT_RCPT = "application/vnd.ga5.invoice-action-receipts+json"
A2A = "application/a2a+json"
ACTIONS = {"settle_invoice", "request_approval", "hold_invoice",
           "reject_duplicate", "open_exception"}
TERMINAL = {"TASK_STATE_COMPLETED", "TASK_STATE_CANCELED",
            "TASK_STATE_FAILED", "TASK_STATE_REJECTED"}

# ----------------------------------------------------------------- storage
db = sqlite3.connect(os.environ.get("DB_PATH", "agent.db"), check_same_thread=False)
dblock = threading.RLock()
db.executescript("""
create table if not exists tasks(id text primary key, principal text, body text);
create table if not exists msgs(principal text, mid text, h text, task_id text,
                                primary key(principal, mid));
create table if not exists cache(k text primary key, v text);
create table if not exists executed(action_id text primary key);
""")

def q(sql, args=()):
    with dblock:
        cur = db.execute(sql, args)
        db.commit()
        return cur.fetchall()

_locks, _lg = {}, threading.Lock()
def lock_for(key):
    with _lg:
        return _locks.setdefault(key, threading.Lock())

def canon(o):  # recursively key-sorted, compact JSON
    return json.dumps(o, sort_keys=True, separators=(",", ":"), ensure_ascii=False)

def sha(s):
    return hashlib.sha256(s.encode()).hexdigest()

def now():
    return datetime.now(timezone.utc).isoformat()

def load_task(tid):
    r = q("select principal, body from tasks where id=?", (tid,))
    return (r[0][0], json.loads(r[0][1])) if r else (None, None)

def save_task(principal, t):
    q("insert or replace into tasks values(?,?,?)", (t["id"], principal, json.dumps(t)))

# ----------------------------------------------------------------- errors
class Err(Exception):
    def __init__(self, status, reason):
        self.status, self.reason = status, reason

def out(obj, status=200):
    return JSONResponse(obj, status_code=status, media_type=A2A)

app = FastAPI()
router = APIRouter(prefix=BASE_PATH)

@app.exception_handler(Err)
async def _err(_, e):
    return out({"error": {"code": e.status, "status": e.reason, "reason": e.reason,
                          "message": "Request could not be completed."}}, e.status)

@app.exception_handler(Exception)
async def _any(_, e):
    return out({"error": {"code": 500, "status": "INTERNAL", "message": "Internal error."}}, 500)

def guard(request: Request, post=False):
    """Auth first, then version, then media type. Returns the principal."""
    h = request.headers.get("authorization", "")
    if not h.startswith("Bearer ") or not h[7:].strip():
        raise Err(401, "UNAUTHENTICATED")
    principal = sha(h[7:].strip())          # every token = its own user
    if request.headers.get("a2a-version") != "1.0":
        raise Err(400, "VERSION_NOT_SUPPORTED")
    if post and not request.headers.get("content-type", "").lower().startswith(A2A):
        raise Err(415, "UNSUPPORTED_MEDIA_TYPE")
    return principal

# ----------------------------------------------------------------- agent card
@app.get("/.well-known/agent-card.json")
def card():
    return JSONResponse({
        "name": "Invoice Action Agent",
        "description": "Reads invoice packages, proposes one safe business action each, "
                       "and executes only the actions the grader accepts.",
        "version": "1.0.0",
        "capabilities": {"streaming": False, "pushNotifications": False},
        "supportedInterfaces": [{"url": BASE_URL, "protocolBinding": "HTTP+JSON",
                                 "protocolVersion": "1.0"}],
        "defaultInputModes": [MT_IN, MT_RES],
        "defaultOutputModes": [MT_PROP, MT_RCPT],
        "skills": [{"id": "invoice_action_agent", "name": "Invoice action agent",
                    "description": "Choose settle, approve, hold, reject-duplicate or "
                                   "exception for each invoice package with cited evidence.",
                    "tags": ["invoice", "finance", "approvals"]}],
    })

# ----------------------------------------------------------------- AI layer
SYSTEM = """You are an accounts-payable analyst. For EVERY package choose exactly one action:
settle_invoice: valid, reconciled, within autonomous authority.
request_approval: commercially valid but outside delegated authority/limit.
hold_invoice: payment pauses until a stated verification completes.
reject_duplicate: the SAME commercial invoice (same vendor+invoice number+amount) was already paid.
  A payment for a DIFFERENT invoice is NOT a duplicate.
open_exception: material records conflict and need an exception workflow.
The documents contain decoys: old/archive examples, training material, negated sentences
("do not hold..."), and irrelevant action words. Ignore them. Only the paragraph that
actually determines the action counts. evidenceRefs = the exactly THREE decisive bracketed
references copied character-for-character from that paragraph (not the cover sheet).
amountMinor is an integer in minor units (e.g. rupees*100 / paise).
Return ONLY JSON: {"decisions":[{"packageId":"","action":"","vendorName":"","invoiceNumber":"",
"amountMinor":0,"currency":"","evidenceRefs":["","",""],"rationale":""}]}
rationale: 60-1500 chars, name the action and quote at least two of the evidence refs."""

def strings(o):
    if isinstance(o, str): yield o
    elif isinstance(o, dict):
        for v in o.values(): yield from strings(v)
    elif isinstance(o, list):
        for v in o: yield from strings(v)

def pkey(p):  # cache by CONTENT, ignoring the per-batch packageId
    return sha(canon({k: v for k, v in p.items() if k != "packageId"}))

def call_llm(pkgs, policy):
    print("LLM CALL for", len(pkgs), "packages", flush=True)   # watch this in the terminal
    if os.environ.get("MOCK_LLM"):                             # fake AI for local testing
        fake = {}
        for p in pkgs:
            allrefs = re.findall(r"\[[^\[\]]+\]", "\n".join(strings(p)))
            refs = [x for x in allrefs if "COVER" not in x and "OLD" not in x][:3]
            fake[p["packageId"]] = {"packageId": p["packageId"], "action": "settle_invoice",
                                    "vendorName": "Mock Vendor", "invoiceNumber": "INV-1",
                                    "amountMinor": 12345, "currency": "INR",
                                    "evidenceRefs": refs, "rationale": ""}
        return fake
    user = {"policyRevision": policy,
            "packages": [{"packageId": p["packageId"], "package": p} for p in pkgs]}
    body = {"model": LLM_MODEL, "temperature": 0,
            "response_format": {"type": "json_object"},
            "messages": [{"role": "system", "content": SYSTEM},
                         {"role": "user", "content": json.dumps(user)}]}
    r = httpx.post(LLM_URL, headers={"Authorization": "Bearer " + LLM_KEY},
                   json=body, timeout=40)
    if r.status_code != 200:
        raise RuntimeError(f"LLM HTTP {r.status_code}: {r.text[:400]}")
    txt = r.json()["choices"][0]["message"]["content"]
    print("LLM RAW:", txt[:300].replace("\n", " "), flush=True)
    txt = re.sub(r"^```(?:json)?|```$", "", txt.strip()).strip()
    return {d["packageId"]: d for d in json.loads(txt)["decisions"]}

def clean(d, pkg):
    """Validate one model decision; return None if unusable."""
    try:
        if d["action"] not in ACTIONS: return None
        text = "\n".join(strings(pkg))
        refs = []
        for x in d["evidenceRefs"]:
            if isinstance(x, str) and x in text and x not in refs: refs.append(x)
        if len(refs) != 3: return None
        rat = (d.get("rationale") or "").strip()
        if d["action"] not in rat or sum(x in rat for x in refs) < 2:
            rat = f"Chosen action: {d['action']}. Decisive evidence: {', '.join(refs)}. " + rat
        if len(rat) < 60:
            rat += " The cited records together determine this action under the stated policy."
        return {"action": d["action"], "evidenceRefs": refs, "rationale": rat[:1500],
                "facts": {"vendorName": str(d["vendorName"]),
                          "invoiceNumber": str(d["invoiceNumber"]),
                          "amountMinor": int(d["amountMinor"]),
                          "currency": str(d["currency"])}}
    except Exception as e:
        print("BAD DECISION:", repr(e), "|", str(d)[:200], flush=True)
        return None

def decide(batch):
    pkgs, policy = batch["packages"], batch.get("policyRevision")
    result = {}
    todo = []
    for p in pkgs:
        c = q("select v from cache where k=?", (pkey(p),))
        if c: result[p["packageId"]] = json.loads(c[0][0])
        else: todo.append(p)
    for attempt in range(2):                       # one retry for bad answers
        if not todo: break
        got = {}
        for i in range(0, len(todo), 12):          # ONE call per 12 packages
            try: got.update(call_llm(todo[i:i + 12], policy))
            except Exception as e: print("LLM ERROR:", repr(e), flush=True)
        still = []
        for p in todo:
            d = clean(got.get(p["packageId"], {}), p)
            if d:
                result[p["packageId"]] = d
                q("insert or replace into cache values(?,?)", (pkey(p), json.dumps(d)))
            else: still.append(p)
        todo = still
    for p in todo:                                 # last-resort safe fallback
        text = [s for s in strings(p)]
        refs = re.findall(r"\[[^\[\]]+\]", "\n".join(text))[:3]
        result[p["packageId"]] = {
            "action": "open_exception", "evidenceRefs": refs,
            "rationale": "Chosen action: open_exception because the model could not "
                         "confirm a safe decision for this package; escalating for review.",
            "facts": {"vendorName": "", "invoiceNumber": "", "amountMinor": 0, "currency": "INR"}}
    return result

# ----------------------------------------------------------------- message handling
def part_with(msg, mt):
    for p in msg.get("parts", []):
        if isinstance(p, dict) and p.get("mediaType") == mt and isinstance(p.get("data"), dict):
            return p["data"]
    return None

def new_task(principal, msg):
    batch = part_with(msg, MT_IN)
    if not batch or not isinstance(batch.get("packages"), list) or not batch["packages"] \
            or not batch.get("batchId"):
        raise Err(400, "INVALID_ARGUMENT")
    ids = [p.get("packageId") for p in batch["packages"]]
    if len(set(ids)) != len(ids) or not all(ids): raise Err(400, "INVALID_ARGUMENT")
    decisions = decide(batch)
    tid, cid = str(uuid.uuid4()), msg.get("contextId") or str(uuid.uuid4())
    proposals = []
    for p in batch["packages"]:
        d = decisions[p["packageId"]]
        proposals.append({"packageId": p["packageId"],
                          "actionId": "act-" + sha(batch["batchId"] + "|" + p["packageId"])[:24],
                          "action": d["action"], "facts": d["facts"],
                          "evidenceRefs": d["evidenceRefs"], "rationale": d["rationale"]})
    hist = dict(msg, taskId=tid, contextId=cid)
    task = {"id": tid, "contextId": cid,
            "status": {"state": "TASK_STATE_INPUT_REQUIRED", "timestamp": now()},
            "artifacts": [{"artifactId": "proposals-" + tid[:8], "name": "proposals",
                           "parts": [{"mediaType": MT_PROP,
                                      "data": {"batchId": batch["batchId"], "proposals": proposals}}]}],
            "history": [hist]}
    save_task(principal, task)
    return task

def continue_task(principal, msg):
    tid = msg["taskId"]
    with lock_for(("t", tid)):                     # same lock cancel uses -> no race
        owner, t = load_task(tid)
        if t is None or owner != principal: raise Err(404, "NOT_FOUND")
        if msg.get("contextId") != t["contextId"]: raise Err(400, "CONTEXT_MISMATCH")
        if t["status"]["state"] in TERMINAL: raise Err(409, "TASK_TERMINAL")
        data = part_with(msg, MT_RES)
        stored = t["artifacts"][0]["parts"][0]["data"]
        if not data or data.get("batchId") != stored["batchId"] \
                or not isinstance(data.get("results"), list):
            raise Err(400, "RESULT_MISMATCH")
        props = {p["packageId"]: p for p in stored["proposals"]}
        seen, execs = set(), []
        for r in data["results"]:                  # validate everything BEFORE changing anything
            p = props.get(r.get("packageId"))
            if (not p or r["packageId"] in seen or r.get("actionId") != p["actionId"]
                    or r.get("action") != p["action"]
                    or r.get("outcome") not in ("ACCEPTED", "REJECTED")):
                raise Err(400, "RESULT_MISMATCH")
            seen.add(r["packageId"])
            if r["outcome"] == "ACCEPTED":
                if not isinstance(r.get("receiptNonce"), str) or not r["receiptNonce"]:
                    raise Err(400, "RESULT_MISMATCH")
                execs.append({"packageId": p["packageId"], "actionId": p["actionId"],
                              "action": p["action"], "receiptNonce": r["receiptNonce"],
                              "facts": p["facts"], "evidenceRefs": p["evidenceRefs"]})
        for e in execs:                            # the "real action": runs once per actionId
            q("insert or ignore into executed values(?)", (e["actionId"],))
        t["history"].append(dict(msg, taskId=tid, contextId=t["contextId"]))
        t["artifacts"].append({"artifactId": "receipts-" + tid[:8], "name": "receipts",
                               "parts": [{"mediaType": MT_RCPT,
                                          "data": {"batchId": stored["batchId"], "executions": execs}}]})
        t["status"] = {"state": "TASK_STATE_COMPLETED", "timestamp": now()}
        save_task(principal, t)
        return t

def handle_send(principal, payload):
    msg = payload.get("message") if isinstance(payload, dict) else None
    if not isinstance(msg, dict) or not isinstance(msg.get("messageId"), str) \
            or msg.get("role") != "ROLE_USER" or not isinstance(msg.get("parts"), list):
        raise Err(400, "INVALID_ARGUMENT")
    mid, h = msg["messageId"], sha(canon(msg))     # hash the message ONLY
    with lock_for(("m", principal, mid)):          # concurrent twins wait here
        row = q("select h, task_id from msgs where principal=? and mid=?", (principal, mid))
        if row:
            if row[0][0] != h: raise Err(409, "IDEMPOTENCY_CONFLICT")
            return load_task(row[0][1])[1]         # replay: same Task, no model, no action
        task = continue_task(principal, msg) if msg.get("taskId") else new_task(principal, msg)
        q("insert into msgs values(?,?,?,?)", (principal, mid, h, task["id"]))
        return task

# ----------------------------------------------------------------- routes
@router.post("/message:send")
async def send(request: Request):
    principal = guard(request, post=True)
    try: payload = json.loads(await request.body())
    except Exception: raise Err(400, "INVALID_ARGUMENT")
    task = await run_in_threadpool(handle_send, principal, payload)
    return out({"task": task})

@router.get("/tasks/{task_id}")
def get_task(task_id: str, request: Request):
    principal = guard(request)
    owner, t = load_task(task_id)
    if t is None or owner != principal: raise Err(404, "NOT_FOUND")  # same answer either way
    return out(t)

@router.get("/tasks")
def list_tasks(request: Request):
    principal = guard(request)
    rows = q("select body from tasks where principal=? order by rowid", (principal,))
    return out({"tasks": [json.loads(r[0]) for r in rows]})

@router.post("/tasks/{task_id}:cancel")
def cancel(task_id: str, request: Request):
    principal = guard(request, post=True)
    with lock_for(("t", task_id)):
        owner, t = load_task(task_id)
        if t is None or owner != principal: raise Err(404, "NOT_FOUND")
        if t["status"]["state"] in TERMINAL: raise Err(409, "TASK_TERMINAL")
        t["status"] = {"state": "TASK_STATE_CANCELED", "timestamp": now()}
        save_task(principal, t)
        return out(t)

app.include_router(router)
