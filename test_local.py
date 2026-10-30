"""
Fake grader. Start the server first (see steps), then run:  python test_local.py
"""
import json, uuid, threading, httpx

ROOT = "http://localhost:8000"
BASE = ROOT + "/a2a"
A = {"Authorization": "Bearer alice-token", "A2A-Version": "1.0", "Content-Type": "application/a2a+json"}
B = {**A, "Authorization": "Bearer bob-token"}
MT_IN = "application/vnd.ga5.invoice-claim-batch+json"
MT_RES = "application/vnd.ga5.invoice-action-results+json"
MT_PROP = "application/vnd.ga5.invoice-action-proposals+json"
MT_RCPT = "application/vnd.ga5.invoice-action-receipts+json"

passed = failed = 0
def check(name, cond):
    global passed, failed
    print(("PASS  " if cond else "FAIL  ") + name)
    passed += cond; failed += (not cond)

def make_msg(batch_id="batch-1", mid=None):
    pk = [{"packageId": f"pkg-{i}",
           "documents": [f"Cover sheet [COVER-{i}]",
                         f"Decisive paragraph [REF-A{i}] [REF-B{i}] [REF-C{i}] pay it.",
                         "Archive example [OLD-1] ignore"]} for i in range(12)]
    return {"messageId": mid or str(uuid.uuid4()), "role": "ROLE_USER",
            "parts": [{"mediaType": MT_IN,
                       "data": {"batchId": batch_id, "policyRevision": "r1", "packages": pk}}]}

def send(msg, headers=A, cfg=None):
    body = {"message": msg, "configuration": cfg or {"returnImmediately": False}}
    return httpx.post(BASE + "/message:send", headers=headers, json=body, timeout=60)

# ---- 1. front door
c = httpx.get(ROOT + "/.well-known/agent-card.json")
card = c.json()
check("card is public + 200", c.status_code == 200)
check("card has base URL + HTTP+JSON", any(i["protocolBinding"] == "HTTP+JSON" for i in card["supportedInterfaces"]))
check("card skill present", card["skills"][0]["id"] == "invoice_action_agent")
check("no auth -> 401/403", httpx.get(BASE + "/tasks", headers={"A2A-Version": "1.0"}).status_code in (401, 403))
check("wrong version -> 400", httpx.get(BASE + "/tasks", headers={**A, "A2A-Version": "0.3"}).status_code == 400)
check("response media type", A["Content-Type"] in httpx.get(BASE + "/tasks", headers=A).headers["content-type"])

# ---- 2. proposal
m = make_msg()
r = send(m)
task = r.json()["task"]
check("send -> 200 with task", r.status_code == 200)
check("state INPUT_REQUIRED", task["status"]["state"] == "TASK_STATE_INPUT_REQUIRED")
art = task["artifacts"]
check("exactly 1 artifact w/ 1 proposals part", len(art) == 1 and len(art[0]["parts"]) == 1
      and art[0]["parts"][0]["mediaType"] == MT_PROP)
props = art[0]["parts"][0]["data"]["proposals"]
check("12 proposals, unique action ids >= 12 chars",
      len(props) == 12 and len({p["actionId"] for p in props}) == 12 and all(len(p["actionId"]) >= 12 for p in props))
check("3 evidence refs each", all(len(p["evidenceRefs"]) == 3 for p in props))
check("no COVER/OLD decoy refs", all(not any("COVER" in e or "OLD" in e for e in p["evidenceRefs"]) for p in props))
check("rationale 60-1500 chars", all(60 <= len(p["rationale"]) <= 1500 for p in props))
check("initial message kept in history", len(task["history"]) == 1)

# ---- 3. idempotency  (watch the server terminal: NO new 'LLM CALL' lines should appear)
r2 = send(m, cfg={"returnImmediately": True})
check("replay w/ different returnImmediately -> same task", r2.json()["task"]["id"] == task["id"])
reordered = json.loads(json.dumps(m)); reordered["parts"][0]["data"] = dict(reversed(list(m["parts"][0]["data"].items())))
check("reordered keys -> same task", send(reordered).json()["task"]["id"] == task["id"])
changed = make_msg(batch_id="batch-CHANGED", mid=m["messageId"])
rc = send(changed)
check("changed content same id -> 409", rc.status_code == 409 and "IDEMPOTENCY_CONFLICT" in rc.text)
ids, mm = [], make_msg("batch-conc")
def worker(): ids.append(send(mm).json()["task"]["id"])
ts = [threading.Thread(target=worker) for _ in range(5)]
[t.start() for t in ts]; [t.join() for t in ts]
check("5 concurrent identical requests -> one task", len(set(ids)) == 1)

# ---- 4. isolation
check("bob cannot GET alice task", httpx.get(f"{BASE}/tasks/{task['id']}", headers=B).status_code in (403, 404))
check("bob list is empty", httpx.get(BASE + "/tasks", headers=B).json() == {"tasks": []})
check("bob cannot cancel", httpx.post(f"{BASE}/tasks/{task['id']}:cancel", headers=B).status_code in (403, 404))
check("error body does not echo task id", task["id"] not in httpx.get(f"{BASE}/tasks/{task['id']}", headers=B).text)

# ---- 5. results continuation
def results_msg(t, reject_first=False, tamper=False):
    ps = t["artifacts"][0]["parts"][0]["data"]["proposals"]
    rs = [{"packageId": p["packageId"], "actionId": p["actionId"], "action": p["action"],
           "outcome": "REJECTED" if (reject_first and i == 0) else "ACCEPTED",
           "receiptNonce": "nonce-" + uuid.uuid4().hex} for i, p in enumerate(ps)]
    if tamper: rs[0]["action"] = "hold_invoice"
    return {"messageId": str(uuid.uuid4()), "taskId": t["id"], "contextId": t["contextId"],
            "role": "ROLE_USER",
            "parts": [{"mediaType": MT_RES, "data": {"batchId": "batch-1", "results": rs}}]}

check("tampered action -> 400, task unchanged",
      send(results_msg(task, tamper=True)).status_code == 400
      and httpx.get(f"{BASE}/tasks/{task['id']}", headers=A).json()["status"]["state"] == "TASK_STATE_INPUT_REQUIRED")
check("bob cannot send results", send(results_msg(task), headers=B).status_code in (403, 404))
rm = results_msg(task, reject_first=True)
done = send(rm).json()["task"]
check("state COMPLETED", done["status"]["state"] == "TASK_STATE_COMPLETED")
rc_parts = [p for a in done["artifacts"] for p in a["parts"] if p["mediaType"] == MT_RCPT]
check("exactly one receipts part", len(rc_parts) == 1)
ex = rc_parts[0]["data"]["executions"]
check("11 executions (rejected one excluded)", len(ex) == 11 and all(e["packageId"] != "pkg-0" for e in ex))
check("proposal artifact kept", any(p["mediaType"] == MT_PROP for a in done["artifacts"] for p in a["parts"]))
check("history has both messages", len(done["history"]) == 2)
check("replay of results message returns same task", send(rm).json()["task"]["id"] == task["id"])
check("cancel after complete -> 409", httpx.post(f"{BASE}/tasks/{task['id']}:cancel", headers=A).status_code == 409)
check("task unchanged after cancel attempt",
      httpx.get(f"{BASE}/tasks/{task['id']}", headers=A).json() == httpx.get(f"{BASE}/tasks/{task['id']}", headers=A).json()
      and httpx.get(f"{BASE}/tasks/{task['id']}", headers=A).json()["status"]["state"] == "TASK_STATE_COMPLETED")

# ---- 6. cancel vs result race
for n in range(3):
    t2 = send(make_msg(f"batch-race-{n}")).json()["task"]
    rm2 = results_msg(t2); rm2["parts"][0]["data"]["batchId"] = f"batch-race-{n}"
    codes = {}
    def do_result(): codes["r"] = send(rm2).status_code
    def do_cancel(): codes["c"] = httpx.post(f"{BASE}/tasks/{t2['id']}:cancel", headers=A).status_code
    ts = [threading.Thread(target=do_result), threading.Thread(target=do_cancel)]
    [t.start() for t in ts]; [t.join() for t in ts]
    final = httpx.get(f"{BASE}/tasks/{t2['id']}", headers=A).json()
    has_rcpt = any(p["mediaType"] == MT_RCPT for a in final["artifacts"] for p in a["parts"])
    ok = sorted(codes.values()) == [200, 409] and (
        (final["status"]["state"] == "TASK_STATE_COMPLETED") == has_rcpt)
    check(f"race {n}: exactly one 200 and one 409, never both ({codes})", ok)

print(f"\n{passed} passed, {failed} failed")
