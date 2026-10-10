#!/usr/bin/env python3
"""Chaos test: fire webhook events under injected failures, recover them,
and measure what happened.

Usage (run from the project root, venv active):
    python scripts/chaos_test.py --name baseline
    python scripts/chaos_test.py --name outage   --receiver-fail 0.2
    python scripts/chaos_test.py --name crashes  --worker-fail 0.3
    python scripts/chaos_test.py --name combined --receiver-fail 0.2 --worker-fail 0.3

Each run:
  1. Sets fault-injection rates on the Receiver / Worker.
  2. Creates a test issue and posts N comments (each one = 1 webhook).
  3. Turns failures OFF (always, even if the script crashes).
  4. Invokes the reconciler every --interval seconds until every event is
     PROCESSED or FAILED (or --max-wait is hit).
  5. Measures losses, recoveries, duplicates and recovery time, then writes
     results/<name>.json and appends a row to results/summary.md.

Note: GitHub limits content creation to roughly 500 requests/hour, so keep
--events around 150 and leave a gap between runs.
"""
import argparse
import json
import re
import statistics
import subprocess
import time
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path

import boto3

STACK = "ResilienthookStack"
OWNER = "ilakyadhinakaran"
REPO = "resilienthook-test"
HOOK_ID = "693022552"
GITHUB_API = "https://api.github.com"
NEXT_LINK = re.compile(r'<([^>]+)>;\s*rel="next"')
DONE_STATES = {"PROCESSED", "FAILED"}

cfn = boto3.client("cloudformation")
lam = boto3.client("lambda")
ddb = boto3.resource("dynamodb")
ssm = boto3.client("ssm")
logs = boto3.client("logs")
sqs = boto3.client("sqs")


# --- AWS helpers ------------------------------------------------------------
def stack_outputs() -> dict:
    stack = cfn.describe_stacks(StackName=STACK)["Stacks"][0]
    return {o["OutputKey"]: o["OutputValue"] for o in stack["Outputs"]}


def set_fail_rate(function_name: str, rate: float) -> None:
    env = lam.get_function_configuration(FunctionName=function_name)["Environment"]["Variables"]
    env["FAIL_RATE"] = str(rate)
    lam.update_function_configuration(FunctionName=function_name, Environment={"Variables": env})
    lam.get_waiter("function_updated_v2").wait(FunctionName=function_name)


def invoke_reconciler(function_name: str) -> dict:
    resp = lam.invoke(FunctionName=function_name, InvocationType="RequestResponse")
    return json.loads(resp["Payload"].read() or "{}")


def dlq_depth(queue_url: str) -> int:
    attrs = sqs.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["ApproximateNumberOfMessages"]
    )["Attributes"]
    return int(attrs["ApproximateNumberOfMessages"])


def get_rows(table_name: str, guids: list) -> dict:
    rows = {}
    keys = [{"event_id": g} for g in guids]
    for i in range(0, len(keys), 100):
        request = {
            table_name: {
                "Keys": keys[i : i + 100],
                "ProjectionExpression": "event_id, #s, processed_at, attempts, failure_reason",
                "ExpressionAttributeNames": {"#s": "status"},
            }
        }
        while request:
            resp = ddb.batch_get_item(RequestItems=request)
            for item in resp["Responses"].get(table_name, []):
                rows[item["event_id"]] = item
            request = resp.get("UnprocessedKeys") or None
            if request:
                time.sleep(0.5)
    return rows


def count_duplicates(worker_name: str, start: int, end: int) -> int:
    """Events whose business logic completed more than once (should be 0)."""
    log_group = lam.get_function_configuration(FunctionName=worker_name)["LoggingConfig"]["LogGroup"]
    query_id = logs.start_query(
        logGroupName=log_group,
        startTime=start,
        endTime=end,
        queryString='filter msg = "processed" | stats count(*) as n by event_id | filter n > 1',
    )["queryId"]
    while True:
        resp = logs.get_query_results(queryId=query_id)
        if resp["status"] in ("Complete", "Failed", "Cancelled", "Timeout"):
            return len(resp.get("results", []))
        time.sleep(1)


# --- GitHub helpers ---------------------------------------------------------
def github(token: str, method: str, url: str, body: dict | None = None, retries: int = 5):
    if not url.startswith("http"):
        url = GITHUB_API + url
    data = json.dumps(body).encode() if body is not None else None
    for attempt in range(retries):
        req = urllib.request.Request(
            url,
            data=data,
            method=method,
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "Content-Type": "application/json",
                "User-Agent": "resilienthook-chaos-test",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                raw = resp.read()
                return (json.loads(raw) if raw else None), resp.headers.get("Link", "")
        except urllib.error.HTTPError as e:
            text = e.read().decode(errors="replace").lower()
            rate_limited = e.code == 429 or (e.code == 403 and "rate limit" in text)
            if rate_limited and attempt < retries - 1:
                wait = int(e.headers.get("Retry-After") or 60)
                print(f"    GitHub rate limit hit, sleeping {wait}s")
                time.sleep(wait)
                continue
            raise


def to_epoch(iso: str) -> int:
    return int(datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp())


def fire_events(token: str, name: str, n: int, pace: float) -> tuple[int, int]:
    issue, _ = github(token, "POST", f"/repos/{OWNER}/{REPO}/issues",
                      {"title": f"chaos: {name} {datetime.now():%Y-%m-%d %H:%M}"})
    number = issue["number"]
    print(f"  created issue #{number}; posting {n} comments")
    for i in range(1, n + 1):
        github(token, "POST", f"/repos/{OWNER}/{REPO}/issues/{number}/comments",
               {"body": f"chaos event {i}/{n}"})
        if i % 25 == 0:
            print(f"    {i}/{n} sent")
        time.sleep(pace)
    return number, n + 1  # comments + the "issue opened" event


def fetch_deliveries(token: str, since: int) -> dict:
    """guid -> first delivery time/status + total attempts, for this run."""
    by_guid: dict = {}
    url = f"/repos/{OWNER}/{REPO}/hooks/{HOOK_ID}/deliveries?per_page=100"
    while url:
        page, link = github(token, "GET", url)
        reached_start = False
        for d in page or []:
            ts = to_epoch(d["delivered_at"])
            if ts < since:
                reached_start = True
                continue
            entry = by_guid.setdefault(d["guid"], {"first_ts": ts, "first_code": d["status_code"], "attempts": 0})
            entry["attempts"] += 1
            if ts < entry["first_ts"]:
                entry["first_ts"], entry["first_code"] = ts, d["status_code"]
        match = NEXT_LINK.search(link)
        url = match.group(1) if match and not reached_start else None
    return by_guid


# --- Main -------------------------------------------------------------------
def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--name", required=True)
    p.add_argument("--events", type=int, default=150)
    p.add_argument("--receiver-fail", type=float, default=0.0)
    p.add_argument("--worker-fail", type=float, default=0.0)
    p.add_argument("--pace", type=float, default=1.2, help="seconds between events")
    p.add_argument("--interval", type=int, default=60, help="seconds between reconciler runs")
    p.add_argument("--max-wait", type=int, default=1800, help="max recovery wait, seconds")
    args = p.parse_args()

    out = stack_outputs()
    hook_token = ssm.get_parameter(Name="/resilienthook/github-token", WithDecryption=True)["Parameter"]["Value"]
    user_token = subprocess.run(["gh", "auth", "token"], capture_output=True, text=True, check=True).stdout.strip()
    dlq_before = dlq_depth(out["DlqUrl"])
    t0 = time.time()

    print(f"\n=== {args.name}: {args.events} events, receiver_fail={args.receiver_fail}, worker_fail={args.worker_fail}")

    # 1-3. Inject failures, fire events, ALWAYS turn failures off.
    try:
        print("  setting fail rates")
        set_fail_rate(out["ReceiverName"], args.receiver_fail)
        set_fail_rate(out["WorkerName"], args.worker_fail)
        start = int(time.time()) - 2
        _, expected = fire_events(user_token, args.name, args.events, args.pace)
    finally:
        print("  resetting fail rates to 0")
        set_fail_rate(out["ReceiverName"], 0)
        set_fail_rate(out["WorkerName"], 0)

    # 4. Recover.
    print(f"  recovering (reconciler every {args.interval}s; stuck rows wait the real 10-min threshold)")
    deadline = time.time() + args.max_wait
    round_no = 0
    while True:
        time.sleep(args.interval)
        round_no += 1
        rec = invoke_reconciler(out["ReconcilerName"])
        deliveries = fetch_deliveries(hook_token, start)
        rows = get_rows(out["TableName"], list(deliveries))
        pending = [g for g in deliveries if rows.get(g, {}).get("status") not in DONE_STATES]
        print(f"    round {round_no}: {len(deliveries)}/{expected} events seen, {len(pending)} pending, "
              f"reconciler redelivered {rec.get('redelivered', '?')}")
        if not pending and len(deliveries) >= expected:
            break
        if time.time() > deadline:
            print("  max wait reached; reporting current state")
            break

    # 5. Measure.
    end = int(time.time())
    time.sleep(10)  # let the last log lines reach CloudWatch
    lost = {g for g, d in deliveries.items() if not 200 <= (d["first_code"] or 0) < 300}
    crashed = {g for g in deliveries if int(rows.get(g, {}).get("attempts", 0)) > 1}
    processed = {g for g in deliveries if rows.get(g, {}).get("status") == "PROCESSED"}
    failed = {g for g in deliveries if rows.get(g, {}).get("status") == "FAILED"}
    affected = lost | crashed
    recovered = affected & processed
    recovery_secs = sorted(int(rows[g]["processed_at"]) - deliveries[g]["first_ts"] for g in recovered)

    summary = {
        "name": args.name,
        "receiver_fail": args.receiver_fail,
        "worker_fail": args.worker_fail,
        "events": len(deliveries),
        "lost_initially": len(lost),
        "worker_crashed": len(crashed),
        "affected": len(affected),
        "recovered": len(recovered),
        "unrecoverable": len(failed),
        "still_pending": len(deliveries) - len(processed) - len(failed),
        "duplicates": count_duplicates(out["WorkerName"], start, end),
        "recovery_rate_pct": round(100 * len(recovered) / len(affected), 1) if affected else None,
        "recovery_median_s": int(statistics.median(recovery_secs)) if recovery_secs else None,
        "recovery_max_s": recovery_secs[-1] if recovery_secs else None,
        "dlq_new_messages": dlq_depth(out["DlqUrl"]) - dlq_before,
        "test_minutes": round((time.time() - t0) / 60, 1),
    }

    print("\n  RESULTS")
    for k, v in summary.items():
        print(f"    {k:<20} {v}")

    results = Path("results")
    results.mkdir(exist_ok=True)
    (results / f"{args.name}.json").write_text(json.dumps(summary, indent=2))
    table = results / "summary.md"
    if not table.exists():
        table.write_text(
            "| Scenario | Receiver fail | Worker fail | Events | Lost | Crashed | Recovered | "
            "Unrecoverable | Duplicates | Recovery rate | Median recovery | Max recovery |\n"
            "|---|---|---|---|---|---|---|---|---|---|---|---|\n"
        )
    rate = f"{summary['recovery_rate_pct']}%" if summary["recovery_rate_pct"] is not None else "n/a"
    med = f"{summary['recovery_median_s']}s" if summary["recovery_median_s"] is not None else "-"
    mx = f"{summary['recovery_max_s']}s" if summary["recovery_max_s"] is not None else "-"
    with table.open("a") as f:
        f.write(
            f"| {args.name} | {int(args.receiver_fail * 100)}% | {int(args.worker_fail * 100)}% | "
            f"{summary['events']} | {summary['lost_initially']} | {summary['worker_crashed']} | "
            f"{summary['recovered']} | {summary['unrecoverable']} | {summary['duplicates']} | "
            f"{rate} | {med} | {mx} |\n"
        )
    print(f"\n  saved results/{args.name}.json and appended to results/summary.md")


if __name__ == "__main__":
    main()
