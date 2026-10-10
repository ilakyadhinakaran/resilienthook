"""Reconciler: finds lost or stuck webhook events and recovers them.

Every run:
  1. List recent deliveries from GitHub's webhook deliveries API.
  2. For each delivery, look up its row in DynamoDB:
       - row missing and GitHub saw a non-2xx      -> "missing"
       - row RECEIVED/PROCESSING for too long      -> "stuck"
     and ask GitHub to redeliver it (it re-enters through the Receiver).
  3. After MAX_ATTEMPTS redeliveries, mark the event FAILED and alert.
  4. Rows stuck since before GitHub's retention window can't be
     redelivered at all -> mark FAILED and alert.
"""
import json
import os
import re
import time
import urllib.error
import urllib.request
from datetime import datetime

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

DDB = boto3.resource("dynamodb")
TABLE = DDB.Table(os.environ["TABLE_NAME"])
SNS = boto3.client("sns")
SSM = boto3.client("ssm")

STATUS_INDEX = os.environ["STATUS_INDEX"]
TOPIC_ARN = os.environ["TOPIC_ARN"]
OWNER = os.environ["GH_OWNER"]
REPO = os.environ["GH_REPO"]
HOOK_ID = os.environ["GH_HOOK_ID"]
LOOKBACK_SECONDS = int(os.environ["LOOKBACK_SECONDS"])
STUCK_SECONDS = int(os.environ["STUCK_SECONDS"])
MAX_ATTEMPTS = int(os.environ["MAX_ATTEMPTS"])
TTL_DAYS = 7

GITHUB_API = "https://api.github.com"
NEXT_LINK = re.compile(r'<([^>]+)>;\s*rel="next"')

_token = None


def _log(**fields) -> None:
    print(json.dumps(fields, default=str))


# --- GitHub ---------------------------------------------------------------
def _github_token() -> str:
    global _token
    if _token is None:
        _token = SSM.get_parameter(
            Name=os.environ["GITHUB_TOKEN_PARAM"], WithDecryption=True
        )["Parameter"]["Value"]
    return _token


def _github(method: str, url: str):
    if not url.startswith("http"):
        url = GITHUB_API + url
    req = urllib.request.Request(
        url,
        method=method,
        headers={
            "Authorization": f"Bearer {_github_token()}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "resilienthook-reconciler",
        },
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        body = resp.read()
        return (json.loads(body) if body else None), resp.headers.get("Link", "")


def _to_epoch(iso: str) -> int:
    return int(datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp())


def fetch_deliveries(cutoff: int) -> dict:
    """guid -> {"attempts": n, "latest": delivery}, newest-first pages."""
    by_guid: dict = {}
    url = f"/repos/{OWNER}/{REPO}/hooks/{HOOK_ID}/deliveries?per_page=100"
    while url:
        page, link = _github("GET", url)
        reached_cutoff = False
        for d in page or []:
            ts = _to_epoch(d["delivered_at"])
            if ts < cutoff:
                reached_cutoff = True
                continue
            entry = by_guid.setdefault(d["guid"], {"attempts": 0, "latest": None, "latest_ts": 0})
            entry["attempts"] += 1  # original delivery + each redelivery
            if ts >= entry["latest_ts"]:
                entry["latest"], entry["latest_ts"] = d, ts
        match = NEXT_LINK.search(link)
        url = match.group(1) if match and not reached_cutoff else None
    return by_guid


def redeliver(delivery_id: int) -> None:
    _github(
        "POST",
        f"/repos/{OWNER}/{REPO}/hooks/{HOOK_ID}/deliveries/{delivery_id}/attempts",
    )


# --- DynamoDB ---------------------------------------------------------------
def get_rows(guids: list) -> dict:
    rows = {}
    keys = [{"event_id": g} for g in guids]
    for i in range(0, len(keys), 100):
        request = {
            TABLE.name: {
                "Keys": keys[i : i + 100],
                "ProjectionExpression": "event_id, #s, received_at, processing_started_at",
                "ExpressionAttributeNames": {"#s": "status"},
            }
        }
        while request:
            resp = DDB.batch_get_item(RequestItems=request)
            for item in resp["Responses"].get(TABLE.name, []):
                rows[item["event_id"]] = item
            request = resp.get("UnprocessedKeys") or None
            if request:
                time.sleep(0.5)  # throttled; back off and retry the rest
    return rows


def give_up(event_id: str, event_type: str, reason: str, attempts: int) -> bool:
    """Mark FAILED and alert. Conditional, so each event alerts only once."""
    now = int(time.time())
    try:
        TABLE.update_item(
            Key={"event_id": event_id},
            UpdateExpression=(
                "SET #s = :failed, failed_at = :now, failure_reason = :reason, "
                "event_type = if_not_exists(event_type, :etype), "
                "received_at = if_not_exists(received_at, :now), "
                "expires_at = if_not_exists(expires_at, :exp)"
            ),
            ConditionExpression="attribute_not_exists(event_id) OR #s IN (:received, :processing)",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":failed": "FAILED",
                ":received": "RECEIVED",
                ":processing": "PROCESSING",
                ":now": now,
                ":reason": reason,
                ":etype": event_type,
                ":exp": now + TTL_DAYS * 86400,
            },
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False  # already FAILED (alerted earlier) or finished meanwhile
        raise

    SNS.publish(
        TopicArn=TOPIC_ARN,
        Subject="ResilientHook: event unrecoverable",
        Message=json.dumps(
            {
                "event_id": event_id,
                "event_type": event_type,
                "reason": reason,
                "redelivery_attempts": attempts,
                "action": "Investigate; the event will not be retried automatically.",
            },
            indent=2,
        ),
    )
    _log(msg="gave_up", event_id=event_id, reason=reason)
    return True


def _stuck_since(row: dict) -> int:
    if row["status"] == "PROCESSING" and row.get("processing_started_at"):
        return int(row["processing_started_at"])
    return int(row["received_at"])


# --- Main -------------------------------------------------------------------
def handler(event, context):
    now = int(time.time())
    stats = {"deliveries": 0, "missing": 0, "stuck": 0, "redelivered": 0, "failed": 0, "errors": 0}

    deliveries = fetch_deliveries(now - LOOKBACK_SECONDS)
    stats["deliveries"] = len(deliveries)
    rows = get_rows(list(deliveries))

    # 1 + 2: missing or stuck events still inside GitHub's window.
    for guid, info in deliveries.items():
        latest = info["latest"]
        row = rows.get(guid)

        if row is None:
            if 200 <= (latest.get("status_code") or 0) < 300:
                continue  # GitHub got a 2xx, so a row exists or has expired
            reason = "missing"
        else:
            if row["status"] not in ("RECEIVED", "PROCESSING"):
                continue  # PROCESSED or FAILED
            if now - _stuck_since(row) < STUCK_SECONDS:
                continue  # probably still in flight; give it time
            reason = f"stuck_{row['status'].lower()}"

        stats["missing" if reason == "missing" else "stuck"] += 1
        redeliveries_so_far = info["attempts"] - 1

        if redeliveries_so_far >= MAX_ATTEMPTS:
            if give_up(guid, latest.get("event", "unknown"), f"{reason}_max_attempts", redeliveries_so_far):
                stats["failed"] += 1
            continue

        try:
            redeliver(latest["id"])
            stats["redelivered"] += 1
            _log(msg="redelivered", event_id=guid, reason=reason, attempt=redeliveries_so_far + 1)
        except urllib.error.HTTPError as e:
            stats["errors"] += 1
            _log(msg="redeliver_failed", event_id=guid, http_status=e.code)

    # 3: rows stuck since before the lookback window. GitHub can no longer
    # redeliver them, so they are unrecoverable — surface them loudly.
    for status in ("RECEIVED", "PROCESSING"):
        kwargs = {
            "IndexName": STATUS_INDEX,
            "KeyConditionExpression": Key("status").eq(status)
            & Key("received_at").lt(now - LOOKBACK_SECONDS),
        }
        while True:
            resp = TABLE.query(**kwargs)
            for item in resp["Items"]:
                if give_up(item["event_id"], item.get("event_type", "unknown"), "outside_retention_window", 0):
                    stats["failed"] += 1
            if "LastEvaluatedKey" not in resp:
                break
            kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

    _log(msg="reconcile_complete", **stats)
    return stats
