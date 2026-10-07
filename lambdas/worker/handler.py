import json
import os
import random
import time

import boto3
from botocore.exceptions import ClientError

TABLE = boto3.resource("dynamodb").Table(os.environ["TABLE_NAME"])
FAIL_RATE = float(os.environ.get("FAIL_RATE", "0"))
LEASE_SECONDS = int(os.environ.get("LEASE_SECONDS", "60"))


class StillLeased(Exception):
    """Another worker holds a live claim on this event; retry later."""


def _log(**fields) -> None:
    print(json.dumps(fields))


def _claim(event_id: str, now: int) -> bool:
    """RECEIVED (or an expired PROCESSING lease) -> PROCESSING.

    Returns True if we now own the event, False if it's already done.
    Raises StillLeased if someone else is actively processing it.
    """
    try:
        TABLE.update_item(
            Key={"event_id": event_id},
            UpdateExpression=(
                "SET #s = :processing, processing_started_at = :now, "
                "attempts = if_not_exists(attempts, :zero) + :one"
            ),
            ConditionExpression=(
                "#s = :received OR "
                "(#s = :processing AND processing_started_at < :lease_cutoff)"
            ),
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":processing": "PROCESSING",
                ":received": "RECEIVED",
                ":now": now,
                ":lease_cutoff": now - LEASE_SECONDS,
                ":zero": 0,
                ":one": 1,
            },
            ReturnValuesOnConditionCheckFailure="ALL_OLD",
        )
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
        old_status = e.response.get("Item", {}).get("status", {}).get("S")
        if old_status == "PROCESSING":
            raise StillLeased(event_id)
        # PROCESSED (or row missing): nothing to do, ack the message.
        _log(msg="skip", event_id=event_id, status=old_status)
        return False


def _business_logic(msg: dict) -> dict:
    """Demo 'processing': extract a summary of the GitHub event."""
    payload = json.loads(msg.get("payload") or "{}")
    return {
        "action": payload.get("action", "-"),
        "repo": (payload.get("repository") or {}).get("full_name", "-"),
        "sender": (payload.get("sender") or {}).get("login", "-"),
    }


def _complete(event_id: str, summary: dict, now: int) -> None:
    """PROCESSING -> PROCESSED."""
    try:
        TABLE.update_item(
            Key={"event_id": event_id},
            UpdateExpression=(
                "SET #s = :processed, processed_at = :now, "
                "summary_action = :a, summary_repo = :r, summary_sender = :u"
            ),
            ConditionExpression="#s = :processing",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":processed": "PROCESSED",
                ":processing": "PROCESSING",
                ":now": now,
                ":a": summary["action"],
                ":r": summary["repo"],
                ":u": summary["sender"],
            },
        )
    except ClientError as e:
        if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
        # Our lease expired and another worker already finished it.
        _log(msg="lost_lease", event_id=event_id)


def _process(record: dict) -> None:
    msg = json.loads(record["body"])
    event_id = msg["event_id"]

    if not _claim(event_id, int(time.time())):
        return

    # Fault injection for chaos tests: crash AFTER claiming, which leaves
    # the row in PROCESSING — exactly the case the lease exists for.
    if random.random() < FAIL_RATE:
        raise RuntimeError("injected failure")

    summary = _business_logic(msg)
    _complete(event_id, summary, int(time.time()))
    _log(msg="processed", event_id=event_id, **summary)


def handler(event, context):
    failures = []
    for record in event["Records"]:
        try:
            _process(record)
        except Exception as e:  # noqa: BLE001
            _log(msg="failed", message_id=record["messageId"], error=repr(e))
            failures.append({"itemIdentifier": record["messageId"]})
    # Partial batch response: only the failed messages are retried.
    return {"batchItemFailures": failures}
