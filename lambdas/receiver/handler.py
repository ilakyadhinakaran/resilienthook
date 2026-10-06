import base64
import hashlib
import hmac
import json
import os
import time

import boto3
from botocore.exceptions import ClientError

TABLE = boto3.resource("dynamodb").Table(os.environ["TABLE_NAME"])
SSM = boto3.client("ssm")
TTL_DAYS = 7

_secret = None  # cached across warm invocations


def _get_secret() -> bytes:
    global _secret
    if _secret is None:
        _secret = SSM.get_parameter(
            Name=os.environ["WEBHOOK_SECRET_PARAM"], WithDecryption=True
        )["Parameter"]["Value"].encode()
    return _secret


def _resp(code: int, msg: str) -> dict:
    return {
        "statusCode": code,
        "headers": {"content-type": "application/json"},
        "body": json.dumps({"message": msg}),
    }


def _log(**fields) -> None:
    print(json.dumps(fields))


def handler(event, context):
    headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}

    # Signature must be computed over the RAW body bytes, exactly as sent.
    body = event.get("body") or ""
    raw = base64.b64decode(body) if event.get("isBase64Encoded") else body.encode()

    signature = headers.get("x-hub-signature-256", "")
    expected = "sha256=" + hmac.new(_get_secret(), raw, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(signature, expected):
        _log(msg="invalid_signature")
        return _resp(401, "invalid signature")

    delivery_id = headers.get("x-github-delivery")
    if not delivery_id:
        return _resp(400, "missing delivery id")

    event_type = headers.get("x-github-event", "unknown")
    now = int(time.time())

    try:
        TABLE.put_item(
            Item={
                "event_id": delivery_id,
                "event_type": event_type,
                "status": "RECEIVED",
                "received_at": now,
                "expires_at": now + TTL_DAYS * 86400,
            },
            # Idempotency: first writer wins.
            ConditionExpression="attribute_not_exists(event_id)",
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            # TEMPORARY (Phase 2): Phase 4 changes this to short-circuit
            # only when status is PROCESSING/PROCESSED.
            _log(msg="duplicate", event_id=delivery_id)
            return _resp(200, "duplicate")
        raise

    _log(msg="received", event_id=delivery_id, event_type=event_type)
    return _resp(202, "received")
