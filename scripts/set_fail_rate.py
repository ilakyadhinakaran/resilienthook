#!/usr/bin/env python3
"""Turn fault injection on/off without redeploying.

Usage:
    python scripts/set_fail_rate.py receiver 1     # every delivery fails
    python scripts/set_fail_rate.py worker 0.3     # 30% of processing crashes
    python scripts/set_fail_rate.py receiver 0     # back to normal

Always set it back to 0 after a test: `cdk deploy` will NOT reset it
unless the stack template itself changed.
"""
import sys

import boto3

STACK = "ResilienthookStack"
OUTPUT_KEYS = {"receiver": "ReceiverName", "worker": "WorkerName"}


def main() -> None:
    if len(sys.argv) != 3 or sys.argv[1] not in OUTPUT_KEYS:
        sys.exit(__doc__)
    target, rate = sys.argv[1], sys.argv[2]
    if not 0 <= float(rate) <= 1:
        sys.exit("rate must be between 0 and 1")

    outputs = {
        o["OutputKey"]: o["OutputValue"]
        for o in boto3.client("cloudformation").describe_stacks(StackName=STACK)[
            "Stacks"
        ][0]["Outputs"]
    }
    function_name = outputs[OUTPUT_KEYS[target]]

    lam = boto3.client("lambda")
    env = lam.get_function_configuration(FunctionName=function_name)["Environment"]["Variables"]
    env["FAIL_RATE"] = rate  # keep every other variable untouched
    lam.update_function_configuration(FunctionName=function_name, Environment={"Variables": env})
    lam.get_waiter("function_updated_v2").wait(FunctionName=function_name)
    print(f"{target} FAIL_RATE = {rate}")


if __name__ == "__main__":
    main()
