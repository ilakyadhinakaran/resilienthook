from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    CfnOutput,
    aws_dynamodb as dynamodb,
    aws_lambda as _lambda,
    aws_lambda_event_sources as event_sources,
    aws_logs as logs,
    aws_sqs as sqs,
    aws_ssm as ssm,
)
from constructs import Construct

WEBHOOK_SECRET_PARAM = "/resilienthook/webhook-secret"
WORKER_TIMEOUT_SECONDS = 30


class ResilienthookStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # --- Event state table -------------------------------------------
        # Provisioned (not on-demand) so it stays inside the always-free
        # 25 RCU / 25 WCU allowance. Phase 4's GSI will use part of the rest.
        table = dynamodb.Table(
            self,
            "EventsTable",
            table_name="resilienthook-events",
            partition_key=dynamodb.Attribute(
                name="event_id", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PROVISIONED,
            read_capacity=5,
            write_capacity=10,
            time_to_live_attribute="expires_at",
            removal_policy=RemovalPolicy.DESTROY,
        )

        webhook_secret = ssm.StringParameter.from_secure_string_parameter_attributes(
            self, "WebhookSecret", parameter_name=WEBHOOK_SECRET_PARAM
        )

        # --- Queues --------------------------------------------------------
        # Messages that fail 3 times land here instead of retrying forever.
        dlq = sqs.Queue(
            self,
            "EventsDLQ",
            queue_name="resilienthook-events-dlq",
            retention_period=Duration.days(14),
        )

        # Visibility timeout = 6x the worker timeout (AWS recommendation for
        # Lambda event sources), so a message isn't redelivered while a
        # worker is still legitimately processing it.
        queue = sqs.Queue(
            self,
            "EventsQueue",
            queue_name="resilienthook-events",
            visibility_timeout=Duration.seconds(WORKER_TIMEOUT_SECONDS * 6),
            retention_period=Duration.days(4),
            dead_letter_queue=sqs.DeadLetterQueue(max_receive_count=3, queue=dlq),
        )

        # --- Receiver Lambda -----------------------------------------------
        receiver_logs = logs.LogGroup(
            self,
            "ReceiverLogs",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
        )

        receiver = _lambda.Function(
            self,
            "Receiver",
            runtime=_lambda.Runtime.PYTHON_3_12,
            architecture=_lambda.Architecture.ARM_64,
            handler="handler.handler",
            code=_lambda.Code.from_asset("lambdas/receiver"),
            timeout=Duration.seconds(10),
            memory_size=256,
            log_group=receiver_logs,
            environment={
                "TABLE_NAME": table.table_name,
                "QUEUE_URL": queue.queue_url,
                "WEBHOOK_SECRET_PARAM": WEBHOOK_SECRET_PARAM,
            },
        )

        table.grant_write_data(receiver)
        queue.grant_send_messages(receiver)
        webhook_secret.grant_read(receiver)

        # Function URL instead of API Gateway: free, and gives a public HTTPS
        # endpoint. Auth is NONE because GitHub can't sign IAM requests —
        # the HMAC signature check in the handler is the real auth.
        url = receiver.add_function_url(auth_type=_lambda.FunctionUrlAuthType.NONE)

        # --- Worker Lambda -------------------------------------------------
        worker_logs = logs.LogGroup(
            self,
            "WorkerLogs",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
        )

        worker = _lambda.Function(
            self,
            "Worker",
            runtime=_lambda.Runtime.PYTHON_3_12,
            architecture=_lambda.Architecture.ARM_64,
            handler="handler.handler",
            code=_lambda.Code.from_asset("lambdas/worker"),
            timeout=Duration.seconds(WORKER_TIMEOUT_SECONDS),
            memory_size=256,
            log_group=worker_logs,
            environment={
                "TABLE_NAME": table.table_name,
                # Fault injection for chaos tests (0.0 - 1.0). Keep 0 normally.
                "FAIL_RATE": "0",
                # A PROCESSING claim older than this can be taken over.
                # Must be > worker timeout and < queue visibility timeout.
                "LEASE_SECONDS": "60",
            },
        )

        table.grant_read_write_data(worker)

        worker.add_event_source(
            event_sources.SqsEventSource(
                queue,
                batch_size=10,
                # Only failed messages in a batch are retried, not all 10.
                report_batch_item_failures=True,
            )
        )

        CfnOutput(self, "ReceiverUrl", value=url.url)
        CfnOutput(self, "TableName", value=table.table_name)
        CfnOutput(self, "QueueUrl", value=queue.queue_url)
        CfnOutput(self, "DlqUrl", value=dlq.queue_url)
        CfnOutput(self, "WorkerName", value=worker.function_name)
