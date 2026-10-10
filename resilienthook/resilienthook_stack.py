from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    CfnOutput,
    aws_dynamodb as dynamodb,
    aws_events as events,
    aws_events_targets as targets,
    aws_lambda as _lambda,
    aws_lambda_event_sources as event_sources,
    aws_logs as logs,
    aws_sns as sns,
    aws_sns_subscriptions as subs,
    aws_sqs as sqs,
    aws_ssm as ssm,
)
from constructs import Construct

# ---- Config -------------------------------------------------------------
WEBHOOK_SECRET_PARAM = "/resilienthook/webhook-secret"
GITHUB_TOKEN_PARAM = "/resilienthook/github-token"
GH_OWNER = "ilakyadhinakaran"
GH_REPO = "resilienthook-test"
GH_HOOK_ID = "693022552"
ALERT_EMAIL = "ilakyadhinakaran129@gmail.com"  # change if you want alerts elsewhere

WORKER_TIMEOUT_SECONDS = 30
STATUS_INDEX = "status-received_at-index"


class ResilienthookStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # --- Event state table -------------------------------------------
        # Provisioned capacity stays inside the always-free 25 RCU / 25 WCU:
        # table 5R/10W + GSI 5R/10W = 10R/20W total.
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

        # Lets the reconciler find rows by status + age without a table scan.
        table.add_global_secondary_index(
            index_name=STATUS_INDEX,
            partition_key=dynamodb.Attribute(
                name="status", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="received_at", type=dynamodb.AttributeType.NUMBER
            ),
            projection_type=dynamodb.ProjectionType.ALL,
            read_capacity=5,
            write_capacity=10,
        )

        webhook_secret = ssm.StringParameter.from_secure_string_parameter_attributes(
            self, "WebhookSecret", parameter_name=WEBHOOK_SECRET_PARAM
        )
        github_token = ssm.StringParameter.from_secure_string_parameter_attributes(
            self, "GithubToken", parameter_name=GITHUB_TOKEN_PARAM
        )

        # --- Queues --------------------------------------------------------
        dlq = sqs.Queue(
            self,
            "EventsDLQ",
            queue_name="resilienthook-events-dlq",
            retention_period=Duration.days(14),
        )

        # Visibility timeout = 6x worker timeout (AWS recommendation).
        queue = sqs.Queue(
            self,
            "EventsQueue",
            queue_name="resilienthook-events",
            visibility_timeout=Duration.seconds(WORKER_TIMEOUT_SECONDS * 6),
            retention_period=Duration.days(4),
            dead_letter_queue=sqs.DeadLetterQueue(max_receive_count=3, queue=dlq),
        )

        # --- Alerts --------------------------------------------------------
        alerts = sns.Topic(self, "AlertsTopic", topic_name="resilienthook-alerts")
        alerts.add_subscription(subs.EmailSubscription(ALERT_EMAIL))

        # --- Receiver Lambda -----------------------------------------------
        receiver = _lambda.Function(
            self,
            "Receiver",
            runtime=_lambda.Runtime.PYTHON_3_12,
            architecture=_lambda.Architecture.ARM_64,
            handler="handler.handler",
            code=_lambda.Code.from_asset("lambdas/receiver"),
            timeout=Duration.seconds(10),
            memory_size=256,
            log_group=self._log_group("ReceiverLogs"),
            environment={
                "TABLE_NAME": table.table_name,
                "QUEUE_URL": queue.queue_url,
                "WEBHOOK_SECRET_PARAM": WEBHOOK_SECRET_PARAM,
                # Fault injection: fraction of deliveries to reject with 503
                # BEFORE recording them (simulates an outage). Keep 0 normally.
                "FAIL_RATE": "0",
            },
        )
        table.grant_read_write_data(receiver)
        queue.grant_send_messages(receiver)
        webhook_secret.grant_read(receiver)

        # Function URL: free public HTTPS endpoint. The HMAC check in the
        # handler is the real auth (GitHub can't sign IAM requests).
        url = receiver.add_function_url(auth_type=_lambda.FunctionUrlAuthType.NONE)

        # --- Worker Lambda -------------------------------------------------
        worker = _lambda.Function(
            self,
            "Worker",
            runtime=_lambda.Runtime.PYTHON_3_12,
            architecture=_lambda.Architecture.ARM_64,
            handler="handler.handler",
            code=_lambda.Code.from_asset("lambdas/worker"),
            timeout=Duration.seconds(WORKER_TIMEOUT_SECONDS),
            memory_size=256,
            log_group=self._log_group("WorkerLogs"),
            environment={
                "TABLE_NAME": table.table_name,
                "FAIL_RATE": "0",
                # Must be > worker timeout and < queue visibility timeout.
                "LEASE_SECONDS": "60",
            },
        )
        table.grant_read_write_data(worker)
        worker.add_event_source(
            event_sources.SqsEventSource(
                queue, batch_size=10, report_batch_item_failures=True
            )
        )

        # --- Reconciler Lambda ---------------------------------------------
        reconciler = _lambda.Function(
            self,
            "Reconciler",
            runtime=_lambda.Runtime.PYTHON_3_12,
            architecture=_lambda.Architecture.ARM_64,
            handler="handler.handler",
            code=_lambda.Code.from_asset("lambdas/reconciler"),
            timeout=Duration.minutes(2),
            memory_size=256,
            log_group=self._log_group("ReconcilerLogs"),
            environment={
                "TABLE_NAME": table.table_name,
                "STATUS_INDEX": STATUS_INDEX,
                "TOPIC_ARN": alerts.topic_arn,
                "GITHUB_TOKEN_PARAM": GITHUB_TOKEN_PARAM,
                "GH_OWNER": GH_OWNER,
                "GH_REPO": GH_REPO,
                "GH_HOOK_ID": GH_HOOK_ID,
                # GitHub only keeps ~3 days of deliveries; stay just under it.
                "LOOKBACK_SECONDS": str(70 * 3600),
                # RECEIVED/PROCESSING rows older than this count as stuck.
                "STUCK_SECONDS": "600",
                # Redeliveries per event before giving up and alerting.
                "MAX_ATTEMPTS": "3",
            },
        )
        table.grant_read_write_data(reconciler)
        alerts.grant_publish(reconciler)
        github_token.grant_read(reconciler)

        # Scheduled EventBridge rule (scheduled events are free).
        events.Rule(
            self,
            "ReconcileSchedule",
            schedule=events.Schedule.rate(Duration.minutes(15)),
            targets=[targets.LambdaFunction(reconciler)],
        )

        # --- Outputs -------------------------------------------------------
        CfnOutput(self, "ReceiverUrl", value=url.url)
        CfnOutput(self, "TableName", value=table.table_name)
        CfnOutput(self, "QueueUrl", value=queue.queue_url)
        CfnOutput(self, "DlqUrl", value=dlq.queue_url)
        CfnOutput(self, "ReceiverName", value=receiver.function_name)
        CfnOutput(self, "WorkerName", value=worker.function_name)
        CfnOutput(self, "ReconcilerName", value=reconciler.function_name)

    def _log_group(self, construct_id: str) -> logs.LogGroup:
        return logs.LogGroup(
            self,
            construct_id,
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
        )
