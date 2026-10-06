from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    CfnOutput,
    aws_dynamodb as dynamodb,
    aws_lambda as _lambda,
    aws_logs as logs,
    aws_ssm as ssm,
)
from constructs import Construct

WEBHOOK_SECRET_PARAM = "/resilienthook/webhook-secret"


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

        # --- Receiver Lambda ---------------------------------------------
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
                "WEBHOOK_SECRET_PARAM": WEBHOOK_SECRET_PARAM,
            },
        )

        table.grant_write_data(receiver)
        webhook_secret.grant_read(receiver)

        # Function URL instead of API Gateway: free, and gives a public HTTPS
        # endpoint. Auth is NONE because GitHub can't sign IAM requests —
        # the HMAC signature check in the handler is the real auth.
        url = receiver.add_function_url(auth_type=_lambda.FunctionUrlAuthType.NONE)

        CfnOutput(self, "ReceiverUrl", value=url.url)
        CfnOutput(self, "TableName", value=table.table_name)
