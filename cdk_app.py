#!/usr/bin/env python3
import aws_cdk as cdk
from aws_cdk import (
    Stack,
    Duration,
    aws_lambda as _lambda,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
)
from constructs import Construct

class YahooFantasyLambdaStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create Lambda layer with dependencies
        lambda_layer = _lambda.LayerVersion(
            self, "YahooFantasyDependencies",
            code=_lambda.Code.from_asset("lambda_layer"),
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_9],
            description="Dependencies for Yahoo Fantasy Baseball scripts"
        )

        # Weekly Updates Lambda Function
        weekly_lambda = _lambda.Function(
            self, "WeeklyUpdatesFunction",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="lambda_handler.lambda_handler_weekly",
            code=_lambda.Code.from_asset("."),
            layers=[lambda_layer],
            timeout=Duration.minutes(15),
            memory_size=512,
            environment={
                "PYTHONPATH": "/opt/python:/var/runtime:/var/task/src"
            }
        )

        # Live Standings Lambda Function  
        live_standings_lambda = _lambda.Function(
            self, "LiveStandingsFunction",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="lambda_handler.lambda_handler_live_standings",
            code=_lambda.Code.from_asset("."),
            layers=[lambda_layer],
            timeout=Duration.minutes(5),
            memory_size=256,
            environment={
                "PYTHONPATH": "/opt/python:/var/runtime:/var/task/src"
            }
        )

        # EventBridge rule for weekly updates (Sundays at 5am ET)
        weekly_rule = events.Rule(
            self, "WeeklyUpdatesSchedule",
            schedule=events.Schedule.cron(
                minute="0",
                hour="9",  # 5am ET = 9am UTC (adjust for daylight saving)
                day="*",
                month="*",
                week_day="SUN"
            )
        )
        weekly_rule.add_target(targets.LambdaFunction(weekly_lambda))

        # EventBridge rule for live standings (every 15 minutes)
        live_standings_rule = events.Rule(
            self, "LiveStandingsSchedule",
            schedule=events.Schedule.rate(Duration.minutes(15))
        )
        live_standings_rule.add_target(targets.LambdaFunction(live_standings_lambda))

app = cdk.App()
YahooFantasyLambdaStack(app, "YahooFantasyLambdaStack")
app.synth()