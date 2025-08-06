#!/usr/bin/env python3
import aws_cdk as cdk
from etl_stack import EtlStack

app = cdk.App()
EtlStack(app, "EtlStack")
app.synth()
