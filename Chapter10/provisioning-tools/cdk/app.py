#!/usr/bin/env python3
import aws_cdk as cdk
from ch10_5_example_cdk_stack import Ch105ExampleCdkStack

app = cdk.App()
Ch105ExampleCdkStack(app, "Ch105ExampleCdkStack")
app.synth()
