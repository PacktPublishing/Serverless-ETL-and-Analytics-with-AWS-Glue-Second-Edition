from aws_cdk import (
    Stack,
    CfnParameter,
    aws_glue as glue,
)
from constructs import Construct
import json


class Ch105ExampleCdkStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # -----------------------------------------------------------
        # Parameters
        # -----------------------------------------------------------
        glue_crawler_role_arn = CfnParameter(
            self, "GlueCrawlerRoleArn",
            description="IAM Role ARN for the Glue Crawler.",
            type="String",
            default="arn:aws:iam::<YOUR_ACCOUNT_ID>:role/service-role/<ROLE_NAME>",
        )

        glue_job_role_arn = CfnParameter(
            self, "GlueJobRoleArn",
            description="IAM Role ARN for the Glue Jobs.",
            type="String",
            default="arn:aws:iam::<YOUR_ACCOUNT_ID>:role/service-role/<ROLE_NAME>",
        )

        glue_job_script_location = CfnParameter(
            self, "GlueJobScriptLocation",
            description=(
                "The combination of S3 bucket name and path that locates "
                "ch10_5_example_cdk_partitioning.py and ch10_5_example_cdk_gen_report.py. "
                "This location must end with a slash (/) and not include any files."
            ),
            type="String",
            default="s3://<bucket-and-path-to-script>/",
            allowed_pattern="^(s3://)(.*)(/)$",
        )

        sales_data_location = CfnParameter(
            self, "SalesDataLocation",
            description=(
                "The combination of S3 bucket name and path that stores sales-data.json. "
                "This location must end with a slash (/) and not include any files."
            ),
            type="String",
            default="s3://<bucket-and-path-to-sales-data.json>/",
            allowed_pattern="^(s3://)(.*)(/)$",
        )

        datalake_location = CfnParameter(
            self, "DataLakeLocation",
            description=(
                "The combination of S3 bucket name and path that stores "
                "the analytic sales data and a sales report. "
                "This location must end with a slash (/) and not include any files."
            ),
            type="String",
            default="s3://<bucket-and-path>/",
            allowed_pattern="^(s3://)(.*)(/)$",
        )

        database_name = CfnParameter(
            self, "DatabaseName",
            description="Database name for the table of the sales data.",
            type="String",
        )

        table_name = CfnParameter(
            self, "TableName",
            description=(
                "Table name for the table of sales data that crawler creates. "
                "You can also set a custom table name. "
                'If you set a custom table name, note that keep the table name starting from '
                '"ch10_5_example_cdk_" and ending with "sales" '
                "(or the last s3 path of your data lake location)."
            ),
            type="String",
            default="ch10_5_example_cdk_sales",
        )

        report_year = CfnParameter(
            self, "ReportYear",
            description="The year when you want to aggregate the dataset and generate a report.",
            type="Number",
            default=2024,
        )

        # -----------------------------------------------------------
        # Glue Crawler
        # -----------------------------------------------------------
        sales_crawler = glue.CfnCrawler(
            self, "SalesCrawler",
            name="ch10_5_example_cdk_sales",
            role=glue_crawler_role_arn.value_as_string,
            database_name=database_name.value_as_string,
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path=sales_data_location.value_as_string,
                    )
                ]
            ),
            table_prefix="ch10_5_example_cdk_",
            schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                update_behavior="UPDATE_IN_DATABASE",
                delete_behavior="DEPRECATE_IN_DATABASE",
            ),
        )

        # -----------------------------------------------------------
        # Glue Jobs
        # -----------------------------------------------------------
        partitioning_job = glue.CfnJob(
            self, "PartitioningJob",
            name="ch10_5_example_cdk_partitioning",
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=f"{glue_job_script_location.value_as_string}ch10_5_example_cdk_partitioning.py",
                python_version="3",
            ),
            default_arguments={
                "--enable-glue-datacatalog": "",
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-metrics": "",
                "--job-language": "python",
                "--extra-jars": "s3://crawler-public/json/serde/json-serde.jar",
            },
            max_retries=0,
            glue_version="4.0",
            role=glue_job_role_arn.value_as_string,
            worker_type="G.1X",
            number_of_workers=3,
            timeout=2880,
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1,
            ),
        )

        gen_report_job = glue.CfnJob(
            self, "GenReportJob",
            name="ch10_5_example_cdk_gen_report",
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=f"{glue_job_script_location.value_as_string}ch10_5_example_cdk_gen_report.py",
                python_version="3",
            ),
            default_arguments={
                "--enable-glue-datacatalog": "",
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-metrics": "",
                "--job-language": "python",
            },
            max_retries=0,
            glue_version="4.0",
            role=glue_job_role_arn.value_as_string,
            worker_type="G.1X",
            number_of_workers=3,
            timeout=2880,
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1,
            ),
        )

        # -----------------------------------------------------------
        # Glue Workflow
        # -----------------------------------------------------------
        workflow = glue.CfnWorkflow(
            self, "SalesReportWorkflow",
            name="ch10_5_example_cdk_workflow",
            default_run_properties={
                "datalake_location": datalake_location.value_as_string,
                "database": database_name.value_as_string,
                "table": table_name.value_as_string,
                "report_year": report_year.value_as_string,
            },
        )

        # -----------------------------------------------------------
        # Triggers
        # -----------------------------------------------------------
        glue.CfnTrigger(
            self, "OndemandStartTrigger",
            name="ch10_5_example_cdk_ondemand_start",
            type="ON_DEMAND",
            workflow_name=workflow.name,
            actions=[
                glue.CfnTrigger.ActionProperty(
                    crawler_name=sales_crawler.name,
                )
            ],
        )

        glue.CfnTrigger(
            self, "EventRunPartitioningTrigger",
            name="ch10_5_example_cdk_event_run_partitioning",
            type="CONDITIONAL",
            workflow_name=workflow.name,
            start_on_creation=True,
            predicate=glue.CfnTrigger.PredicateProperty(
                conditions=[
                    glue.CfnTrigger.ConditionProperty(
                        logical_operator="EQUALS",
                        crawler_name=sales_crawler.name,
                        crawl_state="SUCCEEDED",
                    )
                ]
            ),
            actions=[
                glue.CfnTrigger.ActionProperty(
                    job_name=partitioning_job.name,
                )
            ],
        )

        glue.CfnTrigger(
            self, "EventRunGenReportTrigger",
            name="ch10_5_example_cdk_event_gen_report",
            type="CONDITIONAL",
            workflow_name=workflow.name,
            start_on_creation=True,
            predicate=glue.CfnTrigger.PredicateProperty(
                conditions=[
                    glue.CfnTrigger.ConditionProperty(
                        logical_operator="EQUALS",
                        job_name=partitioning_job.name,
                        state="SUCCEEDED",
                    )
                ]
            ),
            actions=[
                glue.CfnTrigger.ActionProperty(
                    job_name=gen_report_job.name,
                )
            ],
        )
