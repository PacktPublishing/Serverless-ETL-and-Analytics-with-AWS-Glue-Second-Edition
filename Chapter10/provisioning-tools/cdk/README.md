# Provisioning AWS Glue workflows and resources with AWS Cloud Development Kit (CDK)

This example shows how to provision a Glue workflow (consisting of a Glue Crawler and two Glue Jobs) using AWS CDK with Python. The workflow is equivalent to the CloudFormation example in `../cloudformation/`, but defined programmatically with CDK.

## What this CDK stack deploys

- A Glue Crawler (`ch10_5_example_cdk_sales`) that populates a table based on `sales-data.json`
- A Glue Job (`ch10_5_example_cdk_partitioning`) that extracts sales data and writes it with `category` and `report_year` partitioning
- A Glue Job (`ch10_5_example_cdk_gen_report`) that generates a report based on the partitioned data
- A Glue Workflow (`ch10_5_example_cdk_workflow`) that orchestrates the crawler and jobs with triggers

## Prerequisites

- An AWS account with appropriate permissions
- Python 3.8 or later
- AWS CDK CLI installed (`npm install -g aws-cdk`)
- AWS CLI configured with your credentials
- An IAM role for the Glue Crawler
- An IAM role for the Glue Jobs
- An S3 bucket to store the Glue job scripts and sales data

## Step-by-step instructions

### Step 1: Upload the sales data and Glue job scripts to S3

Upload `sales-data.json` (located in `Chapter10/`) and the two Glue job scripts to your S3 bucket:

```bash
# Upload sales data
aws s3 cp ../../sales-data.json s3://<your-bucket>/<path-to-sales-data>/

# Upload Glue job scripts
aws s3 cp ch10_5_example_cdk_partitioning.py s3://<your-bucket>/<path-to-scripts>/
aws s3 cp ch10_5_example_cdk_gen_report.py s3://<your-bucket>/<path-to-scripts>/
```

### Step 2: Set up the CDK project

Navigate to this directory and install the dependencies:

```bash
# Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Step 3: Review and synthesize the CDK stack

Before deploying, you can review the CloudFormation template that CDK will generate:

```bash
cdk synth
```

This outputs the synthesized CloudFormation template to the console. Review it to confirm the resources match your expectations.

### Step 4: Deploy the CDK stack

Deploy the stack with the required parameters:

```bash
cdk deploy \
  --parameters GlueCrawlerRoleArn=arn:aws:iam::<YOUR_ACCOUNT_ID>:role/service-role/<CRAWLER_ROLE_NAME> \
  --parameters GlueJobRoleArn=arn:aws:iam::<YOUR_ACCOUNT_ID>:role/service-role/<JOB_ROLE_NAME> \
  --parameters GlueJobScriptLocation=s3://<your-bucket>/<path-to-scripts>/ \
  --parameters SalesDataLocation=s3://<your-bucket>/<path-to-sales-data>/ \
  --parameters DataLakeLocation=s3://<your-bucket>/<path-to-datalake>/ \
  --parameters DatabaseName=<your-database-name> \
  --parameters TableName=ch10_5_example_cdk_sales \
  --parameters ReportYear=2024
```

Replace the placeholder values with your actual configuration.

### Step 5: Run the workflow

After the deployment completes, run the workflow from the AWS Glue console:

1. Open the AWS Glue console
2. Navigate to **ETL** > **Workflows**
3. Select `ch10_5_example_cdk_workflow`
4. Click **Run**

The workflow will execute in the following order:
1. The on-demand trigger starts the Crawler (`ch10_5_example_cdk_sales`)
2. After the Crawler succeeds, the partitioning job (`ch10_5_example_cdk_partitioning`) runs
3. After the partitioning job succeeds, the report generation job (`ch10_5_example_cdk_gen_report`) runs

### Step 6: Verify the results

Once the workflow completes, verify the results:

1. Check the Glue Data Catalog for the new tables (`ch10_5_example_cdk_sales_analysis` and `ch10_5_example_cdk_sales_report`)
2. Check the S3 data lake location for the partitioned data and report output

### Step 7: Clean up

To remove all resources created by this stack:

```bash
cdk destroy
```

Note that this will not delete the data in S3 or the tables in the Glue Data Catalog. You will need to clean those up manually if desired.
