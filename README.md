# Polarapt HCM Real-Time Data Platform

A real-time, event-driven data engineering pipeline built on AWS for the Data Engineering Hackathon. It ingests live **job postings** and **foreign-exchange rates** every minute, lands them in S3, routes them through SNS/SQS FIFO queues, and loads them into **Snowflake** (jobs) and **SQL Server** (FX).

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Tech Stack](#tech-stack)
4. [Data Sources](#data-sources)
5. [Data Contract](#data-contract)
6. [Repository Structure](#repository-structure)
7. [Prerequisites](#prerequisites)
8. [Setup Guide](#setup-guide)
9. [Testing the Pipeline](#testing-the-pipeline)
10. [Troubleshooting](#troubleshooting)
11. [Cost and Limits](#cost-and-limits)
12. [Security Notes](#security-notes)
13. [Cleanup](#cleanup)
14. [Scope and Future Work](#scope-and-future-work)

---

## Overview

The platform simulates an HCM (Human Capital Management) data feed:

- **Ingestion:** EventBridge triggers ingest Lambdas every minute.
- **Raw storage:** Every API response is stored in S3 as JSON with metadata.
- **Event routing:** S3 events fan out through SNS, and a router Lambda republishes them to an SNS FIFO topic with a `source` attribute.
- **Queueing:** Two SQS FIFO queues receive messages by source (jobs, fx).
- **Processing:** A Lambda loads FX rates into SQL Server, and a Glue Python Shell job loads job postings into Snowflake.

## Architecture

```
EventBridge (rate: 1 min)
   |-- lambda_jobs_ingest --> S3 raw/jobs/YYYY/MM/DD/HHMM.json
   |-- lambda_fx_ingest   --> S3 raw/fx/YYYY/MM/DD/HHMM.json
                                  |
                          S3 ObjectCreated event
                                  v
                       SNS standard topic: raw-events
                                  v
                          lambda_router
                      (reads S3 object metadata)
                                  v
                    SNS FIFO topic: hcm-routed-events.fifo
                   /  filter: source=jobs   \  filter: source=fx
                  v                          v
        jobs-queue.fifo (SQS)         fx-queue.fifo (SQS)
                  v                          v
   EventBridge Scheduler (5 min)    EventBridge Rule (5 min)
                  v                          v
     Glue job: jobs_to_snowflake     lambda_fx_processor
                  v                          v
        Snowflake JOB_POSTINGS       SQL Server dbo.fx_rates
```

**Analogy:** EventBridge is the alarm clock, Lambda the delivery boy, S3 the warehouse, SNS the warehouse bell, the router Lambda the sorting clerk, SQS FIFO queues the separate counters, and Glue/Lambda processors the final destination workers.

## Tech Stack

| Layer | Service |
|---|---|
| Scheduling | Amazon EventBridge (Rules and Scheduler) |
| Ingestion | AWS Lambda (Python 3.12) |
| Raw storage | Amazon S3 |
| Messaging | Amazon SNS (standard and FIFO), Amazon SQS FIFO |
| Routing | AWS Lambda (`lambda_router`) |
| FX warehouse | Amazon RDS for SQL Server Express |
| Jobs warehouse | Snowflake (loaded by AWS Glue Python Shell) |
| Access control | AWS IAM (one role per component) |
| Monitoring | Amazon CloudWatch Logs |

## Data Sources

| Source | Provider | Used for |
|---|---|---|
| Jobs | USAJOBS API | Job postings (title, organization, location, salary, open/close dates) |
| FX | Open Exchange Rates | Currency exchange rates against a base currency |

Benchmarks (BLS) were intentionally skipped. See [Scope and Future Work](#scope-and-future-work).

## Data Contract

Every raw file in S3 follows this envelope:

```json
{
  "contract": { "status": "success" },
  "data": { }
}
```

- `contract.status` is `success` or an error state. Processors only load records when it is `success`.
- For **jobs**, `data` is a list of postings.
- For **fx**, `data` contains `base`, `api_timestamp`, and `rates`.

**S3 key layout:** `raw/<source>/YYYY/MM/DD/HHMM.json` (UTC time).

**S3 object metadata** (read by the router): `source`, `record-count`, `status`, `timestamp`.

## Repository Structure

```
.
├── lambdas/
│   ├── lambda_jobs_ingest/
│   ├── lambda_fx_ingest/
│   ├── lambda_router/
│   │   └── lambda_function.py
│   └── lambda_fx_processor/
│       └── lambda_function.py
├── glue/
│   └── jobs_to_snowflake.py
├── sql/
│   ├── sqlserver_fx_rates.sql
│   └── snowflake_job_postings.sql
├── iam/
│   └── policies/          # router, fx-processor, glue inline policies
└── README.md
```

## Prerequisites

- AWS account and AWS CLI configured (`aws configure`)
- All resources created in **one single region**
- Snowflake account (account identifier, user, password)
- USAJOBS API key and registered email
- Open Exchange Rates App ID
- A SQL client (Azure Data Studio, SSMS, or DBeaver)
- Python 3.12 and PowerShell (for packaging the FX processor)

## Setup Guide

Replace `REGION`, `ACCOUNT_ID`, and `<bucket>` with your own values.

### 1. Ingestion Lambdas and S3

1. Create the S3 bucket `<bucket>`.
2. Deploy `lambda_jobs_ingest` and `lambda_fx_ingest` with environment variables for API credentials (`USAJOBS_EMAIL`, `USAJOBS_KEY`, and the Open Exchange Rates key).
3. Attach an IAM role (`hcm-lambda-ingest-role`) with `s3:PutObject` on the bucket.
4. Run both with the Test button and confirm files appear under `raw/jobs/` and `raw/fx/`.

### 2. EventBridge rules (ingestion)

| Rule | Schedule | Target |
|---|---|---|
| `jobs-every-minute` | `rate(1 minutes)` | `lambda_jobs_ingest` |
| `fx-every-minute` | `rate(1 minutes)` | `lambda_fx_ingest` |

After verifying 3-4 files, **disable `fx-every-minute`** to protect the API quota.

### 3. SNS standard topic and S3 event

1. Create a **Standard** topic `raw-events`.
2. Edit its access policy to allow `s3.amazonaws.com` to `SNS:Publish`, restricted by `aws:SourceArn` (your bucket) and `aws:SourceAccount`.
3. In S3 Properties, create the event notification `raw-object-created` with prefix `raw/`, event type `s3:ObjectCreated:*`, destination SNS topic `raw-events`.

### 4. SNS FIFO topic and SQS FIFO queues

1. Create FIFO topic `hcm-routed-events` with content-based deduplication ON.
2. Create FIFO queues `jobs-queue` and `fx-queue` (content-based deduplication ON, visibility timeout 2 minutes).
3. Subscribe each queue to the FIFO topic with:
   - Raw message delivery ON
   - Filter policy scope `Message attributes`
   - `jobs-queue`: `{"source": ["jobs"]}`
   - `fx-queue`: `{"source": ["fx"]}`
4. Make sure each queue's access policy allows the SNS topic to `sqs:SendMessage`.

### 5. Router Lambda

1. Create role `hcm-router-role` with `AWSLambdaBasicExecutionRole` plus `s3:GetObject` on the bucket and `sns:Publish` on the FIFO topic.
2. Create `lambda_router` (Python 3.12), timeout 30 seconds, environment variable `FIFO_TOPIC_ARN`.
3. Subscribe `lambda_router` to the `raw-events` topic.

The router reads the S3 object's metadata, then republishes to the FIFO topic with:

- `MessageAttributes.source` = the source name
- `MessageGroupId` = the source name
- `MessageDeduplicationId` = the S3 key

### 6. FX processor (SQS to SQL Server)

**Create RDS SQL Server Express with the AWS CLI (PowerShell):**

```powershell
$REGION = "us-east-1"

$VPC_ID = aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query "Vpcs[0].VpcId" --output text --region $REGION
$SG_ID  = aws ec2 create-security-group --group-name hcm-sqlserver-sg --description "HCM SQL Server" --vpc-id $VPC_ID --query GroupId --output text --region $REGION
aws ec2 authorize-security-group-ingress --group-id $SG_ID --protocol tcp --port 1433 --cidr 0.0.0.0/0 --region $REGION

aws rds create-db-instance `
  --db-instance-identifier hcm-sqlserver `
  --engine sqlserver-ex `
  --license-model license-included `
  --db-instance-class db.t3.micro `
  --allocated-storage 20 `
  --storage-type gp2 `
  --master-username admin `
  --master-user-password "<STRONG_PASSWORD>" `
  --vpc-security-group-ids $SG_ID `
  --publicly-accessible `
  --backup-retention-period 0 `
  --no-multi-az `
  --region $REGION

aws rds wait db-instance-available --db-instance-identifier hcm-sqlserver --region $REGION
aws rds describe-db-instances --db-instance-identifier hcm-sqlserver --query "DBInstances[0].Endpoint.Address" --output text --region $REGION
```

**Create the database and table:**

```sql
CREATE DATABASE hcm_payroll;
GO
USE hcm_payroll;
GO
CREATE TABLE dbo.fx_rates (
    id INT IDENTITY(1,1) PRIMARY KEY,
    base_currency VARCHAR(3),
    currency VARCHAR(3),
    rate DECIMAL(18,6),
    api_timestamp DATETIME2,
    s3_key VARCHAR(300),
    inserted_at DATETIME2 DEFAULT SYSUTCDATETIME()
);
```

**Package and deploy `lambda_fx_processor`:**

```powershell
mkdir fx_package; cd fx_package; mkdir package
pip install pymssql -t package --platform manylinux2014_x86_64 --only-binary=:all: --python-version 3.12 --implementation cp
# copy lambda_function.py into package/, then:
cd package
Compress-Archive -Path * -DestinationPath ..\function.zip
```

Upload `function.zip`, attach role `hcm-fx-processor-role` (S3 read plus SQS receive/delete on `fx-queue.fifo`), set timeout to 1 minute, and add environment variables:

| Variable | Value |
|---|---|
| `QUEUE_URL` | URL of `fx-queue.fifo` |
| `DB_HOST` | RDS endpoint |
| `DB_USER` | `admin` |
| `DB_PASSWORD` | your password |
| `DB_NAME` | `hcm_payroll` |

Create the rule `fx-processor-every-5-min` (`rate(5 minutes)`) targeting `lambda_fx_processor`.

### 7. Jobs processor (SQS to Snowflake via Glue)

**Snowflake setup:**

```sql
CREATE WAREHOUSE IF NOT EXISTS HCM_WH WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;
CREATE DATABASE IF NOT EXISTS HCM_DB;
CREATE SCHEMA IF NOT EXISTS HCM_DB.JOBS;
CREATE TABLE IF NOT EXISTS HCM_DB.JOBS.JOB_POSTINGS (
    TITLE VARCHAR,
    ORGANIZATION VARCHAR,
    LOCATION VARCHAR,
    SALARY VARCHAR,
    OPEN_DATE VARCHAR,
    CLOSE_DATE VARCHAR,
    S3_KEY VARCHAR,
    LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

**Glue job `jobs_to_snowflake`** (Python Shell, IAM role `hcm-glue-role`, max concurrency 1):

| Job parameter | Value |
|---|---|
| `--additional-python-modules` | `snowflake-connector-python` |
| `--QUEUE_URL` | URL of `jobs-queue.fifo` |
| `--SF_ACCOUNT` | `orgname-accountname` |
| `--SF_USER` | Snowflake username |
| `--SF_PASSWORD` | Snowflake password |
| `--SF_WAREHOUSE` | `HCM_WH` |
| `--SF_DATABASE` | `HCM_DB` |
| `--SF_SCHEMA` | `JOBS` |

**Trigger it:** Normal EventBridge rules cannot start Glue jobs directly, so create an **EventBridge Scheduler** schedule `jobs-glue-every-5-min` (rate 5 minutes, flexible window Off) with target `Glue > StartJobRun` and input:

```json
{ "JobName": "jobs_to_snowflake" }
```

## Testing the Pipeline

1. Enable all rules and schedules.
2. Wait about 10 minutes.
3. Verify each stage:

| Check | Where |
|---|---|
| New file every minute | S3 `raw/jobs/` and `raw/fx/` |
| Messages flowing | SQS `jobs-queue.fifo` and `fx-queue.fifo` (Messages available) |
| FX rows loaded | `SELECT * FROM dbo.fx_rates;` in SQL Server |
| Job rows loaded | `SELECT * FROM HCM_DB.JOBS.JOB_POSTINGS;` in Snowflake |

## Troubleshooting

| Problem | Fix |
|---|---|
| No files in S3 | Check the rule is Enabled and view Lambda CloudWatch logs |
| Lambda timeout | Increase timeout to 1 minute |
| Jobs API returns 401 | Re-check `USAJOBS_EMAIL` and `USAJOBS_KEY` |
| S3 "Unable to validate destination" | Fix ARN or bucket name in the SNS access policy |
| Router `KeyError: 'source'` | Old files lack metadata; wait for new files |
| Queues empty | Check filter policy and the queue access policy |
| `No module named 'pymssql'` | Rebuild zip so packages are at the zip root |
| SQL connection timeout | RDS public access Yes, port 1433 open in security group |
| `No module named snowflake` | Check `--additional-python-modules` parameter |
| Glue `ConcurrentRunsExceeded` | Wait for the previous run to finish |

## Cost and Limits

- Open Exchange Rates free tier: **1,000 requests/month**. Running every minute exhausts it in about 16 hours, so keep the FX rule disabled except for testing and demos.
- RDS SQL Server Express is **not** covered by the free tier. Delete it after the demo.
- Snowflake warehouse auto-suspends after 60 seconds to save credits.

## Security Notes

This setup is for a hackathon only. For production:

- Do not open port 1433 to `0.0.0.0/0`. Use a VPC with private subnets and security group references.
- Store credentials in **AWS Secrets Manager** instead of environment variables or Glue job parameters.
- Use least-privilege IAM policies scoped to exact ARNs.
- Never commit passwords or API keys to the repository.

## Cleanup

```powershell
aws rds delete-db-instance --db-instance-identifier hcm-sqlserver --skip-final-snapshot --region $REGION
```

Also disable or delete all EventBridge rules and schedules, and empty the S3 bucket if it is no longer needed.

## Scope and Future Work

- **Implemented:** Jobs and FX pipelines end to end.
- **Skipped:** The benchmarks (BLS) source, its queue, and its processor.
- **Next steps:**
  - Add the benchmarks pipeline (the third SQS queue and processor)
  - Add Dead Letter Queues for failed messages
  - Move secrets to Secrets Manager
  - Add CloudWatch alarms and dashboards
  - Define infrastructure as code (Terraform or CloudFormation)

## Authors

Muhammad Mohsin and team, Saylani Mass IT Training, Cloud Data Engineering (Batch 05).
