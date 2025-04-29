from __future__ import annotations
import os
import logging
from typing import Any, Dict, List

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

HL_STORE_ID = os.environ["HL_STORE_ID"]
HL_ROLE_ARN = os.environ["HL_ROLE_ARN"]

healthlake = boto3.client("healthlake")


def _fire_import(s3_uri: str) -> str:
    """Start a HealthLake bulk-import job and return the JobId."""
    logger.info("Starting bulk import from %s", s3_uri)
    response = healthlake.start_fhir_import_job(
        JobName=f"auto-{os.path.basename(s3_uri.rstrip('/'))}",
        InputDataConfig={"S3Uri": s3_uri},
        DatastoreId=HL_STORE_ID,
        DataAccessRoleArn=HL_ROLE_ARN,
    )
    job_id: str = response["JobId"]
    logger.info("Started import job %s", job_id)
    return job_id


def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    """AWS Lambda entry point."""
    logger.info("Received %d record(s)", len(event.get("Records", [])))
    jobs: List[str] = []

    for record in event.get("Records", []):
        key: str = record["s3"]["object"]["key"]
        if not key.startswith("incoming/fhir/"):
            logger.info("Skipping %s (prefix mismatch)", key)
            continue

        bucket: str = record["s3"]["bucket"]["name"]
        # Strip the file name → keep the folder as import root
        folder = key.rsplit("/", 1)[0] + "/"
        s3_uri = f"s3://{bucket}/{folder}"

        try:
            jobs.append(_fire_import(s3_uri))
        except ClientError as exc:
            logger.error("Import failed for %s: %s", s3_uri, exc, exc_info=True)

    return {"started_jobs": jobs}
