import json
import os
from unittest import mock

import handler
from botocore.stub import Stubber

# Set fake env vars for the test process
os.environ["HL_STORE_ID"] = "hl-demo-store"
os.environ["HL_ROLE_ARN"] = "arn:aws:iam::123456789012:role/HealthLakeAccessRole"


def fake_s3_event():
    """Return a minimal S3 PUT event that matches the trigger prefix."""
    return {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "clinic-input-bucket"},
                    "object": {"key": "incoming/fibr/2025-04-29/patient_0.ndjson"},
                }
            }
        ]
    }


def test_lambda_handler_starts_job():
    event = fake_s3_event()

    # Patch the HealthLake client with a Stubber so no real call is made
    with Stubber(handler.healthlake) as stub:
        stub.add_response(
            "start_fhir_import_job",
            {"JobId": "test-job-123"},
            {
                "JobName": mock.ANY,
                "InputDataConfig": {"S3Uri": "s3://clinic-input-bucket/incoming/fibr/2025-04-29/"},
                "DatastoreId": "hl-demo-store",
                "DataAccessRoleArn": "arn:aws:iam::123456789012:role/HealthLakeAccessRole",
            },
        )

        result = handler.lambda_handler(event, None)

    assert result == {"started_jobs": ["test-job-123"]}
