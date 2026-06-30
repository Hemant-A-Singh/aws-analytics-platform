import json
import logging
from datetime import datetime, timedelta, timezone
import requests
from typing import List, Dict, Any, optional
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from config.settings import hubspot, aws, pipeline

logging.basicConfig(level= getattr(logging, pipeline.LOG_LEVEL, logging.INFO),
                    format= "%(asctime)s | %(levelname)s | %(name)s | %(message)s")

logger = logging.getLogger(__name__)

HUBSPOT_API_BASE = "https://api.hubapi.com"
END_POINTS = "/crm/v3/objects/contacts/search"
PAGE_SIZE = 100
LAST_STATE_RUN_HUBSPOT = "logs/pipeline_runs/hubspot_state.json"
MAX_RETRIES = 3

CONTACT_PROPERTIES = [
    "email",
    "firstname",
    "lastname",
    "phone",
    "office",
    "student_status",
    "counsellor",
    "country",
    "lead_source",
    "country_of_passport",
    "interested_destination"
]

class HubspotExtractor:

    def __init__(self):
        self.api_key = hubspot.API_KEY
        self.headers = {
            "Authorization": f"Bearer {self.API_KEY}"}
        
        self.s3_client = self._init_s3_client()
        self.s3_bucket = aws.S3_BUCKET
        self.run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    def _init_s3_client(self):
        import boto3

        s3 = boto3.client(
            "s3",
            aws_access_key_id=aws.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=aws.AWS_SECRET_ACCESS_KEY,
            region_name=aws.AWS_REGION_NAME
        )

    def _read_state(self):
        
        try:

            response = self.s3_client.get_object(
                bucket = self.s3_bucket,
                key = LAST_STATE_RUN_HUBSPOT
            )

            state = json.loads(response["body"].read().decode("utf-8"))
            last_extracted_at = state.get("last_extracted_at")
            logger.info(f"Last extracted at: {last_extracted_at}")
            return last_extracted_at
        
        except self.s3_client.exceptions.NoSuchKey:
            logger.info("No State file found, doing full load")
            return None

        except Exception as e:
            logger.warning(f"No state file found: {e}- Defaulting to full load")
            return None
        
    #-----------------write state--------------------------------------------------#

    def _write_state(self, records_extracted:int, status:str = "Success"):
        try:
            state = {
                "last_extracted_at" : datetime.now(timezone.utc).isoformat(),
                "last_run_status" : status,
                "records_extracted": records_extracted,
                "run_id": self.run_id
            }

            self.s3_client.put_object(
                bucket = self.s3_bucket,
                key = LAST_STATE_RUN_HUBSPOT,
                body = json.dumps(state, indent=2),
            )

            logger.info(f"State update: {records_extracted} records - {status} Status")

        except Exception as e:
            logger.info(f"Unable to connect to S3: {e}")


