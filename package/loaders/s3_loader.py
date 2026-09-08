import logging
import json
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError
from config.settings import aws, pipeline
from typing import Optional

logger = logging.getLogger(__name__)

class S3Loader:

    RAW_FILE_PATH = "raw/{source}/year={year}/month={month:02d}/day={day:02d}/{entity}_{run_id}.json"
    STATE_FILE_PATH = "logs/pipeline_runs/{source}_state.json"
    LOG_FILE_PATH = "logs/pipeline_runs/{run_id}_run_log.json"

    def __init__(self):
        self.client = boto3.client(
            "s3",
            aws_access_key_id = aws.AWS_ACCESS_KEY_ID,
            aws_secret_access_key = aws.AWS_SECRET_ACCESS_KEY,
            region_name = aws.AWS_REGION
        )

    def raw_path(self, source:str, entity:str, run_id:str)->str:

        now = datetime.now(timezone.utc)
        return self.RAW_FILE_PATH.format(
            source = source,
            year = now.year,
            month = now.month,
            day = now.day,
            entity = entity,
            run_id = run_id
        )

    def state_path(self, source:str)->str:
        return self.STATE_FILE_PATH.format(source = source)

    def log_path(self, run_id:str):
        return self.LOG_FILE_PATH.format(run_id=run_id)

    def upload(self, key:str, body:bytes, content_type:str = "application/json", metadata: Optional[dict] = None):

        kwargs = {
            "Bucket": aws.S3_BUCKET,
            "Key": key,
            "Body": body,
            "ContentType": content_type
        }

        if metadata:
            kwargs["Metadata"] = {k: str(v) for k,v in metadata.items()}

        self.client.put_object(**kwargs)
        full_path = f"s3//{aws.S3_BUCKET}/{key}"
        logger.info(f"Uploaded records to {full_path}")
        return full_path

    def upload_raw(self, data: list[dict] ,source:str, entity: str, run_id:str, extra_metadata: Optional[dict]=None):

        key = self.raw_path(source=source, entity=entity, run_id=run_id)
        now = datetime.now(timezone.utc)

        envelop = {
            "metadata":{
                "source":source,
                "entity":entity,
                "run_id": run_id,
                "extracted_at": now,
                "record_count": len(data),
                "extra_metadata": extra_metadata
            },
            "records": data
        }

        body = json.dumps(envelop, indent=2, default = str).encode("utf-8")

        return self.upload(
            key=key,
            body=body,
            content_type= "application/json",
            metadata={
                "source":source,
                "run_id":run_id,
                "entity":entity,
                "record_count":len(data)
            }

        )

    def read_state(self, source:str)->Optional[dict]:

        key = self.state_path(source=source)

        try:
            response = self.client.get_object(Bucket = aws.S3_BUCKET, Key = key)
            state = json.loads(response["Body"].read().decode("utf-8"))
            logger.info(f"state: {state}, last_extracted_at: {state.get("last_extracted_at")}")
            return state

        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                logger.info(f"[{source}] No state file found — will run full load")
                return None
            raise

    def write_state(self, source:str, run_id:str, records_extracted:int, status:str= "Success", extra: Optional[dict]=None):

        key = self.state_path(source=source)
        payload = {
            "source": source,
            "run_id": run_id,
            "last_extracted_at": datetime.now(timezone.utc).isoformat(),
            "records_extracted":records_extracted,
            "status": status,
            "extra_metadata": extra
        }

        body = json.dumps(payload, indent=2, default=str).encode("utf-8")

        self.upload(key=key, body=body)
        logger.info(f"{source} state written in S3, records_extracted: {records_extracted}, Status: {status}")

    def write_run_log(self, run_id:str, log_data:dict)->str:

        key = self.log_path(run_id=run_id)

        log_data["run_id"] = run_id
        log_data["logged_at"] = datetime.now(timezone.utc).isoformat()

        body = json.dumps(log_data, indent=2, default=str).encode("utf-8")
        return self.upload(key=key, body=body)

    def download(self, key:str):

        response = self.client.get_object(Bucket=aws.S3_BUCKET, Key=key)
        content = json.loads(response["Body"].read().decode("utf-8"))
        return content

    def exist(self, key:str):
        try:
            self.client.head_object(Bucket = aws.S3_BUCKET, Key=key)
            return True
        except ClientError:
            return False
