import logging
import json
from typing import Optional
import os
from functools import lru_cache
import boto3

from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

ENV = os.getenv("PIPELINE_ENV","production")
PREFIX = f"analytics/prod" if ENV == "production" else "analytics/dev"

class SecretsLoader:

    def __init__(self, region = "ap-south-1"):
        self.client = boto3.client(
            "secretsmanager",
            region_name = region
        )
        self.region = region

    def _get_secret(self, secret_name:str):

        try:
            response = self.client.get_secret_value(SecretId = secret_name)
            return json.loads(response["SecretString"])

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "ResourceNotFoundException":
                raise ValueError(
                    f"Secret '{secret_name}' not found in Secrets Manager. "
                    f"Did you create it for the '{ENV}' environment?"
                )
            raise

    @lru_cache(maxsize=None)
    def load_all(self):
        logger.info(f"Loading secrtes from secrets manager, environment: {ENV}")

        hubspot = self._get_secret(f"{PREFIX}/hubspot")
        mysql = self._get_secret(f"{PREFIX}/mysql")
        redshift = self._get_secret(f"{PREFIX}/redshift")
        config = self._get_secret(f"{PREFIX}/config")

        secrets = {
            "HUBSPOT_API_KEY": hubspot["api_token"],

            "MYSQL_HOST":             mysql["host"],
            "MYSQL_PORT":             mysql["port"],
            "MYSQL_DATABASE":         mysql["database"],
            "MYSQL_USER":             mysql["username"],
            "MYSQL_PASSWORD":         mysql["password"],

            "REDSHIFT_HOST":          redshift["host"],
            "REDSHIFT_PORT":          redshift["port"],
            "REDSHIFT_DATABASE":      redshift["database"],
            "REDSHIFT_USER":          redshift["username"],
            "REDSHIFT_PASSWORD":      redshift["password"],

            "S3_BUCKET":         config["s3_bucket"],
            "SNS_TOPIC_ARN":          config["sns_topic_arn"],
            "REDSHIFT_S3_ROLE_ARN":   config["redshift_s3_role_arn"],
            "LOG_LEVEL":              config.get("log_level", "INFO")
        } 

        logger.info(f"Secrets loaded successfully for environment: {ENV}")   
        return secrets

    def inject_into_environment(self):

        secrets = self.load_all()
        for key, value in secrets.items():
            os.environ[key] = str(value)
        logger.info(f"Secrets injected into environment variables for environment: {ENV}")

_loader: Optional[SecretsLoader] = None

def get_secrets_loader(region: str = "ap-south-1") -> SecretsLoader:
    """Return singleton SecretsLoader — created once per Lambda container."""
    global _loader
    if _loader is None:
        _loader = SecretsLoader(region=region)
    return _loader