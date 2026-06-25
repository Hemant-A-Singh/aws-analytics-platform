import os
from dotenv import load_dotenv

load_dotenv()

class AWSConfig:

    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    AWS_REGION = os.getenv("AWS_REGION")
    S3_BUCKET = os.getenv("S3_BUCKET")

class RedshiftConfig:

    REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
    REDSHIFT_PORT = int(os.getenv("REDSHIFT_PORT", 5439))
    REDSHIFT_DATABASE = os.getenv("REDSHIFT_DATABASE")
    REDSHIFT_USER = os.getenv("REDSHIFT_USER")
    REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")

class HubspotConfig:
    
    HUBSPOT_API_KEY = os.getenv("HUBSPOT_API_KEY")

class MYSQLConfig:

    MYSQL_HOST = os.getenv("MYSQL_HOST")
    MYSQL_PORT = os.getenv("MYSQL_PORT")
    MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
    MYSQL_USER = os.getenv("MYSQL_USER")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")

class PipelineConfig:

    ENV = os.getenv("PIPELINE_ENV","Development")
    LOG_LEVEL = os.getenv("LOG_LEVEL","INFO")

    
aws = AWSConfig()
redshift = RedshiftConfig()
hubspot = HubspotConfig()
mysql = MYSQLConfig()
pipeline = PipelineConfig()
