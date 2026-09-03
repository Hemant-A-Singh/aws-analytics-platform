import json
import logging
from datetime import datetime, timezone
from loaders.s3_loader import S3Loader
from config.settings import aws, pipeline, redshift
import os
from pathlib import Path
import redshift_connector
from typing import Optional
import boto3


logger = logging.getLogger(__name__)

REDSHIFT_S3_ROLE_ARN = os.getenv("REDSHIFT_S3_ROLE_ARN", "")

SQL_EXECUTION_ORDER = [
    ("staging", "sql/staging/stg_hubspot_contacts.sql"),
    ("staging", "sql/staging/stg_mysql_applications.sql"),

    # Transform layer
    ("transform", "sql/transforms/trf_contacts_cleaned.sql"),
    ("transform", "sql/transforms/trf_applications_cleaned.sql"),
    ("transform", "sql/transforms/trf_entity_aligned.sql"),
    ("transform", "sql/transforms/trf_lead_funnel.sql"),

    # Reporting layer — dimensions first, then facts
    ("reporting", "sql/reporting/dim_date.sql"),
    ("reporting", "sql/reporting/dim_counsellor.sql"),
    ("reporting", "sql/reporting/dim_lead_source.sql"),
    ("reporting", "sql/reporting/dim_institution.sql"),
    ("reporting", "sql/reporting/fact_leads.sql"),
    ("reporting", "sql/reporting/fact_applications.sql"),
    ("reporting", "sql/reporting/fact_funnel.sql"),
]

class RedshiftTransformer:

    def __init__(self):

        self.conn = None
        self.run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    def _connect(self):

        self.conn = redshift_connector.connect(
            host= redshift.REDSHIFT_HOST,
            user= redshift.REDSHIFT_USER,
            database= redshift.REDSHIFT_DATABASE,
            port= redshift.REDSHIFT_PORT,
            password= redshift.REDSHIFT_PASSWORD,
        )
        self.conn.autocommit = False
        logger.info(f"Connected to Redshift: {redshift.REDSHIFT_HOST}/{redshift.REDSHIFT_DATABASE}")

    def _disconnect(self):
        if self.conn:
            self.conn.close()
            self.conn = None
            logger.info("Redshift connection closed")

    def _execute_sql_file(self, filepath:str, layer:str)->None:

        sql_text = Path(filepath).read_text()

        statements = [text.strip() for text in sql_text.split(';') if text.strip()]

        cursor = self.conn.cursor()
        try:

            for i,stmt in enumerate(statements):
                logger.info(f"[{layer}] Executing the statement {i+1}/{len(statements)}: {stmt[:80]}")
                cursor.execute(stmt)

            self.conn.commit()
            logger.info(f"[{layer}] {filepath} committed Successfully")

        except Exception as e:
            self.conn.rollback()
            logger.info(f"[{layer}] {filepath} Failed - ROOLEDBACK: {e}")
            raise

    def _get_s3_latest_files(self, source:str)-> Optional[str]:

        s3 = boto3.client(
            "s3",
            aws_access_key_id = aws.AWS_ACCESS_KEY_ID,
            aws_secret_access_key = aws.AWS_SECRET_ACCESS_KEY,
            region_name = aws.AWS_REGION
        )

        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(
            Bucket = aws.S3_BUCKET,
            Prefix = f"raw/{source}"
        )

        all_objects = []

        for page in pages:
            all_objects.extend(page.get("Contents",[]))

        if not all_objects:
            logger.warning(f"No file found under: raw/{source}")
            return None

        latest = sorted(all_objects, key= lambda x: x["LastModified"], reverse= True)[0]
        logger.info(f"Latest {source} file: {latest['Key']}, {latest['LastModified']}")
        return latest['Key']

    def _copy_json_to_staging(
            self,
            s3_key:str,
            target_table:str
    )->int:


        s3_path = f"s3://{aws.S3_BUCKET}/{s3_key}"
        jsonpath_name = target_table.split('.')[-1]
        jsonpath_path = f"s3://{aws.S3_BUCKET}/config/jsonpath/{jsonpath_name}.json"

        cursor = self.conn.cursor()

        cursor.execute(f"trauncate table {target_table}")

        sql_copy = f"""
                    COPY {target_table}
                    FROM '{s3_path}'
                    IAM_ROLE '{REDSHIFT_S3_ROLE_ARN}'
                    FORMAT AS JSON '{jsonpath_path}'
                    REGION '{aws.AWS_REGION}'
                    TIMEFORMAT 'auto'
                    TRUNCATECOLUMNS
                    BLANKASNULL
                    EMPTYASNULL;
                    """

        try:
            cursor.execute(sql_copy)
            self.conn.commit()
            cursor.execute(f"select count(*) from {target_table}")
            count = cursor.fetchone()[0]
            logger.info(f"Copy operation completed: {count} rows -> {target_table}")
            return count

        except Exception as e:
            self.conn.rollback()
            logger.info(f"Copy failed for {target_table}: {e}")
            raise

    def _post_copy_cleanup(self, table:str, email_col:str="email")->None:

        cursor = self.conn.cursor()
        query = f"""update table {table}
                    set email_clean = lower(trim({email_col}))
                    where {email_col} is not NULL
                    """

        try:
            cursor.execute(query)
            self.conn.commit()
            logger.info(f"Post-COPY cleanup done for {table}")

        except Exception as e:
            self.conn.rollback()
            logger.info(f"Post Copy cleanup failed for {table}")
            raise

    def _reconcile(self, source_count:int, target_table:str)->dict:

        cursor = self.conn.cursor()
        cursor.execute(f"select count(*) from {target_table}")
        target_count = cursor.fetchone()[0]

        match = source_count == target_count
        discrepancy = abs(source_count - target_count)

        result = {
            "run_id":self.run_id,
            "target_table":target_table,
            "source_count": source_count,
            "target_count": target_count,
            "match":match,
            "discrepancy": discrepancy
        }

        if match:
            logger.info(f"Reconciliation PASSED: {target_table} — {target_count} rows match")

        else:
            logger.warning(
                f"Reconciliation Mismatched: {target_table} - "
                f"Source: {source_count}, Target: {target_count}, Discrepancy: {discrepancy}"
            )

        return result

    def run(self, hubspot_record_count:int=0, mysql_record_count:int=0)->dict:

        logger.info(f"Redshift transformer has started...")
        logger.info(f"--------------------------------------")

        result = {
            "run_id": self.run_id,
            "status": "Failed",
            "reconciliation": [],
            "Error": None
        }

        try:

            self._connect()

            logger.info(f"Step-1, Creating staging tables...")
            self._execute_sql_file("sql/staging/stg_hubspot_contacts.sql","Staging")
            self._execute_sql_file("sql/staging/stg_mysql_applications.sql","Staging")

            logger.info("Staging Tables Created Successfully")
            logger.info("Step-2, Loading S3 data into Staging tables")

            hs_key = self._get_s3_latest_files(source="hubspot")

            if hs_key:
                self._copy_json_to_staging(
                    s3_key= hs_key,
                    target_table= "staging.stg_hubspot_contacts"
                )
                self._post_copy_cleanup(table="staging.stg_hubspot_contacts")
                hs_reconciliation = self._reconcile(source_count= hubspot_record_count, target_table="staging.stg_hubspot_contacts")
                result["reconciliation"].append(hs_reconciliation)

            else:
                logger.info(f"No HS_S3 file found- skipping copy")

            my_key = self._get_s3_latest_files(source="mysql")

            if my_key:
                self._copy_json_to_staging(
                    s3_key=my_key, 
                    target_table= "staging.stg_mysql_applications"
                    )
                self._post_copy_cleanup(table="staging.stg_mysql_applications")
                my_reconciliation = self._reconcile(source_count=mysql_record_count, target_table="staging.stg_mysql_applications")
                result["reconciliation"].append(my_reconciliation)

            else:
                logger.info(f"No Mysql-s3 file found - Skipping copy")


            #--------------------------creating tranformation layer redshift---------------------------------------------------
            logger.info("Step 3: Running transform layer...")
            transform_files = ["sql/transforms/trf_contacts_cleaned.sql",
                                "sql/transforms/trf_applications_cleaned.sql",
                                "sql/transforms/trf_entity_aligned.sql",
                                "sql/transforms/trf_lead_funnel.sql"]
            for filepath in transform_files:
                self._execute_sql_file(filepath=filepath,layer="transform")

            
            


            #--------------------------creating REPORTING layer redshift---------------------------------------------------
            
            logger.info("Step 4: Running reporting layer...")

            reporting_files = [
                "sql/reporting/dim_date.sql",
                "sql/reporting/dim_counsellor.sql",
                "sql/reporting/dim_lead_source.sql",
                "sql/reporting/dim_institution.sql",
                "sql/reporting/dim_office.sql"
                "sql/reporting/fact_leads.sql",
                "sql/reporting/fact_applications.sql",
                "sql/reporting/fact_funnel.sql"
            ]

            for filepath in reporting_files:
                self._execute_sql_file(filepath=filepath,layer="reporting")

            result["status"] = "Success"
            logger.info("Transformer completed successfully")

        except Exception as e:
            logger.info(f"Transformer failed, {e}", exc_info=True)
            result["Error"] = str(e)
        finally:
            self._disconnect()

        return result






