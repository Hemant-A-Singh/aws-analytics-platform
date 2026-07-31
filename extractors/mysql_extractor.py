import logging
import json
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
import math
import pymysql
import pymysql.cursors
import boto3
from tenacity import retry, stop_after_attempt, wait_exponential

from config.settings import aws, mysql, pipeline

logging.basicConfig(
    level= getattr(logging, pipeline.LOG_LEVEL, logging.INFO),
    format= "%(asctime)s | %(levelname)s | %(name)s | %(messages)s"
)

logger = logging.getLogger(__name__)

LAST_STATE_RUN_MYSQL = "logs/pipeline_runs/mysql_state.json"
CHUNK_SIZE = 1000

TABLE_NAME = "applications"
APP_ID_COLUMN = "app_id"
APPLICATION_DATE= "app_date"
MAX_TRIES = 3

COLUMNS = [
    "APP_ID",
    "Start_Date",
    "Finish_Date",
    "APP_Status",
    "Offer",
    "COE",
    "Student_Status",
    "To",           
    "Student_ID",
    "Full_Name",
    "Nationality",
    "Lead_Type",
    "Owner",
    "Office",
    "DOB",
    "Email",
    "Email2",
    "Tel",
    "Mobile",
    "City",
    "Country",
    "Institution",
    "Representing_Entity",
    "Level",
    "Faculty",
    "Program",
    "Uni_Student_ID",
    "App_Date",
    "Offer_Date",
    "COE_Date"
]

class MYSQLExtractor:
    
    def __init__(self):

        self.s3_client = self._init_s3_client()
        self.bucket = aws.S3_BUCKET
        self.run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        self.connection = None

    def _init_s3_client(self):
        
        s3 = boto3.client(
            "s3",
            aws_access_key_id= aws.AWS_ACCESS_KEY_ID,
            aws_secret_access_key= aws.AWS_SECRET_ACCESS_KEY,
            region_name= aws.AWS_REGION
        )
        return s3
    
    @retry(
            stop= stop_after_attempt(MAX_TRIES),
            wait= wait_exponential(multiplier=1, min=2, max=30)
            )
    def _connect(self):
        
        try:
            self.connection = pymysql.connect(
                user= mysql.MYSQL_USER,
                password= mysql.MYSQL_PASSWORD,
                port= mysql.MYSQL_PORT,
                host= mysql.MYSQL_HOST,
                database= mysql.MYSQL_DATABASE,
                cursorclass= pymysql.cursors.DictCursor,
                connect_timeout=30,
                read_timeout=300,       
                write_timeout=30
            )
            logger.info(f"Connected to MYSQL: {mysql.MYSQL_HOST}/{mysql.MYSQL_DATABASE}")

        except Exception as e:
            logger.info(f"Couldn't connect with mysql database: {e}")

    def _disconnect(self):
        
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("mysql connection closed")

    def _write_state(self, records_extracted:int, status = "Success"):
        
        state = {
            "last_extracted_at": datetime.now(timezone.utc).isoformat(),
            "last_run_status": status,
            "records_extracted": records_extracted,
            "run_id": self.run_id
        }

        self.s3_client.put_object(
            Bucket = self.bucket,
            Key = LAST_STATE_RUN_MYSQL,
            Body = json.dumps(state, indent=2),
            ContentType = "Application/json"
        )
        logger.info(f"State written - {records_extracted} records - Status: {status}")

    def _read_state(self) -> Optional[str]:
        
        try:
            response = self.s3_client.get_object(
                Bucket = self.bucket,
                Key = LAST_STATE_RUN_MYSQL
            )
            state = json.loads(response["body"].read().decode("utf-8"))
            last_extracted_at = state.get("last_extracted_at")
            logger.info(f"State found - last extracted at: {last_extracted_at}")
            return last_extracted_at

        except self.s3_client.exceptions.NoSuchKey:
            logger.info("No state file found - continuing with full load")
            return None
        except Exception as e:
            logger.warning(f"Could not read state: {e} — defaulting to full load")
            return None

    def _get_record_count(self,last_extracted_at: Optional[str]) -> int:
        
        with self.connection.cursor() as cursor:
            if last_extracted_at:
                sql = f"""
                    select count(*) as cnt from {TABLE_NAME} where {APPLICATION_DATE}>=%s
                    """
                cursor.execute(sql, (last_extracted_at[:10]))

            else:
                sql = f"""select count(*) as cnt from {TABLE_NAME}"""
                cursor.execute(sql)

            result = cursor.fetchone()
            return result["cnt"]

    def _build_query(self, last_extracted_at:Optional[str], offset:int):
        
        col_list = ",".join([f"`{col}`" for col in COLUMNS])

        if last_extracted_at:
            sql = f"""
                        select {col_list} from `{TABLE_NAME}`
                        where `{APPLICATION_DATE}`>=%s
                        order by `{APPLICATION_DATE}` ASC, `{APP_ID_COLUMN}` ASC
                        limit %s offset %s
                """
            params = (last_extracted_at, CHUNK_SIZE, offset)

        else:
            sql = f"""
                        select {col_list} from `{TABLE_NAME}`
                        order by `{APPLICATION_DATE}` ASC, ``{APP_ID_COLUMN} ASC
                        limit %s offset %s
                """
            params = (CHUNK_SIZE, offset)
        return sql , params
    
    def _serialize_row(self, row:dict):

        serialized = {}

        for key,value in row.items():
            if isinstance(value, datetime):
                serialized[key] = value.isoformat()
            elif hasattr(value, "isoformat"):
                serialized[key] = value.isoformat()
            elif value is None:
                serialized[key] = None
            else:
                serialized[key] = str(value) if not isinstance(value, (int, float, bool)) else value

        serialized["run_id"] = self.run_id
        serialized["_extracted_at"] = datetime.now(timezone.utc).isoformat()
        return serialized
        
    def _extract_all_records(self, last_extracted_at: Optional[str]):

        total_count = self._get_record_count(last_extracted_at)
        logger.info(f"Total Records Extracted: {total_count}")

        if total_count == 0:
            return []

        all_records = []

        total_chunks = math.ceil(total_count/CHUNK_SIZE)

        for chunk_num in range(total_chunks):
            offset = chunk_num * CHUNK_SIZE
            logger.info(f"Extracting Chunk: {chunk_num + 1} of {total_chunks}, offset: {offset} ")

            sql, params = self._build_query(last_extracted_at= last_extracted_at, offset= offset)

            with self.connection.cursor() as cursor:
                cursor.execute(sql, params)
                rows = cursor.fetchall()


            serialized_rows = [self._serialize_row(row) for row in rows]
            all_records.extend(serialized_rows)
            logger.info(f"Chunk {chunk_num + 1}: {len(rows)} rows fetched (total so far: {len(all_records)})")

        return all_records

    def _upload_to_s3(self, records: list[dict]):

        "Key format: raw/mysql/year=YYYY/month=MM/day=DD/applications_RUNID.json"

        now = datetime.now(timezone.utc)
        s3_key = (
            f"raw/mysql/"
            f"year={now.year}/"
            f"month={now.month:02d}/"
            f"day={now.day:02d}/"
            f"aplications_{self.run_id}.json"
        )

        payload = {
            "metadata": {
                "source": "mysql",
                "table": TABLE_NAME,
                "run_id": self.run_id,
                "extracted_at": now.isoformat(),
                "record_count": len(records)
            },
            "records": records
        }
        self.s3_client.put_object(
            Bucket = self.bucket,
            Key = s3_key,
            Body = json.loads(payload, indent = 2, default = str),
            ContentType = "Application/json",
            Metadata = {
                "source":       "mysql",
                "record_count": str(len(records)),
                "run_id":       self.run_id
            }
        )
        full_path = f"s3://{self.bucket}/{s3_key}"
        logger.info(f"Uploaded {len(records)} records to {full_path}")
        return full_path


    def run(self):

        logger.info(f"Mysql extractor has started | run_id: {self.run_id}")
        
        result = {
            "source": "Mysql",
            "run_id": self.run_id,
            "status": "Failed",
            "records_extracted": 0,
            "s3_path": None,
            "Error": None
        }

        try:

            last_extracted_at = self._read_state()
            load_type = "incremental" if last_extracted_at else "Full"
            logger.info(f"Load_type: {load_type.upper()}")

            self._connect()

            records = self._extract_all_records(last_extracted_at= last_extracted_at)
            logger.info(f"Total records extracted: {len(records)}")

            if not records:
                logger.info("No new ot updated records found")
                result["status"] = "Success"
                result["records_extracted"] = 0
                return result

            s3_path = self._upload_to_s3(records= records)

            self._write_state(records_extracted= len(records))

            result.update({
                "status": "Success",
                "records_extracted": len(records),
                "s3_path": s3_path,
                "load_type": load_type
            })

        except pymysql.Error as e:
            logger.error(f"Mysql error: {e}", exc_info= True)
            result["Error"] = str(e)
            self._write_state(records_extracted= 0 , status= "Filed")

        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info= True)
            result["Error"] = str(e)
            self._write_state(records_extracted= 0 , status= "Filed")

        finally:
            self._disconnect()

        return result

if __name__ == "__main__":

    extrtactor = MYSQLExtractor()
    result = extrtactor.run()

    for key, value in result.items():
        print(f"{key}:{value}")
