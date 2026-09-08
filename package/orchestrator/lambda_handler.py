import json
import logging
import traceback
from datetime import datetime, timezone
import boto3
import os

from extractors.hubspot_extractor import HubspotExtractor
from extractors.mysql_extractor import MYSQLExtractor
from loaders.s3_loader import S3Loader
from transformers.red_shift_transformer import RedshiftTransformer
from config.settings import aws, pipeline

logging.basicConfig(level= getattr(logging, pipeline.LOG_LEVEL, logging.INFO),
                    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger(__name__)

SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN", "")

def lambda_handler(event:dict, context)->dict:
    """
    AWS Lambda entry point.
    Called by EventBridge on schedule, or manually for testing.

    Args:
        event:   EventBridge event payload (we don't use it — pipeline
                 is fully self-contained with state in S3)
        context: Lambda context object (used for remaining time checks)

    Returns:
        Response dict with statusCode and pipeline summary
    """

    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    started_at = datetime.now(timezone.utc).isoformat

    logger.info("="*60)
    logger.info(f"Analytics pipeline started - run_id-{run_id}")
    logger.info(f"environment: {pipeline.ENV}")
    logger.info("="*60)

    run_log = {
        "run_id": run_id,
        "environment": pipeline.ENV,
        "pipeline_status": "Failed",
        "started_at": started_at,
        "completed_at": None,
        "duration_seconds": None,
        "steps": [],
        "reconciliation": [],
        "error": None 
    }

    loader = S3Loader()

    try:

        logger.info("----------Step-1 Hubspot Extraction started-----------")

        hs_result = {
            "name":"Hubspot Extractor",
            "status": "failed",
            "records_extracted": 0,
            "s3_path": None,
            "error": None,
        }

        try:
            hs_extractor = HubspotExtractor()
            result = hs_extractor.run()
            hs_result.update({
                "status": result.get("status","failed"),
                "records_extracted": result.get("records_extracted", 0),
                "s3_path": result.get("s3_path"),
                "error": result.get("error"),
                "load_type": "Incremental"
            })
            logger.info(
                f"Hubspot:{hs_result['records_extracted']} records"
                f"Status: {hs_result['status']}"
            )

        except Exception as e:
            hs_result["error"] = str(e)
            logger.error(f"Hubspot Extractor Failed: {e}", exc_info=True)

        run_log["steps"].append(hs_result)

        logger.info("----------Step-2 Mysql Extraction started-----------")
        my_result = {
            "name":"MySQL Extractor",
            "status": "failed",
            "records_extracted": 0,
            "s3_path": None,
            "error": None,
        }

        try:
            my_extractor = MYSQLExtractor()
            result = my_extractor.run()

            my_result.update({
                "status":result.get("status","failed"),
                "records_extracted": result.get("records_extracted",0),
                "s3_path": result.get("s3_path"),
                "error": result.get("error"),
                "load_type": "Incremental"
            })

            logger.info(f"Mysql: {my_result['records_extracted']} records"
                        f"Status: {my_result['status']}")

        except Exception as e:
            my_result["error"] = str(e)
            logger.error(f"Mysql Extractor Failed: {e}", exc_info=True)

        run_log["steps"].append(my_result)

        logger.info("----------Step-3 Redshift transformation started-----------")

        both_failed = (hs_result["status"]=="failed" and my_result["status"]=="failed")

        tf_result = {
            "name": "Redshift Transformer",
            "status": "skipped",
            "error": None
        }

        if both_failed:
            logger.warning("Both extractors failed- skipping transformation")
            tf_result["error"] = "Skipped: both extractors failed"
            

        else:
            try:
                transformer = RedshiftTransformer()
                result = transformer.run(hubspot_record_count = hs_result["records_extracted"], 
                                         mysql_record_count = my_result["records_extracted"])

                tf_result.update({
                    "status": result.update("status","failed"),
                    "error":result.update("error")
                })

                run_log["reconciliation"] = result.get("reconciliation",[])
                logger.info(f"Transformer: {tf_result['status']}")

            except Exception as e:
                tf_result["error"] = str(e)
                logger.error(f"Transformer Failed: {e}", exc_info=True)

        run_log["steps"].append(tf_result)

        if tf_result["status"].lower() == "success":
            run_log["pipeline_status"] = "Success"
        elif tf_result["status"].lower() == "skipped":
            run_log["pipeline_status"] = "Failed"
        else:
            run_log["pipeline_status"] = "Failed"
            
    except Exception as e:
        run_log["error"] = traceback.format_exc()
        run_log["pipeline_status"] = "Failed"
        logger.error(f"Unexpected pipeline error: {e}", exc_info=True)

    finally:
        completed_at = datetime.now(timezone.utc).isoformat()
        started_dt = datetime.fromisoformat(started_at)
        completed_dt = datetime.fromisoformat(completed_at)
        duration = round((completed_dt - started_dt).total_seconds(),1)

        run_log["completed_at"] = completed_at
        run_log["duration_seconds"] = duration

        try:
            log_path = loader.write_run_log(run_id=run_id, log_data=run_log)
            logger.info(f"run log written to: {log_path}")
        except Exception as e:
            logger.error(f"Unable to write run log: {e}")

        try:
            subject, message = _format_notification(run_log)
            _send_notification(subject, message)

        except Exception as e:
            logger.error(f"Failed to send notification: {e}")

        logger.info("="*60)
        logger.info(
            f"Pipeline {run_log['pipeline_status'].upper()} | "
            f"run_id: {run_id} | "
            f"duration: {run_log.get('duration_seconds')}s"
        )
        logger.info("="*60)

    return {
        "status_code": 200 if run_log["pipeline_status"].lower()=="success" else 500,
        "body": json.dumps({
            "run_id":          run_id,
            "pipeline_status": run_log["pipeline_status"],
            "duration_seconds": run_log.get("duration_seconds"),
            "steps": [
                {
                    "name":   s.get("name"),
                    "status": s.get("status"),
                    "records": s.get("records_extracted", "N/A")
                }
                for s in run_log.get("steps", [])
            ]
        }, indent=2)
    }

def _format_notification(run_log: dict) -> tuple[str, str]:
    """Build SNS subject and message body from run log."""
    status  = run_log.get("pipeline_status", "UNKNOWN").upper()
    run_id  = run_log.get("run_id", "unknown")
    env     = pipeline.ENV.upper()

    subject = f"[{env}] Analytics Pipeline — {status} | {run_id}"

    lines = [
        f"Pipeline Run: {run_id}",
        f"Status:       {status}",
        f"Environment:  {env}",
        f"Started:      {run_log.get('started_at', 'N/A')}",
        f"Completed:    {run_log.get('completed_at', 'N/A')}",
        f"Duration:     {run_log.get('duration_seconds', 'N/A')}s",
        "",
        "── Extraction Results ──",
    ]

    for step in run_log.get("steps", []):
        name    = step.get("name", "?")
        status  = step.get("status", "?").upper()
        records = step.get("records_extracted", "N/A")
        error   = step.get("error")
        lines.append(f"  {name:<30} {status:<10} records={records}")
        if error:
            lines.append(f"    ERROR: {error}")

    recon = run_log.get("reconciliation", [])
    if recon:
        lines.append("")
        lines.append("── Reconciliation ──")
        for r in recon:
            match = "PASS" if r.get("match") else " MISMATCH"
            lines.append(
                f"  {r['table']:<45} {match} "
                f"(src={r['source_count']:,} rs={r['target_count']:,})"
            )

    if run_log.get("error"):
        lines.append("")
        lines.append("── Pipeline Error ──")
        lines.append(run_log["error"])

    return subject, "\n".join(lines)


def _send_notification(subject: str, message: str) -> None:
    """Send pipeline status notification via SNS → email."""
    if not SNS_TOPIC_ARN:
        logger.warning("SNS_TOPIC_ARN not set — skipping notification")
        return
    try:
        sns = boto3.client("sns", region_name=aws.REGION)
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject[:100],    # SNS subject limit
            Message=message
        )
        logger.info(f"SNS notification sent: {subject}")
    except Exception as e:
        # Never let notification failure break the pipeline
        logger.error(f"SNS notification failed: {e}")


