import logging
from dataclasses import dataclass, field, asdict
import redshift_connector
from config.settings import redshift
from typing import Optional, Any

logger = logging.getLogger(__name__)

@dataclass
class CheckResult:
    check_name:str
    table: str
    status:str
    severity: str
    message: str
    value: Any = None
    threshold: Any = None

    
    def to_dict(self):
        return asdict(self)

    @property
    def _passed(self)->bool:
        return self.status == "Passed"

    @property
    def is_critical(self)->bool:
        return self.severity == 'Critical' and self.status == 'failed'

class DataQualityChecker:

    def __init__(self):
        self.conn = None
        self.result = []

    def _connect(self):
        self.conn = redshift_connector.connect(
            user= redshift.REDSHIFT_USER,
            host= redshift.REDSHIFT_HOST,
            database = redshift.REDSHIFT_DATABASE,
            password = redshift.REDSHIFT_PASSWORD,
            port= redshift.REDSHIFT_PORT
        )
        self.conn.autocommit = True
        logger.info(f"DQ checker connected to Redshift")

    def _disconnect(self):

        if self.conn:
            self.conn.close()
            self.conn = None

    def _query(self, sql: str) ->Any:

        cursor = self.conn.cursor()
        cursor.execute(sql)
        row = cursor.fetchone()
        return row[0] if row else None

    def _check_row_count(
                         self,
                         table: str,
                         min_rows: int,
                         severity: str = 'Critical'):

        count = self._query(f"select count(*) from {table}")
        passed = count >= min_rows

        return CheckResult(
            check_name= f"Row_count_min_{min_rows}",
            table= table,
            status= "Passed" if passed else "Failed",
            severity= severity,
            message= (
                f"row count {count} meets threshold {min_rows}"
                if passed else
                f"Row count {count} is below minimum {min_rows}"
            ),
            value= count,
            threshold= min_rows
        )

    def _check_null_rate(self,
                         table:str,
                         column:str,
                         max_null_pct: float,
                         severity: str = "Warning"):
        sql = f"select round(sum(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END)*100.0/nullif(count(*),0),1) AS null_pct from {table}"
        null_pct = float(self._query(sql=sql) or 0)
        passed = null_pct <= max_null_pct

        return CheckResult(
            check_name= f"Null rate for {table}:{column}",
            table= table,
            status = "Passed" if passed else "Failed",
            severity= severity,
            message=(
                f"{column} from table:{table} has null percent below thrshold {max_null_pct}"
                if passed else
                f"{column} NULL rate {null_pct}% exceeds threshold {max_null_pct}%"
            ),
            value= null_pct,
            threshold= max_null_pct
        )

    def _check_duplicate_key(self,
                             table:str,
                             key_column:str,
                             severity: str = "Critical"):
        sql = f"""
                    select count(*) from 
                    (select {key_column} from {table} group by {key_column} having count(*)>1)
                """
        dup_count = self._query(sql=sql) or 0
        passed = dup_count==0

        return CheckResult(
            check_name= f"no_duplicate_{key_column}",
            table= table,
            status= "Passed" if passed else "Failed",
            severity= severity,
            message=(
                f"{key_column} from table:{table} has no duplicates"
                if passed else
                f"{key_column} has {dup_count} duplicate keys in table:{table}"
            ),
            value= dup_count,
            threshold= 0
        )

    def _check_referential_integrity(self,
                                     fact_table:str,
                                     dim_table:str,
                                     fact_col:str,
                                     dim_col:str,
                                     severity:str = "Warning"):
        sql = f"""
                    select count(*) from {fact_table} f
                    left join {dim_table} d on f.{fact_col} = d.{dim_col}
                    where d.{dim_col} is null
                    and f.{fact_col} is not null
                """
        orphan_rows = self._query(sql=sql)
        passed = orphan_rows == 0

        return CheckResult(
            check_name= f"ref_integrity_{fact_table}_{fact_col}",
            table= fact_table,
            status= "Passed" if passed else "Failed",
            severity= severity,
            message= (
                f"Referential integrity check passed for {fact_table}:{fact_col} -> {dim_table}:{dim_col}"
                if passed else
                f"Referential integrity check failed: {orphan_rows} orphan rows found in {fact_table}:{fact_col} referencing {dim_table}:{dim_col}"
            ),
            value= orphan_rows,
            threshold= 0
        )

    def _check_date_range(self,
                          table:str,
                          date_column:str,
                          min_year:int = 2022,
                          severity: str = "Warning"):

        sql = f"""
               select count(*) from {table}
                where {date_column} is not null
                and (
                extract (year from {date_column}) < {min_year}
                or {date_column} > getdate()
                )
                """
        bad_dates = self._query(sql=sql)
        passed = bad_dates == 0

        return CheckResult(
            check_name= f"date_range_{date_column}",
            table= table,
            status= "Passed" if bad_dates else "Failed",
            severity= severity,
            message= (
                f"All {date_column} values within valid range"
                if passed else
                f"{bad_dates:,} rows have {date_column} outside valid range "
                f"({min_year} to today)"
            ),
            value= bad_dates,
            threshold= 0
        )

    def _hubspot_mysql_match_rate(self,
                                table:str
                                  ):

        sql = f"""
               select 100 * sum(case when lead_source is not null then 1 else 0 end)/
               nullif(count(*),0) as match_pct from {table}
                """
        match_pct = float(self._query(sql=sql) or 0)

        if match_pct>= 60:
            status, severity = "Passed" , "info"
            msg = f"HubSpot↔MySQL match rate {match_pct}% — healthy"
        elif match_pct >= 30:
            status, severity = "failed",  "warning"
            msg = f"HubSpot↔MySQL match rate {match_pct}% — below 60% threshold"
        else:
            status, severity = "failed",  "critical"
            msg = f"HubSpot↔MySQL match rate {match_pct}% — critically low (< 30%)"

        return CheckResult(
            check_name="hubspot_mysql_match_rate",
            table= table,
            status= status,
            severity= severity,
            message=msg,
            value= match_pct
            threshold= 60.0
        )

    def _check_conversion_rate_sanity(self, table:str = "transform.trf_applications_cleaned"):

        sql = f"""
                    select 100 * sum(case when best_coe_status = 1 then 1 else 0 end)/nullif(count(*),0) as coe_receive_pct
                    from {table}
                """
        coe_rate = float(self._query(sql=sql) or 0)
        passed = 0.0 <=coe_rate <= 80.0

        return CheckResult(
            check_name="coe_conversion_rate_sanity",
            table=table,
            status="passed" if passed else "failed",
            severity="warning",
            message=(
                f"COE conversion rate {coe_rate}% — within sane range (1%-80%)"
                if passed else
                f"COE conversion rate {coe_rate}% — outside sane range, "
                f"possible data issue"
            ),
            value=coe_rate,
            threshold="1–80%"          
        )

    def _check_recent_data_exists(
        self,
        table: str,
        date_column: str,
        max_days_old: int = 7,
        severity: str = "warning"
    ) -> CheckResult:
        """
        The most recent record must not be older than max_days_old.
        Catches cases where extraction silently stopped working.
        """
        sql = f"""
            SELECT DATEDIFF(day, MAX({date_column}), GETDATE())
            FROM {table}
            WHERE {date_column} IS NOT NULL
        """
        days_since = self._query(sql)
        if days_since is None:
            return CheckResult(
                check_name=f"recent_data_{date_column}",
                table=table,
                status="failed",
                severity=severity,
                message=f"No data found in {table}.{date_column}",
                value=None,
                threshold=max_days_old
            )

        days_since = int(days_since)
        passed     = days_since <= max_days_old

        return CheckResult(
            check_name=f"recent_data_{date_column}",
            table=table,
            status="passed" if passed else "failed",
            severity=severity,
            message=(
                f"Most recent {date_column} is {days_since} days ago — fresh"
                if passed else
                f"Most recent {date_column} is {days_since} days ago — "
                f"exceeds {max_days_old} day threshold"
            ),
            value=days_since,
            threshold=max_days_old
        )

    def run_all_checks(self):

        logger.info(f"Data quality check started...")

        self.result = []

        checks = [
            # ── Staging: MySQL ───────────────────────────────────────────────
            lambda:self._check_row_count(
                "staging.stg_hubspot_contacts", min_rows=1, severity="critical"
            ),
            lambda:self._check_null_rate(
                table="staging.stg_hubspot_contacts", column="email",
                severity="Warning", max_null_pct=5.0
            ),
            # ── Staging: MySQL ───────────────────────────────────────────────
            lambda: self._check_row_count(
                "staging.stg_mysql_applications", min_rows=1, severity="critical"
            ),
            lambda: self._check_null_rate(
                "staging.stg_mysql_applications", "app_id",
                max_null_pct=0.0, severity="critical"
            ),

            # ── Transform: Entity Alignment ──────────────────────────────────
            lambda: self._check_row_count(
                "transform.trf_entity_aligned", min_rows=1, severity="critical"
            ),
            lambda: self._check_duplicate_key(
                "transform.trf_entity_aligned", "app_id", severity="critical"
            ),
            lambda: self._check_hubspot_mysql_match_rate(),
            lambda: self._check_conversion_rate_sanity(),
            lambda: self._check_date_range(
                "transform.trf_entity_aligned", "app_date", severity="warning"
            ),

            # ── Reporting: Facts ─────────────────────────────────────────────
            lambda: self._check_row_count(
                "reporting.fact_applications", min_rows=1, severity="critical"
            ),
            lambda: self._check_duplicate_key(
                "reporting.fact_applications", "app_id", severity="critical"
            ),
            lambda: self._check_row_count(
                "reporting.fact_leads", min_rows=1, severity="critical"
            ),
            lambda: self._check_row_count(
                "reporting.fact_funnel", min_rows=1, severity="critical"
            ),

            # ── Referential Integrity ────────────────────────────────────────
            lambda: self._check_referential_integrity(
                "reporting.fact_applications", "app_date_key",
                "reporting.dim_date", "date_key",
                severity="warning"
            ),

            # ── Freshness ────────────────────────────────────────────────────
            lambda: self._check_recent_data_exists(
                "transform.trf_applications_cleaned", "app_date",
                max_days_old=30, severity="warning"
            ),

        ]

        for checks_fn in checks:

            try:
                result = checks_fn()
                self.result.append(result)
                logger.info(f"{result.check_name}--{result.message}")

            except Exception as e:
                logger.info(f"check failed to execute: {e}", exc_info= True)
                self.result.append(CheckResult(
                    check_name="Check execution error",
                    table="Unknown",
                    status= "failed",
                    severity= "Critical",
                    message=f"Check could not be executed: {e}"
                ))

        return self._build_summary()

    def _build_summary(self):

        total = len(self.result)
        passed = sum(1 for r in self.result if r._passed)
        failed = total - passed
        critical = sum(1 for r in self.result if r._critical)
        warnings = sum(1 for r in self.results if
                       r.severity == "warning" and not r.passed)

        overall = (
            "Passed" if critical == 0 and failed == 0 else
            "warning"  if critical == 0 and warnings > 0 else
            "Failed"
        )

        summary = {
            "overall_status": overall,
            "total_checks":   total,
            "passed":         passed,
            "failed":         failed,
            "critical":       critical,
            "warnings":       warnings,
            "results":        [r.to_dict() for r in self.results]
        }

        logger.info(f"DQ Summary: {passed}/{total} passed |"
                    f"critical: {critical} | warning: {warnings}"
                    f"overall: {overall}")

        return summary
        