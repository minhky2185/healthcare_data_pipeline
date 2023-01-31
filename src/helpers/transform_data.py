"""
Merge cleansed data to facilitate the analytic jobs.

Drug report:
    Objective:
        Used to do analytics about drug that used in Medicare Part D.
    Layout:
        drug_brand_name : The trademarked name of the drug
        drug            : A term referring to the chemical ingredient of a drug
                          Relation between drug_brand and drug is many to many
        drug_type       : Indicate whether the drug is antibiotic
        total_claims    : Total claims (of Medicare Part D) for the drug
        total_cost      : Total drug cost paid for all associated claims

Prescriber report:
    Objective:
        Used to do analytic about prescribers in Medicare Part D.
    Layout:
        presc_id        : ID (National Provider Identifier) of the prescriber
        presc_fullname  : Prescriber's full name
        presc_specialty : Prescriber's specialty
        presc_type      : Type of prescriber (Individual/Organization)
        presc_state     : The state where the prescriber is located
        total_claims    : Total number of claims the prescriber made
        drug_cost       : Total drug cost paid for all associated claims
"""
import logging
import logging.config
from datetime import datetime
from pyspark.sql import functions as func
from src.common.s3 import S3Connector
from src.common.validation import Validation


# Load logging configuration
logging.config.fileConfig(fname="utils/logging.cfg")
logger = logging.getLogger(__name__)


class Transformation:
    """
    Class contains methods for creating file supporting report process and save into curated zone.
    """

    def __init__(
        self, s3_connector: S3Connector, cleansed_bucket: str, curated_bucket: str
    ):
        """
        Constructor for transformation class

        :param s3_connector: connector to interact with S3
        :param cleansed_bucket: cleansed bucket name
        :param curated_bucket: curated bucket name
        """
        self.s3_connector = s3_connector
        self.cleansed_bucket = cleansed_bucket
        self.curated_bucket = curated_bucket
        self._logger = logger
        self._vali = Validation()
        self.s3_raw_key = f"year={datetime.now().year}/month={datetime.now().month}/day={datetime.now().day}"

    def drug_report(self):
        """
        Merge cleansed data for drug report, validate and save into curated zone
        """
        try:
            # Read cleansed drug and prescriber_drug data from cleansed zone in datalake
            self._logger.info("Reading cleansed drug and prescriber_drug data...")
            s3_presc_drug_key = f"prescriber_drug/{self.s3_raw_key}"
            s3_drug_key = f"drug/{self.s3_raw_key}"
            presc_drug_df = self.s3_connector.read_prq_to_df(
                bucket=self.cleansed_bucket, key=s3_presc_drug_key
            )
            drug_df = self.s3_connector.read_prq_to_df(
                bucket=self.cleansed_bucket, key=s3_drug_key
            )
            # Merge data into reportable form
            self._logger.info("Merging data for drug report...")
            presc_drug_df = presc_drug_df.select(
                "drug_brand_name",
                "drug",
                "total_claims",
                func.col("total_drug_cost").alias("total_cost"),
            )
            presc_drug_df = presc_drug_df.groupBy("drug_brand_name", "drug").agg(
                func.sum("total_claims").alias("total_claims"),
                func.sum("total_cost").alias("total_cost"),
            )
            # Get drug type column
            drug_df = drug_df.alias("drug")
            presc_drug_df = presc_drug_df.alias("presc_drug")
            drug_report_df = presc_drug_df.join(
                drug_df,
                (presc_drug_df["drug_brand_name"] == drug_df["drug_brand_name"])
                & (presc_drug_df["drug"] == drug_df["drug"]),
            ).select("presc_drug.*", "drug.drug_type")
            # Validate curated data
            self._logger.info("Validating drug curated data...")
            self._vali.count_row_df(drug_report_df)
            self._logger.info("Validate drug_report top 5 rows...")
            self._vali.top_row_df(drug_report_df, num_row=5)
            self._logger.info("Validate drug_report schema...")
            self._vali.print_schema_df(drug_report_df)
            # Save report data into curated zone in datalake
            self._logger.info("Saving drug report data into curated zone...")
            drug_report_df = (
                drug_report_df.withColumn("year", func.lit(datetime.now().year))
                .withColumn("month", func.lit(datetime.now().month))
                .withColumn("day", func.lit(datetime.now().day))
            )
            self.s3_connector.write_df_to_prq(
                bucket=self.curated_bucket,
                key="drug_report",
                data_frame=drug_report_df,
                disk_partition_col=["year", "month", "day"],
                memory_partition=20,
                write_mode="overwrite",
            )

        except Exception as exp:
            self._logger.error(
                "Error when merging data for drug report. Please check the Stack Trace. %s",
                exp,
                exc_info=True,
            )
            raise
        else:
            self._logger.info("Perform transformation for drug report successfully.")

    def prescriber_report(self):
        """
        Merge cleansed data for prescriber report, validate and save into curated zone
        """
        try:
            # Read cleansed state, prescriber and prescriber_drug data from cleansed zone in datalake
            self._logger.info(
                "Reading cleansed state, prescriber and prescriber_drug data..."
            )
            s3_state_key = f"state/{self.s3_raw_key}"
            s3_presc_drug_key = f"prescriber_drug/{self.s3_raw_key}"
            s3_presc_key = f"prescriber/{self.s3_raw_key}"
            state_df = self.s3_connector.read_prq_to_df(
                bucket=self.cleansed_bucket, key=s3_state_key
            )
            presc_drug_df = self.s3_connector.read_prq_to_df(
                bucket=self.cleansed_bucket, key=s3_presc_drug_key
            )
            presc_df = self.s3_connector.read_prq_to_df(
                bucket=self.cleansed_bucket, key=s3_presc_key
            )
            # Merge data into reportable form
            self._logger.info("Merging data for prescriber report...")
            presc_drug_df = presc_drug_df.select(
                "presc_id",
                "presc_fullname",
                "presc_specialty",
                "presc_state_code",
                "total_claims",
                "total_drug_cost",
            )
            state_df = state_df.alias("state")
            presc_df = presc_df.alias("presc")
            presc_drug_df = presc_drug_df.alias("presc_drug")
            # Get state name and prescriber type
            presc_report_df = (
                presc_drug_df.join(
                    state_df,
                    presc_drug_df["presc_state_code"] == state_df["state_code"],
                )
                .join(presc_df, presc_drug_df["presc_id"] == presc_df["presc_id"])
                .select("presc_drug.*", "state.state_name", "presc.presc_type")
            )
            # Validate curated data
            self._logger.info("Validating prescriber curated data...")
            self._vali.count_row_df(presc_report_df)
            self._logger.info("Validate prescriber_report top 5 rows...")
            self._vali.top_row_df(presc_report_df, num_row=5)
            self._logger.info("Validate prescriber_report schema...")
            self._vali.print_schema_df(presc_report_df)
            # Save report data into curated zone in datalake
            self._logger.info("Saving prescriber report data into curated zone...")
            presc_report_df = (
                presc_report_df.withColumn("year", func.lit(datetime.now().year))
                .withColumn("month", func.lit(datetime.now().month))
                .withColumn("day", func.lit(datetime.now().day))
            )
            self.s3_connector.write_df_to_prq(
                bucket=self.curated_bucket,
                key="prescriber_report",
                data_frame=presc_report_df,
                disk_partition_col=["year", "month", "day"],
                memory_partition=20,
                write_mode="overwrite",
            )

        except Exception as exp:
            self._logger.error(
                "Error when merging data for prescriber report. Please check the Stack Trace. %s",
                exp,
                exc_info=True,
            )
            raise
        else:
            self._logger.info(
                "Perform transformation for prescriber report successfully."
            )
