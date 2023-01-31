"""Preprocessing raw data into cleansed zone"""
import logging
import logging.config
from datetime import datetime
from pyspark.sql import functions as func
from src.common.s3 import S3Connector
from src.common.validation import Validation


# Load logging configuration
logging.config.fileConfig(fname="utils/logging.cfg")
logger = logging.getLogger(__name__)


class Preprocessing:
    """
    Class contains method for cleansing raw data
    """

    def __init__(
        self,
        s3_connector: S3Connector,
        raw_bucket: str,
        cleansed_bucket: str,
    ):
        """
        Constructor for preprocessing class.

        :param s3_connector: connector to interact with S3
        :param raw_bucket: raw bucket name
        :param cleansed_bucket: cleansed bucket name
        """
        self.s3_connector = s3_connector
        self.raw_bucket = raw_bucket
        self.cleansed_bucket = cleansed_bucket
        self._logger = logger
        self._vali = Validation()
        self.s3_raw_key = f"year={datetime.now().year}/month={datetime.now().month}/day={datetime.now().day}"

    def clean_state_data(self):
        """
        Clean raw state data, validate and save into datalake
        """
        try:
            # Read state raw data into dataframe
            self._logger.info("Reading state raw data into dataframe...")
            s3_state_key = f"state/{self.s3_raw_key}"
            state_df = self.s3_connector.read_prq_to_df(
                bucket=self.raw_bucket, key=s3_state_key
            )
            # Clean state raw data
            self._logger.info("Cleansing state raw data...")
            # Select neccessary columns
            state_df = state_df.select(
                func.col("state_id").alias("state_code"),
                "state_name",
                "city",
                "population",
            )
            # Group state information
            state_df = state_df.groupBy("state_code", "state_name").agg(
                func.sum("population").alias("total_population"),
                func.count("city").alias("num_city"),
            )
            # Validate cleansed state dataframe
            self._logger.info("Validating cleansed data...")
            self._vali.count_row_df(state_df)
            self._logger.info("Validate cleansed state dataframe top 5 rows...")
            self._vali.top_row_df(state_df, num_row=5)
            self._logger.info("Validate cleansed state dataframe schema...")
            self._vali.print_schema_df(state_df)
            # Write cleansed dataframe to S3
            self._logger.info("Saving cleansed state data into cleansed zone...")
            state_df = (
                state_df.withColumn("year", func.lit(datetime.now().year))
                .withColumn("month", func.lit(datetime.now().month))
                .withColumn("day", func.lit(datetime.now().day))
            )
            self.s3_connector.write_df_to_prq(
                bucket=self.cleansed_bucket,
                key="state",
                data_frame=state_df,
                disk_partition_col=["year", "month", "day"],
                memory_partition=20,
                write_mode="overwrite",
            )
        except Exception as exp:
            self._logger.error(
                "Error when cleansing state data. Please check the Stack Trace. %s",
                exp,
                exc_info=True,
            )
            raise
        else:
            self._logger.info("Clean raw state data successfully.")

    def clean_drug_data(self):
        """
        Clean raw drug data, validate and save into datalake
        """
        try:
            # Read drug raw data into dataframe
            self._logger.info("Reading drug raw data into dataframe...")
            s3_drug_key = f"drug/{self.s3_raw_key}"
            drug_df = self.s3_connector.read_prq_to_df(
                bucket=self.raw_bucket, key=s3_drug_key
            )
            # Clean drug raw data
            self._logger.info("Cleansing drug raw data...")
            drug_df = drug_df.select(
                func.col("brnd_name").alias("drug_brand_name"),
                func.col("gnrc_name").alias("drug"),
                func.col("antbtc_drug_flag").alias("is_antibiotic"),
            )
            drug_df = drug_df.withColumn(
                "drug_type",
                func.when(func.col("is_antibiotic") == "Y", "Antibiotic").otherwise(
                    "Non-antibiotic"
                ),
            ).drop("is_antibiotic")
            drug_df = drug_df.groupBy("drug_brand_name", "drug").agg(
                func.last("drug_type").alias("drug_type")
            )
            # Validate drug cleansed data
            self._logger.info("Validating cleansed data...")
            self._vali.count_row_df(drug_df)
            self._logger.info("Validate cleansed drug dataframe top 5 rows...")
            self._vali.top_row_df(drug_df, num_row=5)
            self._logger.info("Validate cleansed drug dataframe schema...")
            self._vali.print_schema_df(drug_df)
            # Write cleansed dataframe to S3
            self._logger.info("Saving cleansed drug data into cleansed zone...")
            drug_df = (
                drug_df.withColumn("year", func.lit(datetime.now().year))
                .withColumn("month", func.lit(datetime.now().month))
                .withColumn("day", func.lit(datetime.now().day))
            )
            self.s3_connector.write_df_to_prq(
                bucket=self.cleansed_bucket,
                key="drug",
                data_frame=drug_df,
                disk_partition_col=["year", "month", "day"],
                memory_partition=20,
                write_mode="overwrite",
            )
        except Exception as exp:
            self._logger.error(
                "Error when cleansing drug data. Please check the Stack Trace. %s",
                exp,
                exc_info=True,
            )
            raise
        else:
            self._logger.info("Clean raw drug data successfully.")

    def clean_prescriber_data(self):
        """
        Clean raw prescriber data, validate and save into datalake
        """
        try:
            # Read prescriber raw data into dataframe
            self._logger.info("Reading prescriber raw data into dataframe...")
            s3_presc_key = f"prescriber/{self.s3_raw_key}"
            presc_df = self.s3_connector.read_prq_to_df(
                bucket=self.raw_bucket, key=s3_presc_key
            )

            # Clean prescriber raw data
            self._logger.info("Cleansing prescriber raw data...")
            # Select neccessary columns
            presc_df = presc_df.select(
                func.col("prscrbr_npi").alias("presc_id"),
                func.col("prscrbr_ent_cd").alias("presc_type"),
                func.col("prscrbr_city").alias("presc_city"),
                func.col("prscrbr_state_abrvtn").alias("prscrbr_state_code"),
            )
            # Process prescriber type
            presc_df = presc_df.withColumn(
                "presc_type",
                func.when(func.col("presc_type") == "I", "Individual").otherwise(
                    "Organization"
                ),
            )
            # Validate prescriber cleansed data
            self._logger.info("Validating cleansed data...")
            self._vali.count_row_df(presc_df)
            self._logger.info("Validate cleansed prescriber dataframe top 5 rows...")
            self._vali.top_row_df(presc_df, num_row=5)
            self._logger.info("Validate cleansed prescriber dataframe schema...")
            self._vali.print_schema_df(presc_df)
            # Write cleansed dataframe to S3
            self._logger.info("Saving cleansed prescriber data into cleansed zone...")
            presc_df = (
                presc_df.withColumn("year", func.lit(datetime.now().year))
                .withColumn("month", func.lit(datetime.now().month))
                .withColumn("day", func.lit(datetime.now().day))
            )
            self.s3_connector.write_df_to_prq(
                bucket=self.cleansed_bucket,
                key="prescriber",
                data_frame=presc_df,
                disk_partition_col=["year", "month", "day"],
                memory_partition=20,
                write_mode="overwrite",
            )
        except Exception as exp:
            self._logger.error(
                "Error when cleansing prescriber data. Please check the Stack Trace. %s",
                exp,
                exc_info=True,
            )
            raise
        else:
            self._logger.info("Clean raw prescriber data successfully.")

    def clean_prescriber_drug_data(self):
        """
        Clean raw prescriber drug data, validate and save into datalake
        """
        try:
            # Read prescriber_drug raw data into dataframe
            self._logger.info("Reading prescriber_drug raw data into dataframe...")
            s3_presc_drug_key = f"prescriber_drug/{self.s3_raw_key}"
            presc_drug_df = self.s3_connector.read_prq_to_df(
                bucket=self.raw_bucket, key=s3_presc_drug_key
            )

            # Clean prescriber_drug raw data
            self._logger.info("Cleansing prescriber_drug raw data...")
            presc_drug_df = presc_drug_df.select(
                func.col("prscrbr_npi").alias("presc_id"),
                func.col("prscrbr_last_org_name").alias("presc_lname"),
                func.col("prscrbr_first_name").alias("presc_fname"),
                func.col("prscrbr_state_abrvtn").alias("presc_state_code"),
                func.col("prscrbr_type").alias("presc_specialty"),
                func.col("brnd_name").alias("drug_brand_name"),
                func.col("gnrc_name").alias("drug"),
                func.col("tot_clms").alias("total_claims"),
                func.col("tot_drug_cst").alias("total_drug_cost"),
            )
            # Check null values in prescriber_drug data
            presc_drug_df.select(
                [
                    func.count(
                        func.when(func.isnan(c) | func.col(c).isNull(), "null_value")
                    ).alias(c)
                    for c in presc_drug_df.columns
                ]
            ).show()
            # Process null values
            presc_drug_df = presc_drug_df.dropna(subset=["presc_specialty"])
            # Create fullname column
            presc_drug_df = presc_drug_df.withColumn(
                "presc_fullname", func.concat_ws(" ", "presc_fname", "presc_lname")
            ).drop("presc_fname", "presc_lname")
            # Validate prescriber_drug cleansed data
            self._logger.info("Validating cleansed data...")
            self._vali.count_row_df(presc_drug_df)
            self._logger.info(
                "Validate cleansed prescriber_drug dataframe top 5 rows..."
            )
            self._vali.top_row_df(presc_drug_df, num_row=5)
            self._logger.info("Validate cleansed prescriber_drug dataframe schema...")
            self._vali.print_schema_df(presc_drug_df)
            # Write cleansed dataframe to S3
            self._logger.info(
                "Saving cleansed prescriber drug data into cleansed zone..."
            )
            presc_drug_df = (
                presc_drug_df.withColumn("year", func.lit(datetime.now().year))
                .withColumn("month", func.lit(datetime.now().month))
                .withColumn("day", func.lit(datetime.now().day))
            )
            self.s3_connector.write_df_to_prq(
                bucket=self.cleansed_bucket,
                key="prescriber_drug",
                data_frame=presc_drug_df,
                disk_partition_col=["year", "month", "day"],
                memory_partition=20,
                write_mode="overwrite",
            )
        except Exception as exp:
            self._logger.error(
                "Error when cleansing prescriber_drug data. Please check the Stack Trace. %s",
                exp,
                exc_info=True,
            )
            raise
        else:
            self._logger.info("Clean raw prescriber_drug data successfully.")
