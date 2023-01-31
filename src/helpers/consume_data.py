"""
Publish report data to read data store (here using MySQL) for optimize performance consumption.
"""
import logging
import logging.config
from datetime import datetime
from src.common.database import DatabaseConnector
from src.common.s3 import S3Connector

# Load logging configuration
logging.config.fileConfig(fname="utils/logging.cfg")
logger = logging.getLogger(__name__)


def consume_data(
    read_data_db_connector: DatabaseConnector,
    s3_connector: S3Connector,
    curated_bucket: str,
    report: str,
):
    """
    Publish data from curated zone to data read store for high performance reporting activities

    :param read_data_db_connector: read data database connector
    :param s3_connector: connector to interact with S3
    :param curated_bucket: curated bucket name
    :param report: report name (also database table name)
    """
    try:
        logger.info("Publish data to table %s", report)
        # Read data from curated zone
        s3_key = f"{report}/year={datetime.now().year}/month={datetime.now().month}/day={datetime.now().day}"
        df = s3_connector.read_prq_to_df(bucket=curated_bucket, key=s3_key)
        read_data_db_connector.write_df_to_db(
            data_frame=df, db_table=report, memory_partition=20
        )
    except Exception as exp:
        logger.error(
            "Error when publishing data to data read database. Please check the Stack Trace. %s",
            exp,
            exc_info=True,
        )
        raise
    else:
        logger.info("Publishing report data to data read store successfully.")
