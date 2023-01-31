"""Extract data from source database and save as-is into raw zone of datalake"""
import logging
import logging.config
from datetime import datetime
from pyspark.sql import functions as func
from src.common.s3 import S3Connector
from src.common.database import DatabaseConnector

# Load logging configuration
logging.config.fileConfig(fname="utils/logging.cfg")
logger = logging.getLogger(__name__)


def ingest_data(
    source_db_connector: DatabaseConnector,
    s3_connector: S3Connector,
    raw_bucket: str,
    db_table: str,
    **kwargs,
):
    """
    Ingest data from Postgres database to raw zone hosted on S3.
    Default store partition is the date this task is performed.

    :param source_db_connector: source database connector
    :param s3_connector: connector to interact with S3
    :param raw_bucket: raw bucket name
    :param db_table: database table name
    """
    try:
        # Do full load or incremental load
        object_num = s3_connector.check_objects_number(bucket=raw_bucket, key=db_table)
        if object_num == 0:
            logger.info("Execute full load for table %s", db_table)
            table_query = f"(SELECT * FROM {db_table}) tmp"
        elif object_num > 0:
            df = s3_connector.read_prq_to_df(bucket=raw_bucket, key=db_table)
            largest_id = df.agg(func.max("id")).collect()[0]
            logger.info(
                "Execute incremental load with current max id in datalake is %s",
                str(largest_id),
            )
            table_query = f"(SELECT * FROM {db_table} WHERE id > {largest_id}) tmp"
        # Read and prepare ingested dataframe
        ingest_df = source_db_connector.read_table_to_df(db_table=table_query, **kwargs)
        ingest_df = (
            ingest_df.withColumn("year", func.lit(datetime.now().year))
            .withColumn("month", func.lit(datetime.now().month))
            .withColumn("day", func.lit(datetime.now().day))
        )
        # Write dataframe into raw bucket on S3
        s3_connector.write_df_to_prq(
            bucket=raw_bucket,
            key=db_table,
            memory_partition=20,
            data_frame=ingest_df,
            disk_partition_col=["year", "month", "day"],
        )

    except Exception as exp:
        logger.error(
            "Error when ingesting data. Please check the Stack Trace. %s",
            exp,
            exc_info=True,
        )
        raise
    else:
        logger.info("Ingest table %s into S3 successfully.", db_table)
