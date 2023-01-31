"""
Run healthcare project pipeline.
"""
import configparser
import logging
import logging.config
from common.s3 import S3Connector
from common.database import DatabaseConnector
from helpers.create_spark_object import create_spark_object
from helpers.ingest_data import ingest_data
from helpers.preprocessing_data import Preprocessing
from helpers.transform_data import Transformation
from helpers.consume_data import consume_data

# Load logging configuration
logging.config.fileConfig(fname="utils/logging.cfg")
# Load project configuration
config = configparser.ConfigParser()
config.read("utils/project.cfg")


def main():
    """
    Run healthcare project pipeline.
    """
    logging.info("Healthcare project pipeline is stating. Please wait for a while...")
    # Create spark object
    spark = create_spark_object(app_name="Healthcare_project")
    # Uncomment this config when testing in Spark local mode
    """
    spark.sparkContext._jsc.hadoopConfiguration().set(
        "fs.s3a.access.key", "IAM_ACCESS_KEY"
    )
    spark.sparkContext._jsc.hadoopConfiguration().set(
        "fs.s3a.secret.key", "IAM_SECRET_KEY"
    )
    spark.sparkContext._jsc.hadoopConfiguration().set(
        "fs.s3a.endpoint", "s3.amazonaws.com"
    )
    """
    # Init connection to data store (Postgres, S3, Mysql)
    s3_connector = S3Connector(
        spark=spark,
    )
    source_db_connector = DatabaseConnector(
        spark=spark,
        rdbms=config["SOURCE_DB"]["RDBMS"],
        host=config["SOURCE_DB"]["HOST"],
        port=config["SOURCE_DB"]["PORT"],
        db_name=config["SOURCE_DB"]["NAME"],
        user=config["SOURCE_DB"]["user"],
        password=config["SOURCE_DB"]["PASSWORD"],
    )
    read_data_db_connector = DatabaseConnector(
        spark=spark,
        rdbms=config["READ_DATA_DB"]["RDBMS"],
        host=config["READ_DATA_DB"]["HOST"],
        port=config["READ_DATA_DB"]["PORT"],
        db_name=config["READ_DATA_DB"]["NAME"],
        user=config["READ_DATA_DB"]["user"],
        password=config["READ_DATA_DB"]["PASSWORD"],
    )
    # Ingest state table into S3
    logging.info("Ingest raw data into datalake")
    ingest_data(
        source_db_connector=source_db_connector,
        s3_connector=s3_connector,
        raw_bucket=config["S3"]["RAW_BUCKET"],
        db_table=config["SOURCE_DB"]["STATE_TABLE"],
        numPartitions=20,
        partitionColumn="id",
        lowerBound="1",
        upperBound="28338",
    )
    # Ingest drug table into S3
    ingest_data(
        source_db_connector=source_db_connector,
        s3_connector=s3_connector,
        raw_bucket=config["S3"]["RAW_BUCKET"],
        db_table=config["SOURCE_DB"]["DRUG_TABLE"],
        numPartitions=20,
        partitionColumn="id",
        lowerBound="1",
        upperBound="115410",
    )
    # Ingest prescriber table into S3
    ingest_data(
        source_db_connector=source_db_connector,
        s3_connector=s3_connector,
        raw_bucket=config["S3"]["RAW_BUCKET"],
        db_table=config["SOURCE_DB"]["PRESCRIBER_TABLE"],
        numPartitions=20,
        partitionColumn="id",
        lowerBound="1",
        upperBound="1255175",
    )
    # Ingest prescriber_drug into S3
    ingest_data(
        source_db_connector=source_db_connector,
        s3_connector=s3_connector,
        raw_bucket=config["S3"]["RAW_BUCKET"],
        db_table=config["SOURCE_DB"]["PRESCRIBER_DRUG_TABLE"],
        numPartitions=20,
        partitionColumn="id",
        lowerBound="1",
        upperBound="25209729",
    )
    # Preprocess raw data into cleansed zone
    logging.info("Preprocess raw data and save into cleansed zone")
    preprocessing = Preprocessing(
        s3_connector=s3_connector,
        raw_bucket=config["S3"]["RAW_BUCKET"],
        cleansed_bucket=config["S3"]["CLEANSED_BUCKET"],
    )
    preprocessing.clean_state_data()
    preprocessing.clean_drug_data()
    preprocessing.clean_prescriber_data()
    preprocessing.clean_prescriber_drug_data()

    # Transform cleansed data into reportable data save in curated zone
    logging.info("Transform cleansed data and save into curated zone")
    transform = Transformation(
        s3_connector=s3_connector,
        cleansed_bucket=config["S3"]["CLEANSED_BUCKET"],
        curated_bucket=config["S3"]["CURATED_BUCKET"],
    )
    transform.drug_report()
    transform.prescriber_report()

    # Publish curated data to data read store for high performance reporting activities.
    logging.info("Publish curated data to read data database")
    consume_data(
        read_data_db_connector=read_data_db_connector,
        s3_connector=s3_connector,
        curated_bucket=config["S3"]["CURATED_BUCKET"],
        report=config["READ_DATA_DB"]["DRUG_REPORT"],
    )
    consume_data(
        read_data_db_connector=read_data_db_connector,
        s3_connector=s3_connector,
        curated_bucket=config["S3"]["CURATED_BUCKET"],
        report=config["READ_DATA_DB"]["PRESCRIBER_REPORT"],
    )
    logging.info("Healthcare pipeline complete successfully.")


if __name__ == "__main__":
    main()
