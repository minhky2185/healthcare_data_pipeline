"""
Connector and method accessing S3 using Spark.
"""
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
import logging
import logging.config

# Load logging configuration
logging.config.fileConfig(fname="utils/logging.cfg")
logger = logging.getLogger("common.s3")


class S3Connector:
    """
    Class for interacting with S3 using Spark.
    """

    def __init__(
        self,
        spark: SparkSession,
    ):
        """
        Contructor for class S3BucketConnector

        :param spark: spark session object
        :param access_key: access key of IAM user
        :param secret_key: secret key of IAM user
        """
        self.spark = spark
        self._logger = logger
        self._sc = spark.sparkContext

    def check_objects_number(self, bucket: str, key: str):
        """
        Check number of objects (folder and file) inside the path

        :param bucket: S3 bucket name
        :param key: location of folder

            returns:
                num_objects: object number is S3 path
        """
        s3_path = f"s3a://{bucket}/{key}"
        try:
            # Set hadoop file system for s3 bucket
            self._logger.info("Retrieving number of objects in S3 path %s ...", s3_path)
            uri_for_bucket = self._sc._jvm.java.net.URI.create(f"s3a://{bucket}")
            fs = self._sc._jvm.org.apache.hadoop.fs.FileSystem.get(
                uri_for_bucket, self._sc._jsc.hadoopConfiguration()
            )
            # Get object list for input path
            object_lists = fs.globStatus(
                self._sc._jvm.org.apache.hadoop.fs.Path(s3_path + "/*")
            )
            num_objects = len(object_lists)
        except Exception as exp:
            self._logger.error(
                "Error when retrieving number of objects. Please check the Stack Trace. %s",
                exp,
                exc_info=True,
            )
            raise
        else:
            # Close hadoop connection to S3 bucket
            fs.close()
            self._logger.info("%s objects in S3 path %s", str(num_objects), s3_path)
            return num_objects

    def read_prq_to_df(self, bucket: str, key: str, **kwargs):
        """
        Read parquet file to spark dataframe

        :param bucket: S3 bucket name
        :param key: specific location of parquet file
        """
        s3_path = f"s3://{bucket}/{key}"
        try:
            self._logger.info("Start reading %s into dataframe...", s3_path)
            data_frame = self.spark.read.options(**kwargs).parquet(s3_path)
        except Exception as exp:
            self._logger.error(
                "Error when reading Parquet from S3. Please check the Stack Trace. %s",
                exp,
                exc_info=True,
            )
            raise
        else:
            self._logger.info("Reading parquet from S3 successfully.")
            return data_frame

    def write_df_to_prq(
        self,
        bucket: str,
        key: str,
        data_frame: DataFrame,
        disk_partition_col: list,
        memory_partition: int = None,
        write_mode: str = "append",
        **kwargs,
    ):
        """
        Write dataframe to S3 under parquet file format

        :param bucket: s3 bucket name
        :param key: specific location of target file
        :param data_frame: spark dataframe need to be written to s3
        :param disk_partition_col: partition in storage
        :memory_partition: partition in memory before disk partition if needed
        :write_mode: write mode using, i.e append, overwrite, error
        """
        try:
            self._logger.info("Start writing dataframe into S3...")
            # Partition on memory before ingesting to s3 (disk)
            if memory_partition:
                current_partition_num = data_frame.rdd.getNumPartitions()
                if current_partition_num > memory_partition:
                    data_frame = data_frame.coalesce(memory_partition)
                elif current_partition_num < memory_partition:
                    data_frame = data_frame.repartition(memory_partition)
            # Write dataframe to s3
            s3_path = f"s3://{bucket}/{key}"
            if kwargs:
                data_frame.write.partitionBy(*disk_partition_col).options(
                    **kwargs
                ).mode(write_mode).parquet(s3_path)
            else:
                data_frame.write.partitionBy(*disk_partition_col).mode(
                    write_mode
                ).parquet(s3_path)
        except Exception as exp:
            self._logger.error(
                "Error when writing to S3. Please check the Stack Trace. %s",
                exp,
                exc_info=True,
            )
            raise
        else:
            self._logger.info("Write dataframe to S3 successfully.")
