"""
Access relational database with Spark through JDBC connection
"""
import logging
import logging.config
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

# Load logging configuration
logging.config.fileConfig(fname="utils/logging.cfg")
logger = logging.getLogger(__name__)


class DatabaseConnector:
    """
    Interacting with relational database using Spark
    """

    def __init__(
        self,
        spark: SparkSession,
        rdbms: str,
        host: str,
        port: int,
        db_name: str,
        user: str,
        password: str,
    ):
        """
        Constructor for DatabaseConnector class

        :param spark: spark session object
        :param rdbms: relational database management system
        :param host: where database is currently hosted on
        :param port: port to communicate with database
        :param db_name: name of database
        :param user: database user
        :param password: database password
        """
        self.spark = spark
        self._logger = logger
        self._url = (
            f"jdbc:{rdbms}://{host}:{port}/{db_name}?user={user}&password={password}"
        )

    def write_df_to_db(
        self,
        data_frame: DataFrame,
        db_table: str,
        memory_partition: int = None,
        write_mode: str = "overwrite",
        **kwargs,
    ):
        """
        Write Spark dataframe to database

        data_frame: Spark dataframe need to be saved to database
        db_table: destination table name
        :memory_partition: partition in memory before disk partition if needed
        :write_mode: write mode using, i.e append, overwrite, error
        """

        try:
            self._logger.info("Start writing dataframe into database...")
            # Partition on memory before ingesting to database
            if memory_partition:
                current_partition_num = data_frame.rdd.getNumPartitions()
                if current_partition_num > memory_partition:
                    data_frame = data_frame.coalesce(memory_partition)
                elif current_partition_num < memory_partition:
                    data_frame = data_frame.repartition(memory_partition)
            # Write dataframe to database
            data_frame.write.format("jdbc").mode(write_mode).options(
                url=self._url, dbtable=db_table, **kwargs
            ).save()
        except Exception as exp:
            self._logger.error(
                "Error when writing to database. Please check the Stack Trace. %s",
                exp,
                exc_info=True,
            )
            raise
        else:
            self._logger.info("Saving dataframe to database successfully.")

    def read_table_to_df(self, db_table: str, **kwargs):
        """
        Read database table to spark dataframe

        :param db_table: database table name or temp table

        returns:
            data_frame: spark dataframe of database table
        """
        try:
            self._logger.info("Start reading from database.")
            self._logger.info("---------------------- %s", self._url)
            data_frame = (
                self.spark.read.format("jdbc")
                .options(url=self._url, dbtable=db_table, **kwargs)
                .load()
            )
        except Exception as exp:
            self._logger.error(
                "Error when reading database table. Please check the Stack Trace. %s",
                exp,
                exc_info=True,
            )
            raise
        else:
            self._logger.info("Reading database table to dataframe successfully.")
            return data_frame
