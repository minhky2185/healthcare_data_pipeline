"""
Validation methods for pipeline
"""
import logging
import logging.config
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as func

# Load logging configuration
logging.config.fileConfig(fname="utils/logging.cfg")
logger = logging.getLogger(__name__)


class Validation:
    """
    Class includes methods to validate each step in data pipeline
    """

    def __init__(self):
        """
        Constructor for class Validation
        """
        self._logger = logger

    def count_row_df(self, data_frame: DataFrame):
        """
        Count number of rows for Spark dataframe

        :param data_frame: dataframe need to be counted
        """
        try:
            num_row = data_frame.count()
            self._logger.info("Dataframe contains %d rows.", num_row)
        except Exception as exp:
            self._logger.error(
                "Error when validating number of rows. Please check the Stack Trace. %s",
                exp,
                exc_info=True,
            )
            raise
        else:
            self._logger.info("Count number of rows validation is completed.")

    def top_row_df(self, data_frame: DataFrame, num_row: int = 10):
        """
        Print first 10 rows of dataframe to logging file

        :param data_frame: dataframe to print first 10 rows
        :num_row: number of rows to print
        """
        try:
            self._logger.info("Dataframe's first %d rows are:", num_row)
            df_pandas = data_frame.limit(num_row).toPandas()
            self._logger.info("\n\t" + df_pandas.to_string(index=False))
        except Exception as exp:
            self._logger.error(
                "Error when printing first %d rows. Please check the Stack Trace. %s",
                num_row,
                exp,
                exc_info=True,
            )
            raise
        else:
            self._logger.info("Print top rows validation is completed.")

    def print_schema_df(self, data_frame: DataFrame):
        """
        Print the schema of dataframe to pipeline log file

        :param data_frame: dataframe to print schema
        """
        try:
            for field in data_frame.schema.fields:
                self._logger.info("\t\t\t\t %s", str(field))
        except Exception as exp:
            self._logger.error(
                "Error when printing schema. Please check the Stack Trace. %s",
                exp,
                exc_info=True,
            )
            raise
        else:
            self._logger.info("Printing schema validation is completed.")
