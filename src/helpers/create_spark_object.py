"""
Create spark session object method.
"""
import logging
import logging.config
from pyspark.sql import SparkSession

# Load the logging config file
logging.config.fileConfig(fname="utils/logging.cfg")
logger = logging.getLogger(__name__)


def create_spark_object(app_name: str):
    """
    Create and return a spark session object.

    :param app_name: name of spark job on Spark UI

    returns:
        spark: spark session object with given app name
    """
    try:
        logger.info("Creating spark session object with appname %s", app_name)
        spark = SparkSession.builder.appName(app_name).getOrCreate()
    except NameError as exp:
        logger.error(
            "NameError in method - %s(). Please check the Stack Trace. %s",
            __name__,
            exp,
            exc_info=True,
        )
    except Exception as exp:
        logger.error(
            "Error in method - %s(). Please check the Stack Trace. %s",
            __name__,
            exp,
            exc_info=True,
        )
    else:
        logger.info("Created spark session instance successfully.")
    return spark
