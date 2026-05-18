import pathlib
import sys
from abc import ABC, abstractmethod
from argparse import ArgumentParser
from typing import Any

import yaml
from pyspark.sql import SparkSession


def get_dbutils(
    spark: SparkSession,
):
    """
    Get the DBUtils object for the given Spark session.

    Parameters
    ----------
    spark : pyspark.sql.SparkSession
        The Spark session.

    Returns
    -------
    DBUtils or None
        The DBUtils object if available, otherwise None.

    Note
    ----
    please note that this function is used in mocking by its name
    """
    try:
        from pyspark.dbutils import DBUtils

        if "dbutils" not in locals():
            utils = DBUtils(spark)
            return utils
        else:
            return locals().get("dbutils")
    except ImportError:
        return None


class Task(ABC):
    """
    This is an abstract class that provides handy interfaces to implement workloads (e.g. jobs or job tasks).
    Create a child from this class and implement the abstract launch method.
    Class provides access to the following useful objects:
    * self.spark is a SparkSession
    * self.dbutils provides access to the DBUtils
    * self.logger provides access to the Spark-compatible logger
    * self.conf provides access to the parsed configuration of the job
    """

    def __init__(self, spark=None, init_conf=None):
        """
        Initialize the Task object.

        Parameters
        ----------
        spark : pyspark.sql.SparkSession, optional
            Spark session to use. If None, a new session is created.
        init_conf : dict, optional
            Initial configuration dictionary. If None, configuration is loaded from file.
        """
        self.spark = self._prepare_spark(spark)
        self.logger = self._prepare_logger()
        self.dbutils = self.get_dbutils()
        if init_conf:
            self.conf = init_conf
        else:
            self.conf = self._provide_config()
        self._log_conf()

    @staticmethod
    def _prepare_spark(spark) -> SparkSession:
        """
        Prepare and return a SparkSession.

        Parameters
        ----------
        spark : pyspark.sql.SparkSession or None
            Existing Spark session or None.

        Returns
        -------
        pyspark.sql.SparkSession
            Spark session object.
        """
        if not spark:
            return SparkSession.builder.getOrCreate()
        else:
            return spark

    def get_dbutils(self):
        """
        Get the DBUtils object for the current Spark session.

        Returns
        -------
        DBUtils or None
            DBUtils object if available, otherwise None.
        """
        utils = get_dbutils(self.spark)

        if not utils:
            self.logger.warn("No DBUtils defined in the runtime")
        else:
            self.logger.info("DBUtils class initialized")

        return utils

    def _provide_config(self):
        """
        Provide the job configuration by reading from the --conf-file argument.

        Returns
        -------
        dict
            Configuration dictionary.
        """
        self.logger.info("Reading configuration from --conf-file job option")
        conf_file = self._get_conf_file()
        if not conf_file:
            self.logger.info(
                "No conf file was provided, setting configuration to empty dict."
                "Please override configuration in subclass init method"
            )
            return {}
        else:
            self.logger.info(f"Conf file was provided, reading configuration from {conf_file}")
            return self._read_config(conf_file)

    @staticmethod
    def _get_conf_file():
        """
        Parse command line arguments to get the configuration file path.

        Returns
        -------
        str or None
            Path to the configuration file, or None if not provided.
        """
        p = ArgumentParser()
        p.add_argument("--conf-file", required=False, type=str)
        namespace = p.parse_known_args(sys.argv[1:])[0]
        return namespace.conf_file

    @staticmethod
    def _read_config(conf_file) -> dict[str, Any]:
        """
        Read and parse the configuration file.

        Parameters
        ----------
        conf_file : str
            Path to the configuration file.

        Returns
        -------
        dict
            Parsed configuration dictionary.
        """
        config = yaml.safe_load(pathlib.Path(conf_file).read_text())
        return config

    def _prepare_logger(self):
        """
        Prepare and return a Spark-compatible logger.

        Returns
        -------
        Logger
            Logger object for the current class.
        """
        log4j_logger = self.spark._jvm.org.apache.log4j
        return log4j_logger.LogManager.getLogger(self.__class__.__name__)

    def _log_conf(self):
        """
        Log the configuration parameters using the logger.

        Returns
        -------
        None
        """
        self.logger.info("Launching job with configuration parameters:")
        for key, item in self.conf.items():
            self.logger.info("\t Parameter: %-30s with value => %-30s" % (key, item))

    @abstractmethod
    def launch(self):
        """
        Main method of the job. Must be implemented by subclasses.

        Returns
        -------
        None
        """
        pass
