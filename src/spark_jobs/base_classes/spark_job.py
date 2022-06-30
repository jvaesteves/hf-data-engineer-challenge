from pyspark.sql import SparkSession
from abc import ABC, abstractmethod

from spark_jobs.validation import GreatExpectationsValidation
from spark_jobs.config import ApplicationSettings

class SparkJob(ABC):

    def __init__(self, config: ApplicationSettings, appName: str):
        self.spark = SparkSession.builder.appName(appName).getOrCreate()
        self.ge = GreatExpectationsValidation(appName, self.spark.sparkContext.applicationId)
        self.config = config

        log4jLogger = self.spark._jvm.org.apache.log4j 
        self.log = log4jLogger.LogManager.getLogger(self.__class__.__name__)

    @abstractmethod
    def job_steps(self):
        pass

    def run(self):
        self.job_steps()
        self.ge.apply_validations()