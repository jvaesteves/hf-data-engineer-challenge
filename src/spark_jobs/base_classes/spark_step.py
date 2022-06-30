from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from typing import Optional, Any

from .spark_job import SparkJob

class SparkStep(ABC):

    def __init__(self, current_job: SparkJob):
        self.current_job = current_job

    @property
    def spark(self):
        return self.current_job.spark

    @property
    def ge(self):
        return self.current_job.ge

    @property
    def config(self):
        return self.current_job.config

    @property
    def log(self):
        log4jLogger = self.spark._jvm.org.apache.log4j 
        return log4jLogger.LogManager.getLogger(self.__class__.__name__)

    @abstractmethod
    def run(self, df: Optional[DataFrame] = None, **kwargs) -> Any:
        pass
