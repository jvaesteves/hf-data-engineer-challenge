from unittest.mock import PropertyMock, Mock
from unittest import TestCase
from pyspark.sql import SparkSession
from pandas.testing import assert_frame_equal
from collections import namedtuple
import pytest

class SparkStepTestCase(TestCase):
    
    def __init__(self, methodName):
        super().__init__(methodName)

    @property
    def spark(self):
        if not hasattr(SparkStepTestCase, '_spark'):
            SparkStepTestCase._spark = SparkSession.builder.appName('testing').getOrCreate()
            SparkStepTestCase._spark.sparkContext.setLogLevel('ERROR')

        return SparkStepTestCase._spark

    def assert_dataframes_are_equal(self, df_a, df_b):
        df_a = df_a.toPandas().sort_values(df_a.columns).reset_index(drop=True)
        df_b = df_b.toPandas().sort_values(df_b.columns).reset_index(drop=True)
        return assert_frame_equal(df_a, df_b)


    def make_spark_job_mock(self, **settings):
        config = namedtuple('ApplicationSettingsMock', settings.keys())
        spark_job = namedtuple('SparkJobMock', ['ge','spark','config'])
        return spark_job(spark=self.spark, ge=Mock(), config=config(**settings))