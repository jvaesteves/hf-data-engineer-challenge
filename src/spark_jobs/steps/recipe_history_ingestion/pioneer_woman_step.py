from pyspark.sql.functions import col
from pyspark.sql import DataFrame

from .commons import cast_pt_time_to_minutes_integer
from spark_jobs.base_classes import SparkStep

class PioneerWomanStep(SparkStep):

    def run(self, df: DataFrame) -> DataFrame:
        for columnName in ['cookTime', 'prepTime']:
            df = df.withColumn(columnName, cast_pt_time_to_minutes_integer(col(columnName)))

        return df
