from pyspark.sql.functions import coalesce, lower, lit, when, avg
from pyspark.sql import DataFrame
from typing import List


from spark_jobs.base_classes import SparkStep

class CalculateByAverageDifficultyStep(SparkStep):

    def calculate_difficulty(self, df: DataFrame):
        sum_df = df.where(coalesce(df.cookTime, df.prepTime).isNotNull())\
                   .select((coalesce(df.cookTime, lit(0)) + coalesce(df.prepTime, lit(0))).alias('total_cook_time'))
                   
        difficulty_column = when(sum_df.total_cook_time < 30, 'easy')\
                                .when(sum_df.total_cook_time < 60, 'medium')\
                                .otherwise('hard')\
                                .alias('difficulty')

        difficulty_df = sum_df.select(difficulty_column, sum_df.total_cook_time)

        return difficulty_df

    def get_difficulty_average(self, df: DataFrame) -> DataFrame:
        result_df = df.groupby('difficulty').agg(avg(df.total_cook_time).alias('avg_total_cooking_time'))
        return result_df

    def run(self, df: DataFrame) -> DataFrame:
        df = self.calculate_difficulty(df)
        df = self.get_difficulty_average(df)

        return df