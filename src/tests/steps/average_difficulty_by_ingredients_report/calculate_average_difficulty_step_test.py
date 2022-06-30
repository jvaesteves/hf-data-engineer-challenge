from spark_jobs.steps.average_difficulty_by_ingredients_report.calculate_average_difficulty_step import CalculateByAverageDifficultyStep

from unittest.mock import PropertyMock
import pytest

from spark_step_test_case import SparkStepTestCase

class CalculateByAverageDifficultyStepTest(SparkStepTestCase):
    
    def test_valid_calculate_difficulty(self):
        spark_job = self.make_spark_job_mock()
        step = CalculateByAverageDifficultyStep(spark_job)

        values = [ (30,30), (10,10), (20,20), (None, None), (25, None), (None, 45) ]
        initial_df = self.spark.createDataFrame(values, 'cookTime: int, prepTime: int')

        values = [("hard", 60), ("easy", 20), ("medium", 40), ("easy", 25), ("medium", 45)]
        expected_df = self.spark.createDataFrame(values, 'difficulty: string, total_cook_time: int')

        output_df = step.calculate_difficulty(initial_df)

        self.assert_dataframes_are_equal(expected_df, output_df)

    def test_valid_get_difficulty_average(self):
        spark_job = self.make_spark_job_mock()
        step = CalculateByAverageDifficultyStep(spark_job)

        values = [("hard", 60), ("hard", 70), ("hard", 80), ("easy", 25), ("medium", 45), ("medium", 35)]
        initial_df = self.spark.createDataFrame(values, 'difficulty: string, total_cook_time: int')

        values = [("hard", 70.0), ("medium", 40.0), ("easy", 25.0)]
        expected_df = self.spark.createDataFrame(values, 'difficulty: string, avg_total_cooking_time: double')

        output_df = step.get_difficulty_average(initial_df)

        self.assert_dataframes_are_equal(expected_df, output_df)

    def test_valid_run(self):
        spark_job = self.make_spark_job_mock()
        step = CalculateByAverageDifficultyStep(spark_job)

        values = [ (30,30), (10,10), (20,20), (None, None), (25, None), (None, 45) ]
        initial_df = self.spark.createDataFrame(values, 'cookTime: int, prepTime: int')

        values = [("hard", 60.0), ("medium", 42.5), ("easy", 22.5)]
        expected_df = self.spark.createDataFrame(values, 'difficulty: string, avg_total_cooking_time: double')

        output_df = step.run(initial_df)

        self.assert_dataframes_are_equal(expected_df, output_df)
