from spark_jobs.steps.average_difficulty_by_ingredients_report.filter_by_ingredients_step import FilterByIngredientsStep

from unittest.mock import PropertyMock
import pytest

from spark_step_test_case import SparkStepTestCase

class FilterByIngredientsStepTest(SparkStepTestCase):

    def test_valid_one_ingredient_filter(self):
        spark_job = self.make_spark_job_mock(filter_by_ingredients='beef')
        step = FilterByIngredientsStep(spark_job)

        values = [("beef",),("beef,carrot",),("carrot,beef",),("bee",),(None,)]
        initial_df = self.spark.createDataFrame(values, 'ingredients: string')

        values = [("beef",),("beef,carrot",),("carrot,beef",)]
        expected_df = self.spark.createDataFrame(values, 'ingredients: string')

        output_df = step.run(initial_df)

        self.assert_dataframes_are_equal(expected_df, output_df)

    def test_valid_two_ingredients_filter(self):
        spark_job = self.make_spark_job_mock(filter_by_ingredients='beef,carrot')
        step = FilterByIngredientsStep(spark_job)

        values = [("beef",),("beef,carrot",),("carrot,beef",),("beer",),("carrot",),("apple",),("beer",),(None,)]
        initial_df = self.spark.createDataFrame(values, 'ingredients: string')

        values = [("beef",),("beef,carrot",),("carrot,beef",),("carrot",)]
        expected_df = self.spark.createDataFrame(values, 'ingredients: string')

        output_df = step.run(initial_df)

        self.assert_dataframes_are_equal(expected_df, output_df)

    def test_valid_no_ingredients_found_filter(self):
        spark_job = self.make_spark_job_mock(filter_by_ingredients='beef,carrot')
        step = FilterByIngredientsStep(spark_job)

        values = [("apple",),("beer",),(None,)]
        initial_df = self.spark.createDataFrame(values, 'ingredients: string')
        expected_df = self.spark.createDataFrame([], 'ingredients: string')

        output_df = step.run(initial_df)

        self.assert_dataframes_are_equal(expected_df, output_df)

    def test_invalid_ingredients_list(self):
        spark_job = self.make_spark_job_mock(filter_by_ingredients='beef#carrot')
        step = FilterByIngredientsStep(spark_job)

        initial_df = self.spark.createDataFrame([], 'ingredients: string')

        with pytest.raises(Exception) as error:
            output_df = step.run(initial_df)

        assert f"Invalid ingredients list were provided by 'filter_by_ingredients' parameter: beef#carrot" == str(error.value)
