from spark_jobs.steps.recipe_history_ingestion.commons import cast_pt_time_to_minutes_integer

from spark_step_test_case import SparkStepTestCase


class RecipeHistoryCommonsTest(SparkStepTestCase):


    def test_valid_cast_pt_time_to_minutes_integer(self):
        spark_job = self.make_spark_job_mock()

        values = [("PT950M",), ("PT20M",), ("",), ("PT",), ("PT2H30M",), ("PT12H",)]
        initial_df = self.spark.createDataFrame(values, 'time: string')

        values = [(950,), (20,), (0,), (0,), (150,), (720,)]
        expected_df = self.spark.createDataFrame(values, 'time: int')

        output_df = initial_df.select(cast_pt_time_to_minutes_integer(initial_df.time).alias('time'))

        self.assert_dataframes_are_equal(expected_df, output_df)
