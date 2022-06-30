from spark_jobs.steps.recipe_history_ingestion.pioneer_woman_step import PioneerWomanStep

from spark_step_test_case import SparkStepTestCase

class PioneerWomanStepTest(SparkStepTestCase):

    def test_valid_run(self):
        spark_job = self.make_spark_job_mock()
        step = PioneerWomanStep(spark_job)

        values = [("PT2H30M", "PT10M"), ("PT12M", "PT1H30M"), ("PT2H30M", "PT1H30M"), ("PT20M", "PT10M"), ("PT", "PT8H"), ("PT10M", "PT"), ("PT", "PT")]
        initial_df = self.spark.createDataFrame(values, 'cookTime: string, prepTime: string')

        values = [(150, 10), (12, 90), (150, 90), (20, 10), (0, 480), (10, 0), (0, 0)]
        expected_df = self.spark.createDataFrame(values, 'cookTime: int, prepTime: int')

        output_df = step.run(initial_df)

        self.assert_dataframes_are_equal(expected_df, output_df)