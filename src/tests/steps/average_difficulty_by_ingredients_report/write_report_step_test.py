from spark_jobs.steps.average_difficulty_by_ingredients_report.write_report_step import WriteReportStep
from spark_step_test_case import SparkStepTestCase

from datetime import datetime
import pytest, tempfile, shutil, os

class WriteReportStepTest(SparkStepTestCase):

    def setUp(self):
        self.execution_datetime = datetime.now()
        self.dest_path = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.dest_path)

    def test_valid_write(self):
        spark_job = self.make_spark_job_mock(dest_path=self.dest_path, execution_datetime=self.execution_datetime)
        step = WriteReportStep(spark_job)

        values = [("hard", 60.0), ("medium", 42.5), ("easy", 22.5)]
        input_df = self.spark.createDataFrame(values, 'difficulty: string, avg_total_cooking_time: double')

        step.run(input_df)
        output_path = os.path.join(self.dest_path, f'execution_date={self.execution_datetime.strftime("%Y-%m-%d")}')

        assert os.path.isdir(output_path)
        assert len([file for file in os.listdir(output_path) if file.endswith('.csv')]) == 1

        output_df = self.spark.read.csv(self.dest_path, header=True, schema=input_df.schema, sep=',', mode='FAILFAST')

        execution_date_column = output_df.select(output_df.execution_date).distinct().collect()
        assert len(execution_date_column) == 1
        assert execution_date_column[0][0] == self.execution_datetime.date()

        output_df = output_df.drop('execution_date')

        self.assert_dataframes_are_equal(input_df, output_df)
