from pyspark.sql import DataFrame

from spark_jobs.steps.average_difficulty_by_ingredients_report import *
from spark_jobs.config import ApplicationSettings
from spark_jobs.base_classes import SparkJob

class AverageDifficultyByIngredientsReport(SparkJob):

    def __init__(self, config: ApplicationSettings):
        super().__init__(config, 'average-difficulty-by-ingredients-report')

    def job_steps(self):
        self.log.info(f"Starting '{self.spark.sparkContext.appName}' job for execution date: {self.config.execution_datetime}")
        
        input_df = ExtractionStep(self).run()
        filtered_df = FilterByIngredientsStep(self).run(input_df)
        report_df = CalculateByAverageDifficultyStep(self).run(filtered_df)
        WriteReportStep(self).run(report_df)

if __name__ == '__main__':
    config = ApplicationSettings.make_from_console_parameters(
        application_parameters=['src_path', 'dest_path', 'filter_by_ingredients']
    )
    job = AverageDifficultyByIngredientsReport(config)
    job.run()
