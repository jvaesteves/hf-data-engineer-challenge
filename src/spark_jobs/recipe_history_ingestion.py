from pyspark.sql import DataFrame
from functools import reduce

from spark_jobs.config import ApplicationSettings
from spark_jobs.steps.recipe_history_ingestion import *
from spark_jobs.base_classes import SparkJob


class RecipesHistoryIngestion(SparkJob):

    def __init__(self, config: ApplicationSettings):
        super().__init__(config, 'recipes-history-ingestion')

    def job_steps(self):
        self.log.info(f"Starting '{self.spark.sparkContext.appName}' job for execution date: {self.config.execution_datetime}")

        input_df = ExtractionStep(self).run()

        domain_rules = {
            'thepioneerwoman.com': PioneerWomanStep(self).run,
            '101cookbooks.com': CookBooks101Step(self).run
        }

        df_partitions = DomainSpliterStep(self).run(input_df, 
            url_column_name='url', 
            domains=domain_rules.keys()
        )

        df_partitions = [ 
            rule_function(df_partitions[domain]) 
            for domain, rule_function in domain_rules.items()
            if len(df_partitions[domain].take(1)) > 0
        ]

        result_df = reduce(DataFrame.unionAll, df_partitions)
        WriteDataframeOnDatalakeStep(self).run(result_df)


if __name__ == '__main__':
    config = ApplicationSettings.make_from_console_parameters(
        application_parameters=['src_path', 'dest_path']
    )
    job = RecipesHistoryIngestion(config)
    job.run()