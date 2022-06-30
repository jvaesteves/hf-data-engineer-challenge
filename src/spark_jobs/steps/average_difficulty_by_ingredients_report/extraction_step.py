from pyspark.sql import DataFrame

from spark_jobs.base_classes import SparkStep

class ExtractionStep(SparkStep):

    def run(self) -> DataFrame:
        execution_date = self.config.execution_datetime.strftime('%Y-%m-%d')
        path = self.config.src_path + ('/' if self.config.src_path[-1] != '/' else '')
        path += f'execution_date={execution_date}/'

        df = self.spark.read.parquet(path)

        self.ge.add_dataframe_to_validation_batches(df=df,
            datasource='sample_data', 
            expectations='recipes_parquet'
        )

        return df
