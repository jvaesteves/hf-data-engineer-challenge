from pyspark.sql import DataFrame

from spark_jobs.base_classes import SparkStep

class WriteDataframeOnDatalakeStep(SparkStep):

    def run(self, df: DataFrame):
        execution_date = self.config.execution_datetime.strftime('%Y-%m-%d')
        path = self.config.dest_path + ('/' if self.config.dest_path[-1] != '/' else '')
        path += f'execution_date={execution_date}/'
        
        self.log.info(f'Writing output for on path: {path}')

        df.coalesce(20).write.parquet(path)

        self.ge.add_dataframe_to_validation_batches(
            df=df,
            datasource='sample_data', 
            expectations='recipes_parquet'
        )