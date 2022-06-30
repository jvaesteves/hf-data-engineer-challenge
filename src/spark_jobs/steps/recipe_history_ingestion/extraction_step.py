from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql import DataFrame

from spark_jobs.base_classes import SparkStep

class ExtractionStep(SparkStep):

    @property
    def schema(self) -> StructType:
        return StructType([
            StructField('cookTime', StringType()),
            StructField('datePublished', StringType()),
            StructField('description', StringType()),
            StructField('image', StringType()),
            StructField('ingredients', StringType()),
            StructField('name', StringType()),
            StructField('prepTime', StringType()),
            StructField('recipeYield', StringType()),
            StructField('url', StringType()),
        ])

    def run(self) -> DataFrame:
        df = self.spark.read.json(self.config.src_path, schema=self.schema)
        df = df.withColumn('datePublished', df.datePublished.cast('date'))

        self.ge.add_dataframe_to_validation_batches(
            df=df,
            datasource='sample_data', 
            expectations='recipes_raw'
        )
        
        return df