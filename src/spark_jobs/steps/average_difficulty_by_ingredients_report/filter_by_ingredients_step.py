from pyspark.sql.functions import split, lower
from pyspark.sql import DataFrame
from typing import List
import re


from spark_jobs.base_classes import SparkStep

class FilterByIngredientsStep(SparkStep):

    def run(self, df: DataFrame) -> DataFrame:

        if not re.match(r'^((\w+)(\||,)?)+$', self.config.filter_by_ingredients):
            raise Exception(f"Invalid ingredients list were provided by 'filter_by_ingredients' parameter: {self.config.filter_by_ingredients}")

        ingredients_list = self.config.filter_by_ingredients.replace(',', '|')

        df = df.where(lower(df.ingredients).rlike(ingredients_list))

        return df 