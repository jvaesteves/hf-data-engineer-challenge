from pyspark.sql.functions import when, col, regexp_extract, reverse, initcap, regexp_replace, coalesce, split
from pyspark.sql import Column, DataFrame

from .commons import cast_pt_time_to_minutes_integer
from spark_jobs.base_classes import SparkStep

class CookBooks101Step(SparkStep):

   def transform_cook_prepare_time(self, df: DataFrame) -> DataFrame:
      for columnName in ['cookTime', 'prepTime']:
         converted_time_column = cast_pt_time_to_minutes_integer(col(columnName))
         df = df.withColumn(columnName, when(col(columnName) != '', converted_time_column))

      return df

   def extract_recipe_name(self, df: DataFrame) -> Column:
      def extract_recipe_name_from_url(column: Column) -> Column:
         url_last_param = reverse(regexp_extract(reverse(column), r'(?i)/?(.+?)/', 1))
         param_without_extension_and_numbers = regexp_replace(url_last_param, r'\d+|(?:\..+)', '')
         recipe_name = initcap(regexp_replace(param_without_extension_and_numbers, r'-|_', ' '))
         return when(param_without_extension_and_numbers != '', recipe_name)

      def extract_recipe_name_from_description(column: Column) -> Column:
         description_extract = regexp_extract(column, r'101\s+Cookbooks:\s+((?:(?:\w+)|(?:\s+))+)', 1)
         return when(description_extract != '', description_extract)

      name_from_description = extract_recipe_name_from_description(df.description)
      name_from_url = extract_recipe_name_from_url(df.url)
      name_from_image = extract_recipe_name_from_url(df.image)

      return when(df.name != '', df.name).otherwise(coalesce(name_from_description,name_from_url,name_from_image))

   def extract_recipe_yield(self, column: Column) -> Column:
      return when(column != '', split(column, '\n').getItem(0))

   def run(self, df: DataFrame) -> DataFrame:
      df = self.transform_cook_prepare_time(df)
      df = df.withColumn('name', self.extract_recipe_name(df))\
             .withColumn('recipeYield', self.extract_recipe_yield(df.recipeYield))

      return df
