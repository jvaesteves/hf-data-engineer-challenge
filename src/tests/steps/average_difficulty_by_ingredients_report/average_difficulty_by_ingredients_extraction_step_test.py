from spark_jobs.steps.average_difficulty_by_ingredients_report.extraction_step import ExtractionStep
from spark_step_test_case import SparkStepTestCase

from datetime import datetime, date
from pyspark.sql.types import StructField, StructType, LongType, StringType, DateType
import pytest, tempfile, json, os, shutil, pandas as pd

class AverageDifficultyByIngredientsExtractionStepTest(SparkStepTestCase):

    @classmethod
    def setUpClass(cls):
        cls.execution_datetime = datetime.now()
        cls.src_path = tempfile.mkdtemp()	

        cls.sample_data = [{"cookTime": 0, "datePublished": date(2013, 4, 1), "description": "Got leftover Easter eggs?    Got leftover Easter ham?    Got a hearty appetite?    Good! You've come to the right place!    I...", "image": "http://static.thepioneerwoman.com/cooking/files/2013/03/leftoversandwich.jpg", "ingredients": "12 whole Hard Boiled Eggs\n1/2 cup Mayonnaise\n3 Tablespoons Grainy Dijon Mustard\n Salt And Pepper, to taste\n Several Dashes Worcestershire Sauce\n Leftover Baked Ham, Sliced\n Kaiser Rolls Or Other Bread\n Extra Mayonnaise And Dijon, For Spreading\n Swiss Cheese Or Other Cheese Slices\n Thinly Sliced Red Onion\n Avocado Slices\n Sliced Tomatoes\n Lettuce, Spinach, Or Arugula", "name": "Easter Leftover Sandwich", "prepTime": 15, "recipeYield": "8", "url": "http://thepioneerwoman.com/cooking/2013/04/easter-leftover-sandwich/"}, {"cookTime": 10, "datePublished": date(2011, 6, 6), "description": "I finally have basil in my garden. Basil I can use. This is a huge development.     I had no basil during the winter. None. G...", "image": "http://static.thepioneerwoman.com/cooking/files/2011/06/pesto.jpg", "ingredients": "3/4 cups Fresh Basil Leaves\n1/2 cup Grated Parmesan Cheese\n3 Tablespoons Pine Nuts\n2 cloves Garlic, Peeled\n Salt And Pepper, to taste\n1/3 cup Extra Virgin Olive Oil\n1/2 cup Heavy Cream\n2 Tablespoons Butter\n1/4 cup Grated Parmesan (additional)\n12 ounces, weight Pasta (cavitappi, Fusili, Etc.)\n2 whole Tomatoes, Diced", "name": "Pasta with Pesto Cream Sauce", "prepTime": 6, "recipeYield": "8", "url": "http://thepioneerwoman.com/cooking/2011/06/pasta-with-pesto-cream-sauce/"}]

        sample_df = pd.DataFrame(cls.sample_data)
        sample_dest = os.path.join(cls.src_path, f'execution_date={cls.execution_datetime.strftime("%Y-%m-%d")}') 
        os.mkdir(sample_dest)

        sample_df.to_parquet(os.path.join(sample_dest, 'test.snappy.parquet'))


    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.src_path)

    def test_valid_extraction(self):
        spark_job = self.make_spark_job_mock(src_path=self.src_path, execution_datetime=self.execution_datetime)
        step = ExtractionStep(spark_job)

        expected_schema = StructType([
            StructField('cookTime', LongType()),
            StructField('datePublished', DateType()),
            StructField('description', StringType()),
            StructField('image', StringType()),
            StructField('ingredients', StringType()),
            StructField('name', StringType()),
            StructField('prepTime', LongType()),
            StructField('recipeYield', StringType()),
            StructField('url', StringType()),
        ])

        output_df = step.run()
        expected_df = self.spark.createDataFrame(self.sample_data, schema=expected_schema)

        self.assert_dataframes_are_equal(output_df, expected_df)
