from spark_jobs.steps.recipe_history_ingestion.write_dataframe_on_datalake_step import WriteDataframeOnDatalakeStep

from spark_step_test_case import SparkStepTestCase
from datetime import datetime, date
import pytest, tempfile, shutil, os

class WriteDataframeOnDatalakeStepTest(SparkStepTestCase):

    def setUp(self):
        self.execution_datetime = datetime.now()
        self.dest_path = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.dest_path)

    def test_valid_run(self):
        spark_job = self.make_spark_job_mock(dest_path=self.dest_path, execution_datetime=self.execution_datetime)
        step = WriteDataframeOnDatalakeStep(spark_job)

        values = [
            (None,date(2009,7,6),"From the Big Sur Bakery cookbook.","http://www.101cookbooks.com/mt-static/images/food/big_sur_bakery_hide_bread.jpg","5 cups all-purpose flour","Big Sur Bakery Hide Bread Recipe",None,None,"http://www.101cookbooks.com/archives/big-sur-bakery-hide-bread-recipe.html"),
            (30,date(2009,8,27),"An old-fashioned blueberry cake","http://www.101cookbooks.com/mt-static/images/food/blueberry_cake_recipe.jpg","1 cup plus 2 tablespoons","Old-Fashioned Blueberry Cake Recipe",10,"Serves  8 - 10.","http://www.101cookbooks.com/archives/oldfashioned-blueberry-cake-recipe.html"),
            (0,date(2013,4,1),"Got leftover Easter eggs?","http://static.thepioneerwoman.com/cooking/files/2013/03/leftoversandwich.jpg","12 whole Hard Boiled Eggs","Easter Leftover Sandwich",15,"8","http://thepioneerwoman.com/cooking/2013/04/easter-leftover-sandwich/"),
            (10,date(2011,6,6),"I finally have basil in my garden","http://static.thepioneerwoman.com/cooking/files/2011/06/pesto.jpg","3/4 cups Fresh Basil Leaves","Pasta with Pesto Cream Sauce",6,"8","http://thepioneerwoman.com/cooking/2011/06/pasta-with-pesto-cream-sauce/")
        ]

        initial_df = self.spark.createDataFrame(values, 'cookTime: int, datePublished: date, description: string, image: string, ingredients: string, name: string, prepTime: int, recipeYield: string, url: string')
        
        step.run(initial_df)
        
        output_path = os.path.join(self.dest_path, f'execution_date={self.execution_datetime.strftime("%Y-%m-%d")}')
        assert os.path.isdir(output_path)

        output_df = self.spark.read.parquet(self.dest_path)

        execution_date_column = output_df.select(output_df.execution_date).distinct().collect()
        assert len(execution_date_column) == 1
        assert execution_date_column[0][0] == self.execution_datetime.date()

        output_df = output_df.drop('execution_date')

        self.assert_dataframes_are_equal(initial_df, output_df)

