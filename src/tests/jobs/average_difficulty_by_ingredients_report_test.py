from spark_jobs.average_difficulty_by_ingredients_report import AverageDifficultyByIngredientsReport

from pyspark.sql import SparkSession
from datetime import datetime, date
from collections import namedtuple
from unittest.mock import patch, Mock
from unittest import TestCase
import tempfile, shutil, os

class AverageDifficultyByIngredientsReportTest(TestCase):

    @classmethod
    def setUpClass(cls):
        spark = SparkSession.builder.appName('testing').getOrCreate()
        spark.sparkContext.setLogLevel('ERROR')

        config = {
            'execution_datetime': datetime.now(),
            'src_path': tempfile.mkdtemp(),
            'dest_path': tempfile.mkdtemp(),
            'filter_by_ingredients': 'beef'
        }

        with patch.object(AverageDifficultyByIngredientsReport, "__init__", lambda self, config: None):
            config = namedtuple('ApplicationSettings', config.keys())(**config)
            cls.job = AverageDifficultyByIngredientsReport(config)
            cls.job.config = config
            cls.job.spark = spark
            cls.job.log = Mock()
            cls.job.ge = Mock()

    def tearDown(self):
        shutil.rmtree(self.job.config.src_path)
        shutil.rmtree(self.job.config.dest_path)

    def test_valid_job_steps(self):
        values = [
            (90,date(2009,7,6),"From the Big Sur Bakery cookbook.","http://www.101cookbooks.com/mt-static/images/food/big_sur_bakery_hide_bread.jpg","beef","Big Sur Bakery Hide Bread Recipe",None,None,"http://www.101cookbooks.com/archives/big-sur-bakery-hide-bread-recipe.html"),
            (30,date(2009,8,27),"An old-fashioned blueberry cake","http://www.101cookbooks.com/mt-static/images/food/blueberry_cake_recipe.jpg","beef","Old-Fashioned Blueberry Cake Recipe",10,"Serves  8 - 10.","http://www.101cookbooks.com/archives/oldfashioned-blueberry-cake-recipe.html"),
            (0,date(2013,4,1),"Got leftover Easter eggs?","http://static.thepioneerwoman.com/cooking/files/2013/03/leftoversandwich.jpg","beef","Easter Leftover Sandwich",15,"8","http://thepioneerwoman.com/cooking/2013/04/easter-leftover-sandwich/"),
            (10,date(2011,6,6),"I finally have basil in my garden","http://static.thepioneerwoman.com/cooking/files/2011/06/pesto.jpg","beef","Pasta with Pesto Cream Sauce",6,"8","http://thepioneerwoman.com/cooking/2011/06/pasta-with-pesto-cream-sauce/")
        ]

        input_df = self.job.spark.createDataFrame(values, 'cookTime: int, datePublished: date, description: string, image: string, ingredients: string, name: string, prepTime: int, recipeYield: string, url: string')
        input_path = os.path.join(self.job.config.src_path, f'execution_date={self.job.config.execution_datetime.strftime("%Y-%m-%d")}')
        input_df.write.parquet(input_path, mode='overwrite')

        values = [ ('medium', "40.0"), ('hard', "90.0"), ('easy', "15.5") ]
        expected_df = self.job.spark.createDataFrame(values, 'difficulty: string, avg_total_cooking_time: string')

        self.job.job_steps()
        
        output_path = os.path.join(self.job.config.dest_path, f'execution_date={self.job.config.execution_datetime.strftime("%Y-%m-%d")}')
        output_df = self.job.spark.read.csv(output_path, header=True)

        assert expected_df.union(output_df).subtract(expected_df.intersect(output_df)).count() == 0

    def test_valid_job_steps_no_total_time(self):
        values = [
            (None,date(2009,7,6),"From the Big Sur Bakery cookbook.","http://www.101cookbooks.com/mt-static/images/food/big_sur_bakery_hide_bread.jpg","beef","Big Sur Bakery Hide Bread Recipe",None,None,"http://www.101cookbooks.com/archives/big-sur-bakery-hide-bread-recipe.html"),
            (None,date(2009,8,27),"An old-fashioned blueberry cake","http://www.101cookbooks.com/mt-static/images/food/blueberry_cake_recipe.jpg","beef","Old-Fashioned Blueberry Cake Recipe",None,"Serves  8 - 10.","http://www.101cookbooks.com/archives/oldfashioned-blueberry-cake-recipe.html"),
            (None,date(2013,4,1),"Got leftover Easter eggs?","http://static.thepioneerwoman.com/cooking/files/2013/03/leftoversandwich.jpg","beef","Easter Leftover Sandwich",None,"8","http://thepioneerwoman.com/cooking/2013/04/easter-leftover-sandwich/"),
            (None,date(2011,6,6),"I finally have basil in my garden","http://static.thepioneerwoman.com/cooking/files/2011/06/pesto.jpg","beef","Pasta with Pesto Cream Sauce",None,"8","http://thepioneerwoman.com/cooking/2011/06/pasta-with-pesto-cream-sauce/")
        ]

        input_df = self.job.spark.createDataFrame(values, 'cookTime: int, datePublished: date, description: string, image: string, ingredients: string, name: string, prepTime: int, recipeYield: string, url: string')
        input_path = os.path.join(self.job.config.src_path, f'execution_date={self.job.config.execution_datetime.strftime("%Y-%m-%d")}')
        input_df.write.parquet(input_path, mode='overwrite')

        self.job.job_steps()
        
        output_path = os.path.join(self.job.config.dest_path, f'execution_date={self.job.config.execution_datetime.strftime("%Y-%m-%d")}')
        output_df = self.job.spark.read.csv(output_path, header=True)

        assert output_df.count() == 0

    def test_valid_job_steps_one_difficulty(self):
        values = [
            (20,date(2009,7,6),"From the Big Sur Bakery cookbook.","http://www.101cookbooks.com/mt-static/images/food/big_sur_bakery_hide_bread.jpg","beef","Big Sur Bakery Hide Bread Recipe",40,None,"http://www.101cookbooks.com/archives/big-sur-bakery-hide-bread-recipe.html"),
            (10,date(2009,8,27),"An old-fashioned blueberry cake","http://www.101cookbooks.com/mt-static/images/food/blueberry_cake_recipe.jpg","beef","Old-Fashioned Blueberry Cake Recipe",85,"Serves  8 - 10.","http://www.101cookbooks.com/archives/oldfashioned-blueberry-cake-recipe.html"),
            (None,date(2013,4,1),"Got leftover Easter eggs?","http://static.thepioneerwoman.com/cooking/files/2013/03/leftoversandwich.jpg","beef","Easter Leftover Sandwich",75,"8","http://thepioneerwoman.com/cooking/2013/04/easter-leftover-sandwich/"),
            (60,date(2011,6,6),"I finally have basil in my garden","http://static.thepioneerwoman.com/cooking/files/2011/06/pesto.jpg","beef","Pasta with Pesto Cream Sauce",None,"8","http://thepioneerwoman.com/cooking/2011/06/pasta-with-pesto-cream-sauce/")
        ]

        input_df = self.job.spark.createDataFrame(values, 'cookTime: int, datePublished: date, description: string, image: string, ingredients: string, name: string, prepTime: int, recipeYield: string, url: string')
        input_path = os.path.join(self.job.config.src_path, f'execution_date={self.job.config.execution_datetime.strftime("%Y-%m-%d")}')
        input_df.write.parquet(input_path, mode='overwrite')

        values = [ ('hard', "72.5") ]
        expected_df = self.job.spark.createDataFrame(values, 'difficulty: string, avg_total_cooking_time: string')

        self.job.job_steps()
        
        output_path = os.path.join(self.job.config.dest_path, f'execution_date={self.job.config.execution_datetime.strftime("%Y-%m-%d")}')
        output_df = self.job.spark.read.csv(output_path, header=True)

        assert expected_df.union(output_df).subtract(expected_df.intersect(output_df)).count() == 0
