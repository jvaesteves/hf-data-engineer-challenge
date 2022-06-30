from spark_jobs.recipe_history_ingestion import RecipesHistoryIngestion

from pyspark.sql import SparkSession
from datetime import datetime, date
from collections import namedtuple
from unittest.mock import patch, Mock
from unittest import TestCase
import pytest, tempfile, shutil, os

class RecipesHistoryIngestionTest(TestCase):

    @classmethod
    def setUpClass(cls):
        spark = SparkSession.builder.appName('testing').getOrCreate()
        spark.sparkContext.setLogLevel('ERROR')

        config = {
            'execution_datetime': datetime.now(),
            'src_path': tempfile.mkdtemp(),
            'dest_path': tempfile.mkdtemp()
        }

        with patch.object(RecipesHistoryIngestion, "__init__", lambda self, config: None):
            config = namedtuple('ApplicationSettings', config.keys())(**config)
            cls.job = RecipesHistoryIngestion(config)
            cls.job.config = config
            cls.job.spark = spark
            cls.job.log = Mock()
            cls.job.ge = Mock()

    def tearDown(self):
        shutil.rmtree(self.job.config.src_path)
        shutil.rmtree(self.job.config.dest_path)

    def test_valid_job_steps(self):
        values = [
            ("","2009-07-06","From the Big Sur Bakery cookbook, a seed-packed pocket bread recipe contributed by a good friend of the bakery. Sesame, sunflower, flax and poppy seeds, millet, oat bran, and a bit of beer impressively cram themselves into these delicious, hearty rolls.","http://www.101cookbooks.com/mt-static/images/food/big_sur_bakery_hide_bread.jpg","5 cups all-purpose flour, plus extra flour for dusting\n1/2 cup flax seeds\n1/2 cup sesame seeds\n2 cups oat bran\n1/4 cup sunflower seeds\n1/2 cup amaranth, quinoa, millet, or poppy seeds (or any combo of these)\n2 tablespoons dulse flakes, or 1 teaspoon kosher salt\n1 teaspoon baking soda\n1/4 cup plus 2 tablespoons beer\n2 1/2 cups buttermilk, half-and-half, milk, or water\nunsalted butter, softened for serving","Big Sur Bakery Hide Bread Recipe","","","http://www.101cookbooks.com/archives/big-sur-bakery-hide-bread-recipe.html"),
            ("PT30M","2009-08-27","An old-fashioned blueberry cake sweetened with molasses adapted from a reader submitted recipe to the July 1974 issue of Gourmet Magazine - rustic, dark as chocolate, tender, and punctuated with lots of tiny pockets of oozy, magenta berry flesh.","http://www.101cookbooks.com/mt-static/images/food/blueberry_cake_recipe.jpg","1 cup plus 2 tablespoons unbleached all-purpose flour\n1 teaspoon baking powder\n1/2 teaspoon baking soda\n3/4 teaspoon fine grain sea salt\n1/2 teaspoon cider vinegar\n5 tablespoons milk (divided)\n1/2 cup unsulphered molasses\n2 large eggs, lightly beaten\n3 tablespoons unsalted butter, barely melted\n1 1/2 cups blueberries, frozen (I freeze fresh berries)\n1 teaspoon flour\nServe with a sprinkling of powdered sugar (optional), or a big dollop of sweetened freshly whipped cream","Old-Fashioned Blueberry Cake Recipe","PT10M","Serves8 - 10.\nFoo","http://www.101cookbooks.com/archives/oldfashioned-blueberry-cake-recipe.html"),
            ("PT","2013-04-01","Got leftover Easter eggs?Got leftover Easter ham?Got a hearty appetite?Good! You've come to the right place!I...","http://static.thepioneerwoman.com/cooking/files/2013/03/leftoversandwich.jpg","12 whole Hard Boiled Eggs\n1/2 cup Mayonnaise\n3 Tablespoons Grainy Dijon Mustard\n Salt And Pepper, to taste\n Several Dashes Worcestershire Sauce\n Leftover Baked Ham, Sliced\n Kaiser Rolls Or Other Bread\n Extra Mayonnaise And Dijon, For Spreading\n Swiss Cheese Or Other Cheese Slices\n Thinly Sliced Red Onion\n Avocado Slices\n Sliced Tomatoes\n Lettuce, Spinach, Or Arugula","Easter Leftover Sandwich","PT15M","8","http://thepioneerwoman.com/cooking/2013/04/easter-leftover-sandwich/"),
            ("PT10M","2011-06-06","I finally have basil in my garden. Basil I can use. This is a huge development.I had no basil during the winter. None. G...","http://static.thepioneerwoman.com/cooking/files/2011/06/pesto.jpg","3/4 cups Fresh Basil Leaves\n1/2 cup Grated Parmesan Cheese\n3 Tablespoons Pine Nuts\n2 cloves Garlic, Peeled\n Salt And Pepper, to taste\n1/3 cup Extra Virgin Olive Oil\n1/2 cup Heavy Cream\n2 Tablespoons Butter\n1/4 cup Grated Parmesan (additional)\n12 ounces, weight Pasta (cavitappi, Fusili, Etc.)\n2 whole Tomatoes, Diced","Pasta with Pesto Cream Sauce","PT6M","8","http://thepioneerwoman.com/cooking/2011/06/pasta-with-pesto-cream-sauce/")
        ]

        input_df = self.job.spark.createDataFrame(values, 'cookTime: string, datePublished: string, description: string, image: string, ingredients: string, name: string, prepTime: string, recipeYield: string, url: string')
        input_df.write.json(self.job.config.src_path, mode='overwrite')

        values = [
            (None,date(2009,7,6),"From the Big Sur Bakery cookbook, a seed-packed pocket bread recipe contributed by a good friend of the bakery. Sesame, sunflower, flax and poppy seeds, millet, oat bran, and a bit of beer impressively cram themselves into these delicious, hearty rolls.","http://www.101cookbooks.com/mt-static/images/food/big_sur_bakery_hide_bread.jpg","5 cups all-purpose flour, plus extra flour for dusting\n1/2 cup flax seeds\n1/2 cup sesame seeds\n2 cups oat bran\n1/4 cup sunflower seeds\n1/2 cup amaranth, quinoa, millet, or poppy seeds (or any combo of these)\n2 tablespoons dulse flakes, or 1 teaspoon kosher salt\n1 teaspoon baking soda\n1/4 cup plus 2 tablespoons beer\n2 1/2 cups buttermilk, half-and-half, milk, or water\nunsalted butter, softened for serving","Big Sur Bakery Hide Bread Recipe",None,None,"http://www.101cookbooks.com/archives/big-sur-bakery-hide-bread-recipe.html"),
            (30,date(2009,8,27),"An old-fashioned blueberry cake sweetened with molasses adapted from a reader submitted recipe to the July 1974 issue of Gourmet Magazine - rustic, dark as chocolate, tender, and punctuated with lots of tiny pockets of oozy, magenta berry flesh.","http://www.101cookbooks.com/mt-static/images/food/blueberry_cake_recipe.jpg","1 cup plus 2 tablespoons unbleached all-purpose flour\n1 teaspoon baking powder\n1/2 teaspoon baking soda\n3/4 teaspoon fine grain sea salt\n1/2 teaspoon cider vinegar\n5 tablespoons milk (divided)\n1/2 cup unsulphered molasses\n2 large eggs, lightly beaten\n3 tablespoons unsalted butter, barely melted\n1 1/2 cups blueberries, frozen (I freeze fresh berries)\n1 teaspoon flour\nServe with a sprinkling of powdered sugar (optional), or a big dollop of sweetened freshly whipped cream","Old-Fashioned Blueberry Cake Recipe",10,"Serves8 - 10.","http://www.101cookbooks.com/archives/oldfashioned-blueberry-cake-recipe.html"),
            (0,date(2013,4,1),"Got leftover Easter eggs?Got leftover Easter ham?Got a hearty appetite?Good! You've come to the right place!I...","http://static.thepioneerwoman.com/cooking/files/2013/03/leftoversandwich.jpg","12 whole Hard Boiled Eggs\n1/2 cup Mayonnaise\n3 Tablespoons Grainy Dijon Mustard\n Salt And Pepper, to taste\n Several Dashes Worcestershire Sauce\n Leftover Baked Ham, Sliced\n Kaiser Rolls Or Other Bread\n Extra Mayonnaise And Dijon, For Spreading\n Swiss Cheese Or Other Cheese Slices\n Thinly Sliced Red Onion\n Avocado Slices\n Sliced Tomatoes\n Lettuce, Spinach, Or Arugula","Easter Leftover Sandwich",15,"8","http://thepioneerwoman.com/cooking/2013/04/easter-leftover-sandwich/"),
            (10,date(2011,6,6),"I finally have basil in my garden. Basil I can use. This is a huge development.I had no basil during the winter. None. G...","http://static.thepioneerwoman.com/cooking/files/2011/06/pesto.jpg","3/4 cups Fresh Basil Leaves\n1/2 cup Grated Parmesan Cheese\n3 Tablespoons Pine Nuts\n2 cloves Garlic, Peeled\n Salt And Pepper, to taste\n1/3 cup Extra Virgin Olive Oil\n1/2 cup Heavy Cream\n2 Tablespoons Butter\n1/4 cup Grated Parmesan (additional)\n12 ounces, weight Pasta (cavitappi, Fusili, Etc.)\n2 whole Tomatoes, Diced","Pasta with Pesto Cream Sauce",6,"8","http://thepioneerwoman.com/cooking/2011/06/pasta-with-pesto-cream-sauce/")
        ]
        expected_df = self.job.spark.createDataFrame(values, 'cookTime: int, datePublished: date, description: string, image: string, ingredients: string, name: string, prepTime: int, recipeYield: string, url: string')

        self.job.job_steps()
        
        output_path = os.path.join(self.job.config.dest_path, f'execution_date={self.job.config.execution_datetime.strftime("%Y-%m-%d")}')
        output_df = self.job.spark.read.parquet(output_path)

        assert expected_df.union(output_df).subtract(expected_df.intersect(output_df)).count() == 0

    def test_valid_job_steps_one_domain(self):
        values = [
            ("","2009-07-06","From the Big Sur Bakery cookbook, a seed-packed pocket bread recipe contributed by a good friend of the bakery. Sesame, sunflower, flax and poppy seeds, millet, oat bran, and a bit of beer impressively cram themselves into these delicious, hearty rolls.","http://www.101cookbooks.com/mt-static/images/food/big_sur_bakery_hide_bread.jpg","5 cups all-purpose flour, plus extra flour for dusting\n1/2 cup flax seeds\n1/2 cup sesame seeds\n2 cups oat bran\n1/4 cup sunflower seeds\n1/2 cup amaranth, quinoa, millet, or poppy seeds (or any combo of these)\n2 tablespoons dulse flakes, or 1 teaspoon kosher salt\n1 teaspoon baking soda\n1/4 cup plus 2 tablespoons beer\n2 1/2 cups buttermilk, half-and-half, milk, or water\nunsalted butter, softened for serving","Big Sur Bakery Hide Bread Recipe","","","http://www.101cookbooks.com/archives/big-sur-bakery-hide-bread-recipe.html"),
            ("PT30M","2009-08-27","An old-fashioned blueberry cake sweetened with molasses adapted from a reader submitted recipe to the July 1974 issue of Gourmet Magazine - rustic, dark as chocolate, tender, and punctuated with lots of tiny pockets of oozy, magenta berry flesh.","http://www.101cookbooks.com/mt-static/images/food/blueberry_cake_recipe.jpg","1 cup plus 2 tablespoons unbleached all-purpose flour\n1 teaspoon baking powder\n1/2 teaspoon baking soda\n3/4 teaspoon fine grain sea salt\n1/2 teaspoon cider vinegar\n5 tablespoons milk (divided)\n1/2 cup unsulphered molasses\n2 large eggs, lightly beaten\n3 tablespoons unsalted butter, barely melted\n1 1/2 cups blueberries, frozen (I freeze fresh berries)\n1 teaspoon flour\nServe with a sprinkling of powdered sugar (optional), or a big dollop of sweetened freshly whipped cream","Old-Fashioned Blueberry Cake Recipe","PT10M","Serves8 - 10.\nFoo","http://www.101cookbooks.com/archives/oldfashioned-blueberry-cake-recipe.html")
        ]

        input_df = self.job.spark.createDataFrame(values, 'cookTime: string, datePublished: string, description: string, image: string, ingredients: string, name: string, prepTime: string, recipeYield: string, url: string')
        input_df.write.json(self.job.config.src_path, mode='overwrite')

        values = [
            (None,date(2009,7,6),"From the Big Sur Bakery cookbook, a seed-packed pocket bread recipe contributed by a good friend of the bakery. Sesame, sunflower, flax and poppy seeds, millet, oat bran, and a bit of beer impressively cram themselves into these delicious, hearty rolls.","http://www.101cookbooks.com/mt-static/images/food/big_sur_bakery_hide_bread.jpg","5 cups all-purpose flour, plus extra flour for dusting\n1/2 cup flax seeds\n1/2 cup sesame seeds\n2 cups oat bran\n1/4 cup sunflower seeds\n1/2 cup amaranth, quinoa, millet, or poppy seeds (or any combo of these)\n2 tablespoons dulse flakes, or 1 teaspoon kosher salt\n1 teaspoon baking soda\n1/4 cup plus 2 tablespoons beer\n2 1/2 cups buttermilk, half-and-half, milk, or water\nunsalted butter, softened for serving","Big Sur Bakery Hide Bread Recipe",None,None,"http://www.101cookbooks.com/archives/big-sur-bakery-hide-bread-recipe.html"),
            (30,date(2009,8,27),"An old-fashioned blueberry cake sweetened with molasses adapted from a reader submitted recipe to the July 1974 issue of Gourmet Magazine - rustic, dark as chocolate, tender, and punctuated with lots of tiny pockets of oozy, magenta berry flesh.","http://www.101cookbooks.com/mt-static/images/food/blueberry_cake_recipe.jpg","1 cup plus 2 tablespoons unbleached all-purpose flour\n1 teaspoon baking powder\n1/2 teaspoon baking soda\n3/4 teaspoon fine grain sea salt\n1/2 teaspoon cider vinegar\n5 tablespoons milk (divided)\n1/2 cup unsulphered molasses\n2 large eggs, lightly beaten\n3 tablespoons unsalted butter, barely melted\n1 1/2 cups blueberries, frozen (I freeze fresh berries)\n1 teaspoon flour\nServe with a sprinkling of powdered sugar (optional), or a big dollop of sweetened freshly whipped cream","Old-Fashioned Blueberry Cake Recipe",10,"Serves8 - 10.","http://www.101cookbooks.com/archives/oldfashioned-blueberry-cake-recipe.html"),
        ]
        expected_df = self.job.spark.createDataFrame(values, 'cookTime: int, datePublished: date, description: string, image: string, ingredients: string, name: string, prepTime: int, recipeYield: string, url: string')

        self.job.job_steps()
        
        output_path = os.path.join(self.job.config.dest_path, f'execution_date={self.job.config.execution_datetime.strftime("%Y-%m-%d")}')
        output_df = self.job.spark.read.parquet(output_path)

        assert expected_df.union(output_df).subtract(expected_df.intersect(output_df)).count() == 0

    def test_invalid_job_steps_not_registered_domain(self):
        unknown_domains = ['notregistered.com']
        values = [
            ("","2009-07-06","From the Big Sur Bakery cookbook, a seed-packed pocket bread recipe contributed by a good friend of the bakery. Sesame, sunflower, flax and poppy seeds, millet, oat bran, and a bit of beer impressively cram themselves into these delicious, hearty rolls.","http://www.101cookbooks.com/mt-static/images/food/big_sur_bakery_hide_bread.jpg","5 cups all-purpose flour, plus extra flour for dusting\n1/2 cup flax seeds\n1/2 cup sesame seeds\n2 cups oat bran\n1/4 cup sunflower seeds\n1/2 cup amaranth, quinoa, millet, or poppy seeds (or any combo of these)\n2 tablespoons dulse flakes, or 1 teaspoon kosher salt\n1 teaspoon baking soda\n1/4 cup plus 2 tablespoons beer\n2 1/2 cups buttermilk, half-and-half, milk, or water\nunsalted butter, softened for serving","Big Sur Bakery Hide Bread Recipe","","","http://www.101cookbooks.com/archives/big-sur-bakery-hide-bread-recipe.html"),
            ("PT30M","2009-08-27","An old-fashioned blueberry cake sweetened with molasses adapted from a reader submitted recipe to the July 1974 issue of Gourmet Magazine - rustic, dark as chocolate, tender, and punctuated with lots of tiny pockets of oozy, magenta berry flesh.","http://www.101cookbooks.com/mt-static/images/food/blueberry_cake_recipe.jpg","1 cup plus 2 tablespoons unbleached all-purpose flour\n1 teaspoon baking powder\n1/2 teaspoon baking soda\n3/4 teaspoon fine grain sea salt\n1/2 teaspoon cider vinegar\n5 tablespoons milk (divided)\n1/2 cup unsulphered molasses\n2 large eggs, lightly beaten\n3 tablespoons unsalted butter, barely melted\n1 1/2 cups blueberries, frozen (I freeze fresh berries)\n1 teaspoon flour\nServe with a sprinkling of powdered sugar (optional), or a big dollop of sweetened freshly whipped cream","Old-Fashioned Blueberry Cake Recipe","PT10M","Serves8 - 10.\nFoo","http://www.101cookbooks.com/archives/oldfashioned-blueberry-cake-recipe.html"),
            ("PT","2013-04-01","Got leftover Easter eggs?Got leftover Easter ham?Got a hearty appetite?Good! You've come to the right place!I...","http://static.thepioneerwoman.com/cooking/files/2013/03/leftoversandwich.jpg","12 whole Hard Boiled Eggs\n1/2 cup Mayonnaise\n3 Tablespoons Grainy Dijon Mustard\n Salt And Pepper, to taste\n Several Dashes Worcestershire Sauce\n Leftover Baked Ham, Sliced\n Kaiser Rolls Or Other Bread\n Extra Mayonnaise And Dijon, For Spreading\n Swiss Cheese Or Other Cheese Slices\n Thinly Sliced Red Onion\n Avocado Slices\n Sliced Tomatoes\n Lettuce, Spinach, Or Arugula","Easter Leftover Sandwich","PT15M","8","http://thepioneerwoman.com/cooking/2013/04/easter-leftover-sandwich/"),
            ("PT10M","2011-06-06","I finally have basil in my garden. Basil I can use. This is a huge development.I had no basil during the winter. None. G...",f"http://static.{unknown_domains[0]}/cooking/files/2011/06/pesto.jpg","3/4 cups Fresh Basil Leaves\n1/2 cup Grated Parmesan Cheese\n3 Tablespoons Pine Nuts\n2 cloves Garlic, Peeled\n Salt And Pepper, to taste\n1/3 cup Extra Virgin Olive Oil\n1/2 cup Heavy Cream\n2 Tablespoons Butter\n1/4 cup Grated Parmesan (additional)\n12 ounces, weight Pasta (cavitappi, Fusili, Etc.)\n2 whole Tomatoes, Diced","Pasta with Pesto Cream Sauce","PT6M","8",f"http://{unknown_domains[0]}/cooking/2011/06/pasta-with-pesto-cream-sauce/")
        ]

        input_df = self.job.spark.createDataFrame(values, 'cookTime: string, datePublished: string, description: string, image: string, ingredients: string, name: string, prepTime: string, recipeYield: string, url: string')
        input_df.write.json(self.job.config.src_path, mode='overwrite')

        with pytest.raises(Exception) as error:
            self.job.job_steps()
        
        assert f'Found not registered domains: {unknown_domains}' == str(error.value)