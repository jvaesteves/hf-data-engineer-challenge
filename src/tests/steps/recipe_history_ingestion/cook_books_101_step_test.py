from spark_jobs.steps.recipe_history_ingestion.cook_books_101_step import CookBooks101Step

from datetime import date
from spark_step_test_case import SparkStepTestCase


class CookBooks101StepTest(SparkStepTestCase):

    def test_valid_transform_cook_prepare_time(self):
        spark_job = self.make_spark_job_mock()
        step = CookBooks101Step(spark_job)

        values = [("PT950M","PT60M"), ("PT20M","PT15M"), ("","PT40M"), ("PT5M","PT25M"), ("PT10M","PT45M"), ("",""), ("PT20M","PT20M"), ("PT180M","")]
        initial_df = self.spark.createDataFrame(values, 'cookTime: string, prepTime: string')

        values = [(950, 60), (20, 15), (None, 40), (5, 25), (10, 45), (None, None), (20, 20), (180, None)]
        expected_df = self.spark.createDataFrame(values, 'cookTime: int, prepTime: int')

        output_df = step.transform_cook_prepare_time(initial_df)

        self.assert_dataframes_are_equal(expected_df, output_df)


    def test_valid_extract_recipe_name(self):
        spark_job = self.make_spark_job_mock()
        step = CookBooks101Step(spark_job)

        values = [
            ("Orzo Super Salad Recipe","And orzo salad packed with nutritious ingredients ","http://www.101cookbooks.com/archives/orzo-super-salad-recipe.html","http://www.101cookbooks.com/mt-static/images/food/orzo_super_salad_recipe.jpg"),
            ("","101 Cookbooks: Penne alla Vodka Recipe","http://www.101cookbooks.com/archives/000054.html","http://www.101cookbooks.com/mt-static/images/food/.jpg"),
            ("","A twist on a classic onion soup recipe adapted for cry-babies (like me)","http://www.101cookbooks.com/archives/001076.html","http://www.101cookbooks.com/mt-static/images/food/onion_soup_recipe1.jpg"),
            ("","In the realm of garlic soup recipes, this is a favorite of mine.","http://www.101cookbooks.com/archives/richard-olneys-garlic-soup-recipe.html","http://www.101cookbooks.com/mt-static/images/food/garlic_soup_recipe.jpg"),
        ]

        initial_df = self.spark.createDataFrame(values, 'name: string, description: string, url: string, image: string')
        
        values = [("Orzo Super Salad Recipe",), ("Penne alla Vodka Recipe",), ("Onion Soup Recipe",), ("Richard Olneys Garlic Soup Recipe",)]
        expected_df = self.spark.createDataFrame(values, 'name: string')
        
        output_df = initial_df.select(step.extract_recipe_name(initial_df).alias('name'))

        self.assert_dataframes_are_equal(expected_df, output_df)

    def test_valid_extract_recipe_yield(self):
        spark_job = self.make_spark_job_mock()
        step = CookBooks101Step(spark_job)

        values = [
            ("Makes four sandwiches.",),
            ("Serves about 4 - 6 as a side.",),
            ("Makes two mega scones.",),
            ("Makes about 1 1/2 to 2 cups of puree.",),
            ("Serves 2 - 4.",),
            ("4",),
            ("Serves 6 to 8\nActive time: 40 minutes\nStart to finish: 5 1/2 hours",),
        ]

        initial_df = self.spark.createDataFrame(values, 'recipeYield: string')

        values = [
            ("Makes four sandwiches.",),
            ("Serves about 4 - 6 as a side.",),
            ("Makes two mega scones.",),
            ("Makes about 1 1/2 to 2 cups of puree.",),
            ("Serves 2 - 4.",),
            ("4",),
            ("Serves 6 to 8",),
        ]
        expected_df = self.spark.createDataFrame(values, 'recipeYield: string')
        
        output_df = initial_df.select(step.extract_recipe_yield(initial_df.recipeYield).alias('recipeYield'))

        self.assert_dataframes_are_equal(expected_df, output_df)

    def test_valid_run(self):
        spark_job = self.make_spark_job_mock()
        step = CookBooks101Step(spark_job)

        values = [
            ("",date(2004,10,20),"101 Cookbooks: Apple Pie Recipe","http://www.101cookbooks.com/mt-static/images/food/.jpg","3 tablespoons all-purpose flour","","","Serves 6 to 8\nActive time: 40 minutes\nStart to finish: 5 1/2 hours","http://www.101cookbooks.com/archives/000122.html"),
            ("PT30M",date(2009,8,27),"An old-fashioned blueberry cake","http://www.101cookbooks.com/mt-static/images/food/blueberry_cake_recipe.jpg","1 cup plus 2 tablespoons unbleached all-purpose flour","","PT10M","Serves 8 - 10","http://www.101cookbooks.com/archives/oldfashioned-blueberry-cake-recipe.html"),
            ("PT60M",date(2009,10,25),"An apple and carrot-flecked shortbread","http://www.101cookbooks.com/mt-static/images/food/apple_carrot_shortbread.jpg","50gsemolina flour","","","","http://www.101cookbooks.com/archives/apple-and-carrot-shortbread-recipe.html"),
            ("",date(2009,11,15),"Rustic orange-scented oat scones","http://www.101cookbooks.com/mt-static/images/food/nepenthe_scone_recipe.jpg","3 cups whole wheat pastry flour","Orange and Oat Scone Recipe","PT100M","8","http://www.101cookbooks.com/archives/figgy-buckwheat-scones-recipe.html"),
        ]

        initial_df = self.spark.createDataFrame(values, 'cookTime: string, datePublished: date, description: string, image: string, ingredients: string, name: string, prepTime: string, recipeYield: string, url: string')

        values = [
            (None,date(2004,10,20),"101 Cookbooks: Apple Pie Recipe","http://www.101cookbooks.com/mt-static/images/food/.jpg","3 tablespoons all-purpose flour","Apple Pie Recipe",None,"Serves 6 to 8","http://www.101cookbooks.com/archives/000122.html"),
            (30,date(2009,8,27),"An old-fashioned blueberry cake","http://www.101cookbooks.com/mt-static/images/food/blueberry_cake_recipe.jpg","1 cup plus 2 tablespoons unbleached all-purpose flour","Oldfashioned Blueberry Cake Recipe",10,"Serves 8 - 10","http://www.101cookbooks.com/archives/oldfashioned-blueberry-cake-recipe.html"),
            (60,date(2009,10,25),"An apple and carrot-flecked shortbread","http://www.101cookbooks.com/mt-static/images/food/apple_carrot_shortbread.jpg","50gsemolina flour","Apple And Carrot Shortbread Recipe",None,None,"http://www.101cookbooks.com/archives/apple-and-carrot-shortbread-recipe.html"),
            (None,date(2009,11,15),"Rustic orange-scented oat scones","http://www.101cookbooks.com/mt-static/images/food/nepenthe_scone_recipe.jpg","3 cups whole wheat pastry flour","Orange and Oat Scone Recipe",100,"8","http://www.101cookbooks.com/archives/figgy-buckwheat-scones-recipe.html")
        ]

        expected_df = self.spark.createDataFrame(values, 'cookTime: int, datePublished: date, description: string, image: string, ingredients: string, name: string, prepTime: int, recipeYield: string, url: string')

        output_df = step.run(initial_df)

        self.assert_dataframes_are_equal(expected_df, output_df)
