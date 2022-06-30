from spark_jobs.steps.recipe_history_ingestion.domain_spliter_step import DomainSpliterStep

from spark_step_test_case import SparkStepTestCase
import pytest

class DomainSpliterStepTest(SparkStepTestCase):

    def test_valid_split_dataframe_by_url_domains(self):
        spark_job = self.make_spark_job_mock()
        step = DomainSpliterStep(spark_job)

        values = [
            ("Easter Leftover Sandwich","http://thepioneerwoman.com/cooking/2013/04/easter-leftover-sandwich/"),
            ("Pasta with Pesto Cream Sauce","http://thepioneerwoman.com/cooking/2011/06/pasta-with-pesto-cream-sauce/"),
            ("Big Sur Bakery Hide Bread Recipe","http://www.101cookbooks.com/archives/big-sur-bakery-hide-bread-recipe.html"),
            ("Old-Fashioned Blueberry Cake Recipe","http://www.101cookbooks.com/archives/oldfashioned-blueberry-cake-recipe.html")
        ]

        initial_df = self.spark.createDataFrame(values, 'name: string, url: string')

        values = {
            'thepioneerwoman.com': [
                ("Easter Leftover Sandwich","http://thepioneerwoman.com/cooking/2013/04/easter-leftover-sandwich/"),
                ("Pasta with Pesto Cream Sauce","http://thepioneerwoman.com/cooking/2011/06/pasta-with-pesto-cream-sauce/")
            ],
            '101cookbooks.com': [
                ("Big Sur Bakery Hide Bread Recipe","http://www.101cookbooks.com/archives/big-sur-bakery-hide-bread-recipe.html"),
                ("Old-Fashioned Blueberry Cake Recipe","http://www.101cookbooks.com/archives/oldfashioned-blueberry-cake-recipe.html")
            ],
        }

        expected_dfs = {
            domain: self.spark.createDataFrame(data, 'name: string, url: string')
            for domain, data in values.items()
        }

        output_dfs = step.split_dataframe_by_url_domains(initial_df, 'url', values.keys())

        for domain, output_df in output_dfs.items():
            expected_df = expected_dfs.get(domain)
            assert expected_df is not None
            self.assert_dataframes_are_equal(expected_df, output_df)

    def test_valid_split_dataframe_by_url_domains_missing_data(self):
        spark_job = self.make_spark_job_mock()
        step = DomainSpliterStep(spark_job)

        values = [ ]

        initial_df = self.spark.createDataFrame(values, 'name: string, url: string')

        values = {
            'thepioneerwoman.com': [  ],
            '101cookbooks.com': [ ],
        }

        expected_dfs = {
            domain: self.spark.createDataFrame(data, 'name: string, url: string')
            for domain, data in values.items()
        }

        output_dfs = step.split_dataframe_by_url_domains(initial_df, 'url', values.keys())


        for domain, output_df in output_dfs.items():
            expected_df = expected_dfs.get(domain)
            assert expected_df is not None
            self.assert_dataframes_are_equal(expected_df, output_df)

    def test_invalid_split_dataframe_by_url_domains_unknown_domains(self):
        spark_job = self.make_spark_job_mock()
        step = DomainSpliterStep(spark_job)

        values = [
            ("Easter Leftover Sandwich","http://thepioneerwoman.com/cooking/2013/04/easter-leftover-sandwich/"),
            ("Pasta with Pesto Cream Sauce","http://thepioneerwoman.com/cooking/2011/06/pasta-with-pesto-cream-sauce/"),
            ("Big Sur Bakery Hide Bread Recipe","http://www.101cookbooks.com/archives/big-sur-bakery-hide-bread-recipe.html"),
            ("Old-Fashioned Blueberry Cake Recipe","http://www.101cookbooks.com/archives/oldfashioned-blueberry-cake-recipe.html")
        ]

        initial_df = self.spark.createDataFrame(values, 'name: string, url: string')

        unknown_domains = ['thepioneerwoman.com']
        with pytest.raises(Exception) as error:
            output_dfs = step.split_dataframe_by_url_domains(initial_df, 'url', ['101cookbooks.com'])

        assert f'Found not registered domains: {unknown_domains}' == str(error.value)
