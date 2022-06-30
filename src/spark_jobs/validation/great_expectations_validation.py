from great_expectations.data_context import DataContext
from datetime import datetime, timezone
from pyspark.sql import DataFrame

class GreatExpectationsValidation(object):
    
    def __init__(self, application_name: str, application_id: str, **kwargs):
        self.run_name = f'spark_{application_name}_{application_id}'
        self.context = DataContext()
        self.validation_batches = []

    def add_dataframe_to_validation_batches(self, df: DataFrame, datasource: str, expectations: str, **extra_params):
        batch_kwargs = { 'dataset': df, 'datasource': datasource, **extra_params }
        batch = self.context.get_batch(batch_kwargs, expectations)
        self.validation_batches.append(batch)

    def apply_validations(self):
        run_id = { "run_name": self.run_name, "run_time": datetime.now(timezone.utc) }

        results = self.context.run_validation_operator("action_list_operator",
            assets_to_validate=self.validation_batches,
            run_id=run_id
        )       

        self.context.build_data_docs() 
