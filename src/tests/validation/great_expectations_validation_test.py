from spark_jobs.validation.great_expectations_validation import GreatExpectationsValidation

from unittest.mock import patch, PropertyMock
from unittest import TestCase

class GreatExpectationsValidationTest(TestCase):

    def setUp(self):
        with patch.object(GreatExpectationsValidation, '__init__', lambda self, application_name, application_id, **kwargs: None):
            self.ge = GreatExpectationsValidation('foo', 'bar')
            self.ge.run_name = 'testing'
            self.ge.context = PropertyMock()
            self.ge.validation_batches = PropertyMock()

    def test_valid_add_dataframe_to_validation_batches(self):
        self.ge.add_dataframe_to_validation_batches(None, None, None)
        assert self.ge.context.get_batch.call_count == 1
        assert self.ge.validation_batches.append.call_count == 1

    def test_valid_apply_validations(self):
        self.ge.apply_validations()
        assert self.ge.context.run_validation_operator.call_count == 1
        assert self.ge.context.build_data_docs.call_count == 1