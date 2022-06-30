from spark_jobs.config.aws_parameter_store import get_settings_from_aws_parameter_store

from unittest.mock import Mock, patch
from unittest import TestCase
import boto3, pytest

class AWSParameterStoreConfigTest(TestCase):

    @patch('spark_jobs.config.aws_parameter_store.make_request_to_parameter_store')
    def test_valid_parameter_path(self, mock_make_request_to_parameter_store):
        parameter_path = '/foo/bar/'
        return_value = { 'Parameters': [{ 'Name': f'{parameter_path}API_SECRET', 'Value': 'foobar'}] }
        mock_make_request_to_parameter_store.return_value = return_value

        config = get_settings_from_aws_parameter_store(parameter_path)

        assert config is not None
        assert config['API_SECRET'] == 'foobar'

    @patch('spark_jobs.config.aws_parameter_store.make_request_to_parameter_store')
    def test_valid_parameter_path_no_trailing_slash(self, mock_make_request_to_parameter_store):
        parameter_path = '/foo/bar'
        return_value = { 'Parameters': [{ 'Name': f'{parameter_path}/API_SECRET', 'Value': 'foobar'}] }
        mock_make_request_to_parameter_store.return_value = return_value

        config = get_settings_from_aws_parameter_store(parameter_path)

        assert config is not None
        assert config['API_SECRET'] == 'foobar'

    @patch('spark_jobs.config.aws_parameter_store.make_request_to_parameter_store')
    def test_empty_parameter_path(self, mock_make_request_to_parameter_store):
        mock_make_request_to_parameter_store.return_value = {}
        parameter_path = '/foo/bar/'

        with pytest.raises(Exception) as error:
            get_settings_from_aws_parameter_store(parameter_path)

        assert f'There are no parameters config on path: {parameter_path}' == str(error.value)
