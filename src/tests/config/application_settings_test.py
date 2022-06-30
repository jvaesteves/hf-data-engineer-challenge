from spark_jobs.config.application_settings import ApplicationSettings

from argparse import ArgumentParser, Namespace
from datetime import datetime
from unittest.mock import patch
from unittest import TestCase
import pytest

class ApplicationSettingsTest(TestCase):

    @patch('spark_jobs.config.aws_parameter_store.get_settings_from_aws_parameter_store')
    def test_valid_aws_settings(self, mock_get_settings_from_aws_parameter_store):
        mock_get_settings_from_aws_parameter_store.return_value = {
            'src_path': 'foo',
            'dest_path': 'bar'
        }

        config = ApplicationSettings('aws', ['src_path', 'dest_path'], parameter_path='/foo')

        assert config is not None
        assert config.src_path == 'foo'
        assert config.dest_path == 'bar'

    @patch('spark_jobs.config.configuration_file.get_settings_from_configuration_file')
    def test_valid_configfile_settings(self, mock_get_settings_from_configuration_file):
        mock_get_settings_from_configuration_file.return_value = { 
            'src_path': 'foo', 
            'dest_path': 'bar' 
        }

        config = ApplicationSettings('ini', ['src_path', 'dest_path'], filepath='/foo/config.ini')

        assert config is not None
        assert config.src_path == 'foo'
        assert config.dest_path == 'bar'

    @patch('spark_jobs.config.configuration_file.get_settings_from_configuration_file')
    def test_valid_console_parameters_extra_parameters_warning(self, mock_get_settings_from_configuration_file):
        extra_param = 'extra_param'
        mock_get_settings_from_configuration_file.return_value = { 
            'src_path': 'foo', 
            'dest_path': 'bar',
            extra_param: 'foobar'
        }

        with self.assertLogs(ApplicationSettings.__name__, level='WARN') as logs:
            config = ApplicationSettings('ini', ['src_path', 'dest_path'], filepath='/foo/config.ini')

        expected_arning_message = f"WARNING:ApplicationSettings:The configuration source has the following extra parameters: {{'{extra_param}'}}"
        assert expected_arning_message in logs.output


    def test_invalid_settings_type(self):
        settings_type = 'foo'
        with pytest.raises(Exception) as error:
            config = ApplicationSettings(settings_type, ['src_path', 'dest_path'], foo='bar')

        assert f'Unknown application settings type: {settings_type}' == str(error.value)

    def test_missing_kwargs_parameter(self):
        settings_type = 'aws'
        kwargs_parameter = 'parameter_path'
        with pytest.raises(Exception) as error:
            config = ApplicationSettings(settings_type, ['src_path', 'dest_path'])

        assert f"Mandatory parameter '{kwargs_parameter}' was not informed on kwargs for '{settings_type}' application settings" == str(error.value)

    @patch('spark_jobs.config.aws_parameter_store.get_settings_from_aws_parameter_store')
    def test_missing_application_parameters(self, mock_get_settings_from_aws_parameter_store):
        mock_get_settings_from_aws_parameter_store.return_value = { 'src_path': 'foo' }
        missing_parameters = { 'dest_path' }

        settings_type = 'aws'
        with pytest.raises(Exception) as error:
            config = ApplicationSettings(settings_type, ['src_path', 'dest_path'], parameter_path='/foo')

        assert f"The following mandatory parameters were not informed: {missing_parameters}" == str(error.value)

    @patch('argparse.ArgumentParser.parse_args')
    def test_valid_console_parameters_config_file(self, mock_parse_args):
        execution_datetime='2000-01-01T00:00:00'
        mock_parse_args.return_value = Namespace(
            execution_datetime=execution_datetime, 
            parameter_store=None,
            config_file='/foo/bar.ini'
        )

        with patch.object(ApplicationSettings, "__init__", lambda self, settings_type, application_parameters, **kwargs: None):
            config = ApplicationSettings.make_from_console_parameters(application_parameters=[])

        assert config is not None
        assert config.execution_datetime == datetime.fromisoformat(execution_datetime)

    @patch('argparse.ArgumentParser.parse_args')
    def test_valid_console_parameters_parameter_store(self, mock_parse_args):
        execution_datetime='2000-01-01T00:00:00'
        mock_parse_args.return_value = Namespace(
            execution_datetime=execution_datetime, 
            parameter_store='/foo/bar',
            config_file=None
        )

        with patch.object(ApplicationSettings, "__init__", lambda self, settings_type, application_parameters, **kwargs: None):
            config = ApplicationSettings.make_from_console_parameters(application_parameters=[])

        assert config is not None
        assert config.execution_datetime == datetime.fromisoformat(execution_datetime)

    @patch('argparse.ArgumentParser.parse_args')
    def test_invalid_console_parameters(self, mock_parse_args):
        mock_parse_args.return_value = Namespace(
            execution_datetime=None, 
            parameter_store=None,
            config_file=None
        )

        with pytest.raises(Exception) as error:
            config = ApplicationSettings.make_from_console_parameters(application_parameters=[])

        assert 'You should choose to get parameters from AWS Parameter Store or a configuration file' == str(error.value)
    