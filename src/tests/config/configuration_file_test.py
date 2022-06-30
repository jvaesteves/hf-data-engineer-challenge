from spark_jobs.config.configuration_file import get_settings_from_configuration_file

from configparser import ConfigParser, MissingSectionHeaderError
from unittest.mock import Mock
from unittest import TestCase
import pytest


class ConfigurationFileTest(TestCase):

    def test_valid_configuration_file(self):
        sections = ['default']
        ConfigParser.read = Mock(return_value=sections)
        ConfigParser.sections = Mock(return_value=sections)
        ConfigParser.__getitem__ = Mock(return_value={'foo': 'bar'})
        file_path = 'foo'

        configuration_file = get_settings_from_configuration_file(file_path)
        
        assert configuration_file is not None
        assert configuration_file['foo'] == 'bar'

    def test_invalid_configuration_file(self):
        ConfigParser.read = Mock(side_effect=MissingSectionHeaderError('', 1, 1))
        file_path = 'foo'

        with pytest.raises(Exception) as error:
            get_settings_from_configuration_file(file_path)

        assert f'Invalid configuration file: {file_path}' == str(error.value)

    def test_no_section(self):
        sections = []
        ConfigParser.read = Mock(return_value=sections)
        ConfigParser.sections = Mock(return_value=sections)
        file_path = 'foo'

        with pytest.raises(Exception) as error:
            get_settings_from_configuration_file(file_path)

        assert f'No configuration file on path: {file_path}' == str(error.value)

    def test_no_default_section(self):
        sections = ['foo']
        ConfigParser.read = Mock(return_value=sections)
        ConfigParser.sections = Mock(return_value=sections)
        file_path = 'bar'

        with pytest.raises(Exception) as error:
            get_settings_from_configuration_file(file_path)

        assert f'No section [default] was defined' == str(error.value)
