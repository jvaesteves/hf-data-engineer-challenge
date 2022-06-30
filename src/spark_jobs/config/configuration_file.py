from configparser import ConfigParser, MissingSectionHeaderError
from typing import Dict

def get_settings_from_configuration_file(file_path: str) -> Dict[str, str]:
    config = ConfigParser()

    try:
        config.read(file_path)
    except MissingSectionHeaderError:
        raise Exception(f'Invalid configuration file: {file_path}')

    if not config.sections():
        raise Exception(f'No configuration file on path: {file_path}')
    elif 'default' not in config.sections():
        raise Exception('No section [default] was defined')

    return dict(config['default'])
