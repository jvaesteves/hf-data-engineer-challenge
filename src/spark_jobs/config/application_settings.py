from __future__ import annotations
from datetime import datetime, timezone
from argparse import ArgumentParser
from typing import Dict, List
import logging

logging.basicConfig(format='%(asctime)s [%(levelname)s] %(name)s.%(funcName)s: %(message)s', level=logging.INFO)

class ApplicationSettings:
    
    def __init__(self, settings_type: str, application_parameters: List[str], **kwargs):
        log = logging.getLogger(ApplicationSettings.__name__)

        try:
            if settings_type == 'aws':
                log.info(f'Creating settings object from AWS Parameter Store with the following parameters: {application_parameters}')

                from spark_jobs.config.aws_parameter_store import get_settings_from_aws_parameter_store
                parameters = get_settings_from_aws_parameter_store(kwargs['parameter_path'])
            elif settings_type == 'ini':
                log.info(f'Creating settings object from configuration file with the following parameters: {application_parameters}')

                from spark_jobs.config.configuration_file import get_settings_from_configuration_file
                parameters = get_settings_from_configuration_file(kwargs['filepath'])
            else:
                raise Exception(f'Unknown application settings type: {settings_type}')    
        except KeyError as ex:
            raise Exception(f"Mandatory parameter '{ex.args[0]}' was not informed on kwargs for '{settings_type}' application settings")

        self.set_parameters_from_dict(application_parameters, parameters)

    def set_parameters_from_dict(self, application_parameters: List[str], parameters_dict: Dict[str, str]):
        expected_parameters = set(application_parameters)
        received_parameters = set(parameters_dict.keys())

        missing_parameters = expected_parameters.difference(received_parameters)
        if missing_parameters:
            raise Exception(f'The following mandatory parameters were not informed: {missing_parameters}')

        unknown_parameters = received_parameters.difference(expected_parameters)
        if unknown_parameters:
            log = logging.getLogger(ApplicationSettings.__name__)
            log.warning(f'The configuration source has the following extra parameters: {unknown_parameters}')
            
        for parameter in application_parameters:
            setattr(self, parameter, parameters_dict[parameter])

    @staticmethod
    def make_from_console_parameters(application_parameters: List[str]) -> ApplicationSettings:
        parser = ArgumentParser()
        now = datetime.now(timezone.utc).isoformat()

        parser.add_argument("-d", "--execution-datetime", default=now, help="Execution datetime to run this process on ISO format.")
        parser.add_argument("-P", "--parameter-store", help="A path for AWS Parameter Store.")
        parser.add_argument("-C", "--config-file", help="A configuration file path.")
        args = parser.parse_args()

        if args.parameter_store is not None:
            config = ApplicationSettings('aws', application_parameters, parameter_path=args.parameter_store)
        elif args.config_file is not None:
            config = ApplicationSettings('ini', application_parameters, filepath=args.config_file)
        else:
            raise Exception('You should choose to get parameters from AWS Parameter Store or a configuration file')

        log = logging.getLogger(ApplicationSettings.__name__)
        log.info(f"Adding the extra parameter 'execution_datetime' to ApplicationSettings with value: {args.execution_datetime}")
        setattr(config, 'execution_datetime', datetime.fromisoformat(args.execution_datetime))

        return config
