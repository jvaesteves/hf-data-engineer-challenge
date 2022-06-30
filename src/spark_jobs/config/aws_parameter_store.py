from typing import Dict, Any
import boto3

def make_request_to_parameter_store(parameter_path: str) -> Dict[str, str]:
    ssm = boto3.client('ssm')
    return ssm.get_parameters_by_path(Path=parameter_path) 

def get_settings_from_aws_parameter_store(parameter_path: str) -> Dict[str, str]:
    if parameter_path[-1] != '/':
        parameter_path += '/'

    response = make_request_to_parameter_store(parameter_path)
    parameters = response.get('Parameters')

    if parameters:
        parameters = {
            parameter['Name'][len(parameter_path):]: parameter['Value']
            for parameter in parameters    
        }
    else:
        raise Exception(f'There are no parameters config on path: {parameter_path}')

    return parameters
