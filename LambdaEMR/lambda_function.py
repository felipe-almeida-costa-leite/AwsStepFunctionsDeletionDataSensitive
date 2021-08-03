# !/usr/bin/python3
# encoding: iso-8859-1
"""
Lambda para realizar os processos no EMR
"""

import json
import os
from EMRClient import EMRClient


def lambda_handler(event, context) -> str:
    """
    Recebe um Event no formato json.
    :param event: Contém informações da operação a ser realizada.
    :type event: STRING(JSON)
    :param context: Não utilizado
    :type context: None
    :return: Retorna um json
    :rtype: STRING(JSON)
    """
    operation = event['Operation']
    try:
        name_cluster = event['body']['name']
    except KeyError:
        name_cluster = os.environ['NAME_CLUSTER']
    if operation == 'get_id':
        emr = EMRClient()
        response_operation = emr.get_cluster_id(name_cluster)
        if response_operation:
            status_code = 200
        else:
            status_code = 417
        response_lambda = {
            'status_code': status_code,
            'body': response_operation
        }
        response_lambda = json.dumps(response_lambda)
        response_lambda = json.loads(response_lambda)
        return response_lambda
