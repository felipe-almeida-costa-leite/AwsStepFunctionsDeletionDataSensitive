# !/usr/bin/python3
# encoding: iso-8859-1
"""
Função lambda para realizar as operações no DynamoDB
"""


import json
import os
from DynamoClient import DynamoClient


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
        table = event['Table']
    except KeyError:
        table = os.environ['TABLE']
    if operation == 'get':
        dynamodb = DynamoClient(table)
        hash_string = event['body']['Hash']
        response_operation = dynamodb.get_item(hash_string)
        if not response_operation:
            status_code = 417
        else:
            status_code = 200
        response_lambda = {
            'status_code': status_code,
            'body': response_operation
        }
        response_lambda = json.dumps(response_lambda)
        response_lambda = json.loads(response_lambda)
        return response_lambda
    elif operation == 'put':
        dynamodb = DynamoClient(table)
        hash_string = event['body']['Hash']
        response_operation = dynamodb.put_item(hash_id=hash_string)
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
    elif operation == 'update':
        dynamodb = DynamoClient(table)
        hash_string = event['body']['Hash']
        status_string = event['body']['Status']
        response_operation = dynamodb.update_item(hash_id=hash_string, status=status_string)
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
    elif operation == 'updates':
        dynamodb = DynamoClient(table)
        response_operation = dynamodb.update_itens(event['body'])
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
