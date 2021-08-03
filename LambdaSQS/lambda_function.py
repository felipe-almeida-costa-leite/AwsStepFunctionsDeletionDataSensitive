# !/usr/bin/python3
# encoding: iso-8859-1
"""
Função Lambda para realizar operações em SQS
"""


import json
import os
from SQSClient import SQSClient
from math import ceil


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
        queue = event['Queue']
    except KeyError:
        queue = os.environ['QUEUE']
    if operation == 'receive':
        sqs = SQSClient(queue)
        response_operation = sqs.receive_message()
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
    elif operation == 'send':
        sqs = SQSClient(queue)
        message = event['message']
        response_operation = sqs.send_message(message)
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
    elif operation == 'delete':
        sqs = SQSClient(queue)
        message = event['ReceiptHandle']
        response_operation = sqs.delete_message(message)
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
    elif operation == 'receives':
        list_body = []
        list_hash_string = []
        list_receipthandles = []
        sqs = SQSClient(queue)
        messages = sqs.receive_messages()
        if not messages:
            status_code = 417
        else:
            status_code = 200
        for message in messages:
            list_receipthandles.append(message['ReceiptHandle'])
            bodys = json.loads(message['Body'])
            list_body.append(bodys)
            list_hash_string.append(bodys['Hash'])
        response_lambda = {
            'status_code': status_code,
            'messages': {
                'Bodys': list_body,
                'Hashs': list_hash_string,
                'ReceiptHandles': list_receipthandles
            }
        }
        response_lambda = json.dumps(response_lambda)
        response_lambda = json.loads(response_lambda)
        return response_lambda
    elif operation == 'deletion':
        sqs = SQSClient(queue)
        messages = event['ReceiptHandles']
        response_operation = sqs.delete_messages(messages)
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
    elif operation == 'list':
        sqs = SQSClient(queue)
        response_operation = int(sqs.list_messages())
        smallest_number = response_operation//100
        higher_number = response_operation/100
        cycle_numbers = higher_number - smallest_number
        if response_operation == 0:
            status_code = 200
            response_lambda = {
                'status_code': status_code,
                'body': 0
            }
            response_lambda = json.dumps(response_lambda)
            response_lambda = json.loads(response_lambda)
            return response_lambda
        else:
            if cycle_numbers >= 0:
                response_operation = []
                for i in range(0, ceil(higher_number), 1):
                    response_operation.append(i)
                response_lambda = {
                    'status_code': 200,
                    'body': response_operation
                }
                response_lambda = json.dumps(response_lambda)
                response_lambda = json.loads(response_lambda)
                return response_lambda
            else:
                response_lambda = {
                    'status_code': 417,
                    'body': False
                }
                response_lambda = json.dumps(response_lambda)
                response_lambda = json.loads(response_lambda)
                return response_lambda
