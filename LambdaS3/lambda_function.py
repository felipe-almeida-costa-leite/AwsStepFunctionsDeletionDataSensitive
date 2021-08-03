# !/usr/bin/python3
# encoding: iso-8859-1
"""
Lambda para realizar os processos no DynamoDB
"""


import json
import os

from S3Client import S3Client


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
        name_bucket = event['body']['name']
    except KeyError:
        name_bucket = os.environ['NAME_BUCKET']
    if operation == 'delete_bucket':
        s3 = S3Client()
        response_operation = s3.delete_bucket(name_bucket)
        if response_operation:
            status_code = 200
        else:
            status_code = 400
        response_lambda = {
            'status_code': status_code,
            'body': response_operation
        }
        response_lambda = json.dumps(response_lambda)
        response_lambda = json.loads(response_lambda)
        return response_lambda
    elif operation == 'create_bucket':
        s3 = S3Client()
        response_operation = s3.create_bucket(name_bucket)
        if response_operation:
            status_code = 200
        else:
            status_code = 400
        response_lambda = {
            'status_code': status_code,
            'body': response_operation
        }
        response_lambda = json.dumps(response_lambda)
        response_lambda = json.loads(response_lambda)
        return response_lambda
    elif operation == 'put_object':
        try:
            key = event['body']['key']
        except KeyError:
            key = os.environ['KEY_PUT_OBJECT']
        body = bytes(json.dumps(event['body']['body']), encoding='ISO-8859-1')
        s3 = S3Client()
        response_operation = s3.put_object(name_bucket, key, body)
        if response_operation:
            status_code = 200
        else:
            status_code = 400
        response_lambda = {
            'status_code': status_code,
            'body': response_operation
        }
        response_lambda = json.dumps(response_lambda)
        response_lambda = json.loads(response_lambda)
        return response_lambda
    elif operation == 'get_object':
        try:
            key = event['body']['key']
        except KeyError:
            key = os.environ['KEY_GET_OBJECT']
        s3 = S3Client()
        response_operation = s3.get_object(name_bucket, key)
        if response_operation:
            status_code = 200
        else:
            status_code = 400
        response_lambda = {
            'status_code': status_code,
            'body': response_operation
        }
        response_lambda = json.dumps(response_lambda)
        response_lambda = json.loads(response_lambda)
        return response_lambda
    elif operation == 'delete_object':
        key = event['body']['key']
        s3 = S3Client()
        response_operation = s3.delete_object(name_bucket, key)
        if response_operation:
            status_code = 200
        else:
            status_code = 400
        response_lambda = {
            'status_code': status_code,
            'body': response_operation
        }
        response_lambda = json.dumps(response_lambda)
        response_lambda = json.loads(response_lambda)
        return response_lambda
    elif operation == 'delete_objects':
        s3 = S3Client()
        list_files = s3.list_object(name_bucket)
        if list_files:
            for file in list_files:
                s3.delete_object(name_bucket, file)
            if len(list_files) > 0:
                status_code = 200
                response_operation = True
                response_lambda = {
                    'status_code': status_code,
                    'body': response_operation
                }
                response_lambda = json.dumps(response_lambda)
                response_lambda = json.loads(response_lambda)
                return response_lambda
            else:
                status_code = 400
                response_operation = False
                response_lambda = {
                    'status_code': status_code,
                    'body': response_operation
                }
                response_lambda = json.dumps(response_lambda)
                response_lambda = json.loads(response_lambda)
                return response_lambda
        else:
            status_code = 400
            response_operation = False
            response_lambda = {
                'status_code': status_code,
                'body': response_operation
            }
            response_lambda = json.dumps(response_lambda)
            response_lambda = json.loads(response_lambda)
            return response_lambda
