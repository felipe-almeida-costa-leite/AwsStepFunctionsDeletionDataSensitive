# !/usr/bin/python3
# encoding: iso-8859-1
"""
Este módulo realiza as operações no DynamoDB, PUT, GET and UPDATE item.
"""
# Imports
import boto3
from typing import Union, Dict, List
from datetime import datetime
from pytz import utc


class DynamoClient(object):
    """
    Classe criada para instanciar o objeto da classe DynamoClient
    """
    @staticmethod
    def connect_dynamodb():
        """
        Cria uma conexão no dynamodb
        :return: Retorna a conexão
        :rtype: Objeto DynamoDB
        """
        client = boto3.client('dynamodb', region_name='us-east-1')
        return client

    @staticmethod
    def data() -> str:
        """
        Data atual em formato UTC
        :return: Data no formato 00:00:00 01/01/1997
        :rtype: String
        """
        now = datetime.now(tz=utc)
        date_time: str = now.strftime('%H:%M:%S %d/%m/%Y')
        return date_time

    def __init__(self, table: str):
        self.__table = table

    def get_item(self, hash_id: str) -> Union[Dict, bool]:
        """
        Retorna informações do item
        :param hash_id: Id na tabela do dynamodb
        :type hash_id: String
        :return: Retorna as informações do item
        :rtype: Dict or Bollean
        """
        dynamodb_get = self.connect_dynamodb()
        try:
            response = dynamodb_get.batch_get_item(
                RequestItems={
                    self.__table: {
                        'Keys': [
                            {
                                'HASH': {
                                    'S': hash_id
                                }
                            }
                        ]
                    }
                }
            )
            return response['Responses'][str(self.__table)][0]
        except (KeyError, dynamodb_get.exceptions.InternalServerError,
                dynamodb_get.exceptions.RequestLimitExceeded,
                dynamodb_get.exceptions.ProvisionedThroughputExceededException):
            return False

    def put_item(self, hash_id: str, status: str = 'AGUARDANDO') -> bool:
        """
        Cria um item na tabela do dynamodb.
        :param hash_id: Id na tabela do dynamodb
        :type hash_id: String
        :param status: Status do item na tabela
        :type status: String
        :return: Retorna se o processo for sucesso ou falha
        :rtype: Bollean
        """
        created_at: str = self.data()
        updated_at: str = self.data()
        dynamodb_put = self.connect_dynamodb()
        try:
            dynamodb_put.put_item(
                TableName=self.__table,
                Item={
                    'HASH': {
                        'S': hash_id
                    },
                    'STATUS': {
                        'S': status
                    },
                    'CREATED_AT': {
                        'S': created_at
                    },
                    'UPDATED_AT': {
                        'S': updated_at
                    }
                }
            )
            return True
        except (dynamodb_put.exceptions.ConditionalCheckFailedException,
                dynamodb_put.exceptions.ProvisionedThroughputExceededException,
                dynamodb_put.exceptions.ConditionalCheckFailedException,
                dynamodb_put.exceptions.ProvisionedThroughputExceededException,
                dynamodb_put.exceptions.ResourceNotFoundException,
                dynamodb_put.exceptions.TransactionConflictException,
                dynamodb_put.exceptions.RequestLimitExceeded,
                dynamodb_put.exceptions.InternalServerError):
            return False

    def update_item(self, hash_id: str, status: str = 'EM PROCESSO') -> bool:
        """
        Atualiza um item.
        :param hash_id: Id do item na tabela do dynamo.
        :type hash_id: String
        :param status: Status do item na tabela
        :type status: String
        :return: Retorna se o processo for sucesso ou falha
        :rtype: String
        """
        dynamodb_update = self.connect_dynamodb()
        try:
            dynamodb_update.update_item(
                TableName=self.__table,
                Key={
                    'HASH': {
                        'S': hash_id
                    }
                },
                AttributeUpdates={
                    'STATUS': {
                        'Value': {
                            'S': status
                        }
                    },
                    'UPDATED_AT': {
                        'Value': {
                            'S': self.data()
                        }
                    }
                },
                ReturnValues='UPDATED_NEW'
            )
            return True
        except (dynamodb_update.exceptions.ConditionalCheckFailedException,
                dynamodb_update.exceptions.ProvisionedThroughputExceededException,
                dynamodb_update.exceptions.ResourceNotFoundException,
                dynamodb_update.exceptions.TransactionConflictException,
                dynamodb_update.exceptions.InternalServerError
                ):
            return False

    def update_itens(self, list_itens: List[Union[Dict, str]]) -> List[bool]:
        """
        Atualiza diversos itens.
        :param list_itens: Lista dos itens.
        :type list_itens: String
        :return: Retorna de foi sucesso ou falha
        :rtype: String
        """
        list_result = []
        for item in list_itens:
            if type(item) == dict:
                hash_string_item: str = item['Hash']
                status_string_item: str = item['Status']
                response = self.update_item(hash_string_item, status_string_item)
                list_result.append(response)
            elif type(item) == str:
                hash_string: str = item
                response = self.update_item(hash_id=hash_string)
                list_result.append(response)
        return list_result
