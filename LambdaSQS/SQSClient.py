# !/usr/bin/python3
# encoding: iso-8859-1
"""
Este módulo realiza os processos relevantes para mensagens SQS, SEND, RECEIVE e DELETE
"""
import boto3
from typing import *
import json


class SQSClient(object):
    """
    Classe criada para instanciar o objeto SQS
    """

    @staticmethod
    def connect_sqs():
        """
        Cria uma conexão com o SQS
        :return: Return to connection instance
        :rtype: Object SQS
        """
        sqs = boto3.client('sqs')
        return sqs

    def __init__(self, queue: str) -> None:
        self.__queue = queue

    def send_message(self, body: str) -> bool:
        """
        Envia uma mensagem para a fila
        :param body: Conteúdo da mensagem.
        :type body: String(JSON)
        :return: Retorna se o processo foi sucedido ou falho
        :rtype: Bollean
        """
        sqs_send = self.connect_sqs()
        try:
            sqs_send.send_message(
                QueueUrl=self.__queue,
                MessageBody=body)
            return True
        except(sqs_send.exceptions.TooManyEntriesInBatchRequest,
               sqs_send.exceptions.EmptyBatchRequest,
               sqs_send.exceptions.BatchEntryIdsNotDistinct,
               sqs_send.exceptions.BatchRequestTooLong,
               sqs_send.exceptions.InvalidBatchEntryId,
               sqs_send.exceptions.UnsupportedOperation):
            return False

    def receive_messages(self, numbermessages: int = 10, waittime: int = 5) -> list:
        """
        Lista mensagens na fila
        :return: Lista contendo mensagens
        :rtype: Lista se o processo de recebimento foi sucedido ou não
        """
        listmessages: List = []
        sqs_receive = self.connect_sqs()
        for i in range(0, 10):
            try:
                messages = sqs_receive.receive_message(
                    QueueUrl=self.__queue,
                    MessageAttributeNames=['All'],
                    MaxNumberOfMessages=numbermessages,
                    WaitTimeSeconds=waittime
                )
                for message in messages['Messages']:
                    message = json.dumps(message)
                    message = json.loads(message)
                    listmessages.append(message)
            except KeyError:
                break
        return listmessages

    def receive_message(self, numbermessages: int = 1, waittime: int = 5) -> Union[list, bool]:
        """
        Retorna a ultima mensagem da fila
        :return: Conteúdo da mensagem
        :rtype: Retorna se o processo foi sucedido ou falho
        """
        try:
            sqs_receive = self.connect_sqs()
            messages = sqs_receive.receive_message(
                QueueUrl=self.__queue,
                MessageAttributeNames=['All'],
                MaxNumberOfMessages=numbermessages,
                WaitTimeSeconds=waittime
            )
            listmessages: List = []
            for message in messages['Messages']:
                listmessages.append(message)
            return listmessages
        except KeyError:
            return False

    def delete_message(self, receipthandle) -> bool:
        """
        Deleta a mensagem na fila
        :param receipthandle: ReceiptHandle da mensagem
        :type receipthandle: String
        :return: Retorna se a deleção foi realizada ou não
        :rtype: Bollean
        """
        sqs_delete = self.connect_sqs()
        try:
            sqs_delete.delete_message(
                QueueUrl=self.__queue,
                ReceiptHandle=receipthandle
            )
            return True
        except TypeError:
            return False

    def delete_messages(self, receipthandles: List[str]) -> List[bool]:
        """
        Deleta diversas mensagens na fila
        :param receipthandles: Lista dos ReceiptHandles das mensagens
        :type receipthandles: String
        :return: Retorna o Status da deleção
        :rtype: List
        """
        list_result = []
        for receipthandle in receipthandles:
            response = self.delete_message(receipthandle)
            list_result.append(response)
        return list_result

    def list_messages(self) -> int:
        """
        Lista as mensagens na fila, e retorna a quantidade
        :return: Retorna o número de mensagens na fila
        :rtype: Integer
        """
        listmessages = []
        for i in range(0, 500):
            try:
                sqs_receive = self.connect_sqs()
                messages = sqs_receive.receive_message(
                    QueueUrl=self.__queue,
                    MessageAttributeNames=['All'],
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=5
                )
                for message in messages['Messages']:
                    message = json.dumps(message['MessageId'])
                    message = json.loads(message)
                    if message in listmessages:
                        continue
                    else:
                        listmessages.append(message)
            except KeyError:
                break
        cont = len(listmessages)
        return cont
