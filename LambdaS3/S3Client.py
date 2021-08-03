# !/usr/bin/python3
# encoding: iso-8859-1
"""
Este módulo executa os processos relevantes para S3, PUT, GET e DELETE item ou itens.
"""
# Imports
import json
import boto3
from typing import Union, List, ByteString


class S3Client(object):
    """
    Classe criada para instanciar o objeto S3
    """

    @staticmethod
    def connect_s3():
        """
        Cria uma conexão com S3
        :return: Retorna a conexão com S3
        :rtype: Object S3
        """
        s3 = boto3.client('s3')
        return s3

    @staticmethod
    def s3_resource():
        """
        Cria uma conexão com o recurso S3
        :return: Retorna a conexão com o recurso S3
        :rtype: Object S3
        """
        s3 = boto3.resource('s3')
        return s3

    def create_bucket(self, name_bucket: str) -> bool:
        """
        Cria um Bucket
        :param name_bucket: Nome do Bucket
        :type name_bucket: String
        :return: Retorna se o processo foi bem sucedido ou não.
        :rtype: Bollean
        """
        s3 = self.connect_s3()
        try:
            s3.create_bucket(
                Bucket=name_bucket
            )
            return True
        except (s3.exceptions.BucketAlreadyExists,
                s3.exceptions.BucketAlreadyOwnedByYou):
            return False

    def delete_bucket(self, name_bucket: str) -> bool:
        """
        Deleta um bucket
        :param name_bucket: Nome do Bucket
        :type name_bucket: String
        :return: Retorna se o processo foi bem sucedido ou não.
        :rtype: Bollean
        """
        s3 = self.connect_s3()
        try:
            s3.delete_bucket(
                Bucket=name_bucket
            )
            return True
        except (s3.exceptions.BucketAlreadyExists,
                s3.exceptions.BucketAlreadyOwnedByYou):
            return False

    def delete_object(self, name_bucket: str, name_object: str) -> bool:
        """
        Deleta um objeto no bucket
        :param name_bucket: Nome do Bucket
        :type name_bucket: String
        :param name_object: Nome do Objeto
        :type: name_object: String
        :return: Retorna se o processo foi bem sucedido ou não
        :rtype: Bollean
        """
        s3 = self.connect_s3()
        try:
            s3.delete_object(
                Bucket=name_bucket,
                Key=name_object)
            return True
        except (TypeError, s3.exceptions.NoSuchBucket):
            return False

    def put_object(self, name_bucket: str, name_key_object: str, object_body: Union[str, ByteString, bytes]) -> bool:
        """
        Escreve um objeto no bucket.
        :param name_bucket: Nome do Bucket
        :type name_bucket: String
        :param name_key_object: Nome do Arquivo
        :type name_key_object: String
        :param object_body: Conteúdo
        :type object_body: String or ByteString or Bytes
        :return: Retorna se o processo foi bem sucedido ou não
        :rtype: Bollean
        """
        s3 = self.s3_resource()
        try:
            s3.Object(name_bucket, name_key_object).put(Body=object_body)
            return True
        except(TypeError, s3.exceptions.NoSuchBucket):
            return False

    def get_object(self, name_bucket: str, name_key_object: str) -> Union[bool, List]:
        """
        Lê um objeto e retorna o conteúdo dele
        :param name_bucket: Nome do Bucket
        :type name_bucket: String
        :param name_key_object: Nome do Arquivo
        :type name_key_object: String
        :return: Retorna se o processo foi bem sucedido ou não
        :rtype: List
        """
        s3 = self.s3_resource()
        try:
            response = s3.Object(name_bucket, name_key_object).get()
            objects = response['Body'].read().decode('ISO-8859-1').splitlines()
            list_files = []
            for file in objects:
                object_new = json.loads(file)
                list_files.append(object_new)
            return list_files
        except(TypeError, s3.exceptions.NoSuchBucket):
            return False

    def list_object(self, name_bucket: str) -> Union[bool, List[str]]:
        """
        Lista todos os objetos no bucket
        :param name_bucket: Nome do Bucket
        :type name_bucket: String
        :return:  Retorna uma lista com o nome dos objetos
        :rtype: List
        """
        s3 = self.s3_resource()
        try:
            bucket = s3.Bucket(name_bucket)
            list_objects = []
            for obj in bucket.objects.all():
                list_objects.append(obj.key)
            return list_objects
        except(TypeError, s3.exceptions.NoSuchBucket, s3.exceptions.ObjectSummary):
            return False
