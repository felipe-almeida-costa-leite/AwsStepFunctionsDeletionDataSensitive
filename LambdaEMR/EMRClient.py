# !/usr/bin/python3
# encoding: iso-8859-1
"""
Este módulo executa os processos relevantes para EMR.
"""
# Imports
import boto3
from typing import List, Dict


class EMRClient(object):
    """
    Classe criada para instanciar o EMR
    """

    @staticmethod
    def connect_emr():
        """
        Cria uma conexão com o EMR
        :return: Retorna a conexão
        :rtype: Object EMR
        """
        emr = boto3.client('emr')
        return emr

    def get_cluster_id(self, cluster_name: str) -> str:
        """
        Retorna o id do cluster.
        :param cluster_name: Nome do Cluster EMR
        :type cluster_name: String
        :return: ID Cluster EMR
        :rtype: String
        """
        emr = self.connect_emr()
        clusters: List[Dict[str, str]] = emr.list_clusters()['Clusters']
        for cluster in clusters:
            if cluster['Name'] == cluster_name:
                return cluster['Id']
