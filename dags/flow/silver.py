from airflow.utils.task_group import TaskGroup
from scripts.silver.fato_exportacoes import ExportacoesLoader
from scripts.silver.fato_importacoes import ImportacoesLoader
from scripts.silver.dim_auxiliares import AuxLoader

from airflow.decorators import task

import logging

@task
def exportacoes():
    """Realiza transform and load de exportacoes"""
    exportacoes = ExportacoesLoader()
    exportacoes.run()
    logging.info("Tabela exportacao inserida com sucesso.")

@task
def importacoes():
    """Realiza transform and load de importacoes"""
    exportacoes = ImportacoesLoader()
    exportacoes.run()
    logging.info("Tabela importacao inserida com sucesso.")

@task
def auxiliares():
    """Realiza download das tabelas auxiliares."""
    aux_loader = AuxLoader()
    aux_loader.run(replace_table=True)
    logging.info("Tabelas auxiliares inseridas com sucesso.")


def camada_silver():
    with TaskGroup("silver", tooltip="Grupo de Tarefas Silver") as silver_group:

        exportacao = exportacoes()
        importacao = importacoes()
        auxiliar = auxiliares()

        auxiliar >> exportacao  >> importacao

    return silver_group