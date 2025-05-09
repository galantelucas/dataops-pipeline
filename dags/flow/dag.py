from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from datetime import datetime

from scripts.bronze.get_years import GetYears
from scripts.bronze.download_files import DownloadFiles
from scripts.bronze.updates import CheckUpdates
from scripts.bronze.download_aux_files import DownloadAuxiliares

from tools.pipeline import Pipeline
import logging

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    "DAG_COMEX_ESTADOS",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    description="Pipeline para atualização dos dados de Comércio Exterior por Estado",
) as dag:

    end = EmptyOperator(task_id='end')

    def verificar_atualizacao():
        """Verifica se há atualização disponível na fonte."""
        logging.info("Iniciando verificação de atualização...")
        checker = CheckUpdates()
        if checker.precisa_atualizar:
            logging.info("Atualização necessária. Prosseguindo com o pipeline.")
            return 'obter_anos'
        else:
            logging.info("Banco de dados já atualizado. Finalizando DAG.")
            return 'end'

    @task(task_id='obter_anos')
    def obter_anos():
        """Obtém a lista de anos disponíveis na fonte de dados."""
        getter = GetYears()
        anos = getter.get_years(Pipeline.SOURCE_URL)
        logging.info(f"Ano(s) disponíveis encontrados: {anos}")
        return anos

    @task
    def baixar_exportacao(ano: int):
        """Baixa os arquivos de exportação do ano informado."""
        downloader = DownloadFiles()
        downloader.download_exportacao(ano)
        logging.info(f"Exportação de {ano} baixada com sucesso.")

    @task
    def baixar_importacao(ano: int):
        """Baixa os arquivos de importação do ano informado."""
        downloader = DownloadFiles()
        downloader.download_importacao(ano)
        logging.info(f"Importação de {ano} baixada com sucesso.")

    @task
    def download_auxiliares():
        """Realiza download das tabelas auxiliares."""
        download = DownloadAuxiliares()
        download.baixar_arquivos()
        logging.info("Tabelas auxiliares baixadas com sucesso.")

    branch_task = BranchPythonOperator(
        task_id='check_update',
        python_callable=verificar_atualizacao,
    )


    anos_task = obter_anos()
    exportacoes = baixar_exportacao.expand(ano=anos_task)
    importacoes = baixar_importacao.expand(ano=anos_task)
    download_aux_task = download_auxiliares()

    branch_task >> anos_task
    branch_task >> end

    anos_task >> exportacoes >> importacoes >> download_aux_task >> end
