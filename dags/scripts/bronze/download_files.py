import os
import requests
import time
from tools.pipeline import Pipeline


class DownloadFiles:
    """Realiza download dos arquivos"""

    def _download_file(self, url_template: str, storage: str, ano: int, max_retries: int = 3):
        """Método genérico para download com tentativas de retry."""
        os.makedirs(storage, exist_ok=True)
        file_url = url_template.format(ano=ano)
        filename = os.path.join(storage, os.path.basename(file_url))

        for attempt in range(1, max_retries + 1):
            try:
                print(
                    f"[Tentativa {attempt}/{max_retries}] Baixando {file_url} ...")
                response = requests.get(file_url, timeout=60)
                response.raise_for_status()

                with open(filename, "wb") as f:
                    f.write(response.content)

                print(f"Arquivo salvo em {filename}.")
                return  # sucesso: sai da função

            except Exception as e:
                print(f"Erro ao baixar {file_url}: {e}")
                if attempt < max_retries:
                    print("Tentando novamente em 5 segundos...")
                    time.sleep(5)  # aguarda antes de tentar novamente
                else:
                    print("Número máximo de tentativas atingido. Falhando...")
                    raise  # relança a exceção para a task falhar no Airflow

    def download_exportacao(self, ano: int):
        """Baixa o arquivo de exportação para o ano especificado."""
        url_template = Pipeline.EXPORTACAO_ESTADOS_URL_DOWNLOAD
        storage = Pipeline.STORAGE_EXP
        self._download_file(url_template, storage, ano)

    def download_importacao(self, ano: int):
        """Baixa o arquivo de importação para o ano especificado."""
        url_template = Pipeline.IMPORTACAO_ESTADOS_URL_DOWNLOAD
        storage = Pipeline.STORAGE_IMP
        self._download_file(url_template, storage, ano)
