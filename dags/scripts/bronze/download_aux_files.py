import requests
import os
from tools.pipeline import Pipeline


class DownloadAuxiliares:
    """ Realiza download das tabelas auxiliares"""

    def baixar_arquivos(self):
        for item in Pipeline.AUX_URL_DOWNLOAD:
            nome_arquivo = item["NAME"]
            url = item["URL"]
            storage = Pipeline.STORAGE_AUX
            os.makedirs(storage, exist_ok=True)

            # Garante que o nome do arquivo tenha extensão .csv
            base_name = os.path.basename(nome_arquivo)
            if not base_name.endswith(".csv"):
                base_name += ".csv"

            filename = os.path.join(storage, base_name)

            try:
                response = requests.get(url)
                response.raise_for_status()  # Verifica se a requisição foi bem-sucedida

                # Salvar o arquivo com a extensão correta
                with open(filename, 'wb') as file:
                    file.write(response.content)

                print(f"Arquivo {filename} baixado com sucesso.")

            except requests.exceptions.RequestException as e:
                print(f"Erro ao baixar {filename}: {e}")
