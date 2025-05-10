import os
import requests
import time
import pandas as pd
from tools.pipeline import Pipeline


class DownloadFiles:
    """Realiza download dos arquivos, converte para Parquet e remove o CSV"""

    def _download_file(self, url_template: str, storage: str, ano: int, max_retries: int = 3):
        """Método genérico para download com tentativas de retry e conversão para Parquet."""
        os.makedirs(storage, exist_ok=True)
        file_url = url_template.format(ano=ano)
        base_filename = os.path.splitext(os.path.basename(file_url))[0]  # sem extensão
        csv_path = os.path.join(storage, f"{base_filename}.csv")

        for attempt in range(1, max_retries + 1):
            try:
                print(f"[Tentativa {attempt}/{max_retries}] Baixando {file_url} ...")
                response = requests.get(file_url, timeout=60)
                response.raise_for_status()

                # Salva arquivo CSV inicial
                with open(csv_path, "wb") as f:
                    f.write(response.content)

                print(f"Arquivo CSV salvo em {csv_path}.")

                # ---- Conversão para Parquet ----
                try:
                    df = pd.read_csv(csv_path, sep=';', encoding='ISO-8859-1')

                    # Define diretório final (ex: storage/bronze/COMEX_EXPORTACAO/exp_2024)
                    parquet_dir = os.path.join(storage, base_filename)
                    os.makedirs(parquet_dir, exist_ok=True)

                    # Define nome parquet (ex: exp_2024.parquet)
                    parquet_filename = f"{base_filename}.parquet"
                    parquet_path = os.path.join(parquet_dir, parquet_filename)

                    # Verifica se o arquivo Parquet já existe e remove se necessário
                    if os.path.exists(parquet_path):
                        print(f"Arquivo Parquet {parquet_path} já existe. Sobrescrevendo...")

                        # Remove o arquivo Parquet existente
                        os.remove(parquet_path)

                    # Converte para Parquet
                    df.to_parquet(parquet_path, index=False)

                    print(f"Arquivo Parquet salvo em {parquet_path}.")

                    # Apaga CSV original
                    os.remove(csv_path)
                    print(f"Arquivo CSV {csv_path} removido após conversão.")

                except Exception as conv_err:
                    print(f"Erro ao converter {csv_path} para Parquet: {conv_err}")

                return  # sucesso

            except Exception as e:
                print(f"Erro ao baixar {file_url}: {e}")
                if attempt < max_retries:
                    print("Tentando novamente em 5 segundos...")
                    time.sleep(5)
                else:
                    print("Número máximo de tentativas atingido. Falhando...")
                    raise  # relança exceção

    def download_exportacao(self, ano: int):
        """Baixa o arquivo de exportação e converte para Parquet"""
        url_template = Pipeline.EXPORTACAO_ESTADOS_URL_DOWNLOAD
        storage = Pipeline.STORAGE_EXP
        self._download_file(url_template, storage, ano)

    def download_importacao(self, ano: int):
        """Baixa o arquivo de importação e converte para Parquet"""
        url_template = Pipeline.IMPORTACAO_ESTADOS_URL_DOWNLOAD
        storage = Pipeline.STORAGE_IMP
        self._download_file(url_template, storage, ano)
