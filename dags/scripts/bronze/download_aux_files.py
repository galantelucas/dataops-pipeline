import requests
import os
import pandas as pd
from tools.pipeline import Pipeline


class DownloadAuxiliares:
    """ Realiza download das tabelas auxiliares, converte para Parquet e remove o CSV original """

    def baixar_arquivos(self):
        for item in Pipeline.AUX_URL_DOWNLOAD:
            nome_arquivo = item["NAME"]
            url = item["URL"]
            bronze_base = Pipeline.STORAGE_AUX  # storage/bronze/COMEX_AUXILIARES
            os.makedirs(bronze_base, exist_ok=True)

            # Garante que o nome do arquivo tenha extensão .csv
            base_name = os.path.basename(nome_arquivo)
            if not base_name.endswith(".csv"):
                base_name += ".csv"

            csv_temp_path = os.path.join(bronze_base, base_name)

            try:
                # Download do arquivo CSV temporário
                response = requests.get(url)
                response.raise_for_status()

                with open(csv_temp_path, 'wb') as file:
                    file.write(response.content)

                print(f"Arquivo {csv_temp_path} baixado com sucesso.")

                # --- Conversão para Parquet ---
                try:
                    df = pd.read_csv(csv_temp_path, sep=';', encoding='ISO-8859-1')

                    # Nome base sem extensão
                    parquet_base = os.path.splitext(base_name)[0]

                    # Define diretório final: storage/bronze/COMEX_AUXILIARES/nome_arquivo/
                    parquet_dir = os.path.join(bronze_base, parquet_base)
                    os.makedirs(parquet_dir, exist_ok=True)

                    # Define arquivo final: nome_arquivo_nome_arquivo.parquet
                    parquet_filename = f"{parquet_base}.parquet"
                    parquet_path = os.path.join(parquet_dir, parquet_filename)

                    # Verifica se o arquivo Parquet já existe e remove se necessário
                    if os.path.exists(parquet_path):
                        print(f"Arquivo Parquet {parquet_path} já existe. Sobrescrevendo...")

                        # Remove o arquivo Parquet existente
                        os.remove(parquet_path)

                    # Converte para Parquet
                    df.to_parquet(parquet_path, index=False)

                    print(f"Arquivo {parquet_path} salvo com sucesso em formato Parquet.")

                    # Remove o CSV temporário
                    os.remove(csv_temp_path)
                    print(f"Arquivo CSV {csv_temp_path} removido após conversão.")

                except Exception as e:
                    print(f"Erro ao converter {csv_temp_path} para Parquet: {e}")

            except requests.exceptions.RequestException as e:
                print(f"Erro ao baixar {csv_temp_path}: {e}")
