import os
import json
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# Defina as constantes diretamente aqui para facilitar a execução
class Pipeline:
    STORAGE_EXP = "/opt/airflow/storage/bronze/COMEX_EXPORTACAO"  # Substitua pelo caminho real da pasta de arquivos Parquet
    TABLE_F_EXPORTACOES_ESTADOS = "silver.F_EXPORTACOES_ESTADOS" # Substitua pelo nome da sua tabela
    JSON_PATH = "/opt/airflow/dags/tools/connections.json" # Substitua pelo caminho real do seu arquivo de configuração JSON

class ExportacoesLoader:

    def __init__(self, parquet_folder=Pipeline.STORAGE_EXP, table_name=Pipeline.TABLE_F_EXPORTACOES_ESTADOS, json_path=Pipeline.JSON_PATH):
        self.parquet_folder = parquet_folder
        self.table_name = table_name
        self.json_path = json_path
        self.db_config = self.load_db_config()
        self.engine = self.create_sqlalchemy_engine()
        self._create_parquet_folder_if_not_exists() # Garante que a pasta de parquet existe

    def _create_parquet_folder_if_not_exists(self):
        """Cria a pasta de arquivos Parquet se ela não existir."""
        if not os.path.exists(self.parquet_folder):
            os.makedirs(self.parquet_folder)
            print(f"Pasta '{self.parquet_folder}' criada.")

    def run(self):
        """Executa o pipeline completo: leitura, transformação e carga."""
        df = self.read_parquet_files()
        if df is not None and not df.empty:
            df_transformed = self.transform_data(df)
            self.load_to_postgres(df_transformed)
        else:
            print("Nenhum dado para processar.")

    def load_db_config(self):
        """Carrega as configurações de conexão do JSON."""
        try:
            with open(self.json_path, 'r') as file:
                data = json.load(file)
                return data[0]
        except FileNotFoundError:
            raise FileNotFoundError(f"Arquivo de configuração não encontrado em: {self.json_path}")
        except json.JSONDecodeError:
            raise json.JSONDecodeError(f"Erro ao decodificar o JSON no arquivo: {self.json_path}")
        except Exception as e:
            raise Exception(f"Erro ao ler o arquivo de configuração: {e}")

    def create_sqlalchemy_engine(self):
        """Cria engine SQLAlchemy para o banco Postgres usando as configurações carregadas."""
        db_config = self.db_config
        url = (
            f"postgresql+psycopg2://{db_config['login']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}/{db_config['schema']}"
        )
        return create_engine(url)

    def read_parquet_files(self):
        """Lê todos os arquivos .parquet da pasta e concatena em um DataFrame único."""
        all_files = [
            os.path.join(self.parquet_folder, f)
            for f in os.listdir(self.parquet_folder)
            if f.endswith('.parquet')
        ]
        if not all_files:
            print(f"Aviso: Nenhum arquivo .parquet encontrado na pasta: {self.parquet_folder}")
            return None

        df_list = []
        for file in all_files:
            print(f"Lendo arquivo: {file}")
            try:
                df = pd.read_parquet(file)
                df_list.append(df)
            except Exception as e:
                print(f"Erro ao ler o arquivo '{file}': {e}")

        if df_list:
            combined_df = pd.concat(df_list, ignore_index=True)
            return combined_df
        else:
            return None

    def transform_data(self, df):
        """Aplica transformações nos dados antes de carregar no banco."""
        df.columns = [col.lower() for col in df.columns]
        df['data_carga'] = datetime.now()
        return df

    def load_to_postgres(self, df):
        """Carrega o DataFrame no banco de dados Postgres."""
        try:
            df.to_sql(self.table_name, self.engine, if_exists='append', index=False)
            print(f"Dados carregados com sucesso na tabela {self.table_name}")
        except Exception as e:
            raise Exception(f"Erro ao carregar os dados no Postgres: {e}")

if __name__ == "__main__":

    loader = ExportacoesLoader()
    loader.run()
    print("Processo de carregamento concluído.")