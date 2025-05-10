import os
import json
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from tools.pipeline import Pipeline

class ImportacoesLoader:

    def __init__(self, parquet_folder=Pipeline.STORAGE_IMP, table_name=Pipeline.TABLE_F_IMPORTACOES_ESTADOS, json_path=Pipeline.JSON_PATH):
        self.parquet_folder = parquet_folder
        self.table_name = table_name
        self.db_config = self.load_db_config(json_path)
        self.engine = self.create_sqlalchemy_engine(self.db_config)

    def run(self):
        """Executa o pipeline completo: leitura, transformação e carga."""
        df = self.read_parquet_files()
        df_transformed = self.transform_data(df)
        self.load_to_postgres(df_transformed)

    def load_db_config(self, json_path):
        """Carrega as configurações de conexão do JSON."""
        try:
            with open(json_path, 'r') as file:
                data = json.load(file)
                return data[0]
        except Exception as e:
            raise Exception(f"Erro ao ler o arquivo de configuração: {e}")

    def create_sqlalchemy_engine(self, db_config):
        """Cria engine SQLAlchemy para o banco Postgres usando as configurações carregadas."""
        url = (
            f"postgresql+psycopg2://{db_config['login']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}/{db_config['schema']}"
        )
        return create_engine(url)

    def read_parquet_files(self):
        """Lê todos os arquivos .parquet da pasta e subpastas, e concatena em um DataFrame único."""
        all_files = []
        for root, dirs, files in os.walk(self.parquet_folder):
            for file in files:
                if file.endswith('.parquet'):
                    all_files.append(os.path.join(root, file))
        if not all_files:
            raise Exception("Nenhum arquivo .parquet encontrado na pasta.")

        df_list = []
        for file in all_files:
            print(f"Lendo arquivo: {file}")
            df = pd.read_parquet(file)
            df_list.append(df)

        combined_df = pd.concat(df_list, ignore_index=True)
        return combined_df

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
