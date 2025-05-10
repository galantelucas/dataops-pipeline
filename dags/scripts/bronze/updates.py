import requests
import psycopg2
import json
from datetime import datetime
import os
from bs4 import BeautifulSoup
import sys

from tools.pipeline import Pipeline

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from tools.pipeline import Pipeline

class CheckUpdates:
    """Realiza comparação de atualizações entre a fonte e o banco local."""
    def __init__(self):
        self.db_config = self.load_db_config()
        self.source_last_update = self.get_updates()
        self.db_last_update = self.get_db_last_update(self.db_config)
        self.precisa_atualizar = self.compara_datas(self.source_last_update, self.db_last_update)

    def get_updates(self):
        """Obtém a última atualização da fonte de dados."""
        try:
            print("Localizando última atualização da fonte")
            response = requests.get(Pipeline.SOURCE_URL)
            site = BeautifulSoup(response.text, 'lxml')
            source_last_update = site.find('span', {'class': 'documentModified'}).find(
                'span', {'class': 'value'}).text.replace('h', ':')

            print(source_last_update)
            return source_last_update
        except Exception as e:
            raise Exception(f"Erro ao localizar atualização: {e}")

    def load_db_config(self, json_path=Pipeline.JSON_PATH):
        """Carrega as configurações de conexão do JSON."""
        try:
            with open(json_path, 'r') as file:
                data = json.load(file)
                return data[0]
        except Exception as e:
            raise Exception(f"Erro ao ler o arquivo de configuração: {e}")

    def get_db_last_update(self, db_config):
        """Retorna a última data de atualização da tabela no banco de dados."""

        TABLE_NAME = 'logs.pipeline_execucoes'
        COLUMN = 'ultima_atualizacao'

        try:
            print("Acessando banco de dados para localizar última atualização local")
            conn = psycopg2.connect(
                dbname=db_config['schema'],
                user=db_config['login'],
                password=db_config['password'],
                host=db_config['host'],
                port=db_config['port']
            )
            cursor = conn.cursor()

            query = f"SELECT MAX({COLUMN}) FROM {TABLE_NAME} WHERE CAMADA = 'SILVER';"
            cursor.execute(query)

            result = cursor.fetchone()
            db_last_update = result[0]  # datetime ou None

            cursor.close()
            conn.close()

            return db_last_update
        except Exception as e:
            raise Exception(f"Erro ao acessar o banco de dados: {e}")

    def compara_datas(self, source_last_update_str, db_last_update):
        """Compara a data da web com a data do banco."""
        source_last_update = datetime.strptime(source_last_update_str, '%d/%m/%Y %H:%M')  # ajuste o formato

        if db_last_update is None:
            print("Banco não tem data de atualização registrada.")
            return True  # Precisa atualizar

        print(f"Data do banco: {db_last_update}")
        print(f"Data da fonte: {source_last_update}")

        if source_last_update > db_last_update:
            print("Há uma atualização mais recente na fonte.")
            return True
        else:
            print("O banco já está atualizado.")
            return False