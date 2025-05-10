import json
import psycopg2
import os
import re
from tools.pipeline import Pipeline

class DatabaseInitializer:
    """Verifica se as tabelas existem no banco de dados"""

    def __init__(self,  json_path=Pipeline.JSON_PATH):
        """Inicializa a classe carregando as configurações do banco."""
        self.db_config = self.load_db_config(json_path)
        self.conn = psycopg2.connect(
            dbname=self.db_config['schema'],
            user=self.db_config['login'],
            password=self.db_config['password'],
            host=self.db_config['host'],
            port=self.db_config['port']
        )
        self.conn.autocommit = True  # garante execução de DDL sem commit explícito
        self.cursor = self.conn.cursor()

    def load_db_config(self, json_path):
        """Carrega as configurações de conexão do JSON."""
        try:
            with open(json_path, 'r') as file:
                data = json.load(file)
                return data[0]
        except Exception as e:
            raise Exception(f"Erro ao ler o arquivo de configuração: {e}")

    def table_exists(self, schema_name, table_name):
        """Verifica se uma tabela existe no schema."""
        query = """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            );
        """
        self.cursor.execute(query, (schema_name, table_name))
        exists = self.cursor.fetchone()[0]
        return exists

    def initialize_tables(self, sql_file_path):
        """Cria as tabelas usando CREATE TABLE IF NOT EXISTS."""
        if not os.path.exists(sql_file_path):
            raise FileNotFoundError(f"Arquivo SQL não encontrado: {sql_file_path}")

        with open(sql_file_path, 'r') as f:
            sql_content = f.read()

        sql_content = re.sub(
            r'(?i)\bcreate\s+table\s+(\w+)\.(\w+)',
            r'CREATE TABLE IF NOT EXISTS \1.\2',
            sql_content
        )

        print("Executando script SQL com IF NOT EXISTS...")
        self.cursor.execute(sql_content)
        print("Script executado com sucesso.")

    def close(self):
        """Fecha a conexão com o banco."""
        self.cursor.close()
        self.conn.close()
