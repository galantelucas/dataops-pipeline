import os
import pandas as pd

# Caminho da pasta com os arquivos CSV
folder_path = "/opt/airflow/storage"

# Caminho onde o SQL será salvo
output_sql_path = "/opt/airflow/storage/table_script.sql"

# Mapeamento de tipos pandas → SQL
dtype_to_sql = {
    'int64': 'INT',
    'float64': 'DECIMAL(18,4)',
    'object': 'VARCHAR(255)',
    'bool': 'BOOLEAN',
    'datetime64[ns]': 'TIMESTAMP'
}

all_sql_scripts = []

for dirpath, dirnames, filenames in os.walk(folder_path):
    csv_files = [f for f in filenames if f.endswith('.csv')]

    for csv_file in csv_files:
        csv_path = os.path.join(dirpath, csv_file)

        try:
            # Lê apenas 1 linha de dados (header + 1 linha)
            df = pd.read_csv(csv_path, nrows=1, sep=';', encoding='ISO-8859-1')

            columns = df.columns.tolist()
            dtypes = df.dtypes.to_dict()
        except Exception as e:
            print(f"Erro ao ler {csv_file}: {e}")
            continue

        table_name = os.path.splitext(csv_file)[0]

        sql_columns = []
        for col in columns:
            dtype_name = str(dtypes[col])
            sql_type = dtype_to_sql.get(dtype_name, 'VARCHAR(255)')  # fallback padrão
            sql_columns.append(f"    {col} {sql_type}")

        sql = f"CREATE TABLE {table_name} (\n"
        sql += ",\n".join(sql_columns)
        sql += "\n);\n"

        all_sql_scripts.append(sql)

if all_sql_scripts:
    final_sql = "\n".join(all_sql_scripts)
    with open(output_sql_path, 'w', encoding='utf-8') as f:
        f.write(final_sql)
    print(f"Scripts SQL gerados e salvos em: {output_sql_path}")
else:
    print("Nenhum arquivo CSV encontrado em todas as pastas/subpastas.")