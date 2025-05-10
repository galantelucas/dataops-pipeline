class Pipeline(object):
    """Centraliza valores constantes de URLs e base de dados da pipeline atual.
    """

    # URL
    SOURCE_URL = 'https://www.gov.br/produtividade-e-comercio-exterior/pt-br/assuntos/comercio-exterior/estatisticas/base-de-dados-bruta'
    BASE_URL = 'https://balanca.economia.gov.br/balanca/bd'

    # Pastas de download dos arquivos
    STORAGE = '/opt/airflow/storage/bronze/'
    STORAGE_IMP = STORAGE + 'COMEX_IMPORTACAO'
    STORAGE_EXP = STORAGE + 'COMEX_EXPORTACAO'
    STORAGE_AUX = STORAGE + 'COMEX_AUXILIARES'

    # Arquivos a serem baixados
    EXPORTACAO_ESTADOS_URL_DOWNLOAD = BASE_URL + \
        "/comexstat-bd/ncm/EXP_{ano}.csv"
    IMPORTACAO_ESTADOS_URL_DOWNLOAD = BASE_URL + \
        "/comexstat-bd/ncm/IMP_{ano}.csv"

    # Arquivos auxiliares
    AUX_URL_DOWNLOAD = [
        {"NAME": "PAIS", "URL": "https://balanca.economia.gov.br/balanca/bd/tabelas/PAIS.csv"},
        {"NAME": "URF", "URL": "https://balanca.economia.gov.br/balanca/bd/tabelas/URF.csv"},
        {"NAME": "VIA", "URL": "https://balanca.economia.gov.br/balanca/bd/tabelas/VIA.csv"},
        {"NAME": "NCM", "URL": "https://balanca.economia.gov.br/balanca/bd/tabelas/NCM.csv"},
        {"NAME": "NCM_SH", "URL": "https://balanca.economia.gov.br/balanca/bd/tabelas/NCM_SH.csv"},
        {"NAME": "UNIDADE", "URL": "https://balanca.economia.gov.br/balanca/bd/tabelas/NCM_UNIDADE.csv"},
        {"NAME": "UF", "URL": "https://balanca.economia.gov.br/balanca/bd/tabelas/UF.csv"},
        {"NAME": "BLOCOS", "URL": "https://balanca.economia.gov.br/balanca/bd/tabelas/PAIS_BLOCO.csv"},
        {"NAME": "NCM_ISIC",
            "URL": "https://balanca.economia.gov.br/balanca/bd/tabelas/NCM_ISIC.csv"}
    ]

    # Scripts de criaçao das tabelas
    SQL_FILE_PATH = '/opt/airflow/dags/tools/table_scripts/tabelas_geradas.sql'

    # Conexao ao banco de dados
    JSON_PATH= 'dags/tools/connections.json'

    TABLE_D_BLOCOS = 'silver.D_BLOCOS'
    TABLE_D_NCM = 'silver.D_NCM'
    TABLE_D_NCM_ISIC = 'silver.D_NCM_ISIC'
    TABLE_D_NCM_SH = 'silver.D_NCM_SH'
    TABLE_D_PAIS = 'silver.D_PAIS'
    TABLE_D_UF = 'silver.D_UF'
    TABLE_D_UNIDADE = 'silver.D_UNIDADE'
    TABLE_D_URF = 'silver.D_URF'
    TABLE_D_VIA = 'silver.D_VIA'
    TABLE_F_EXPORTACOES_ESTADOS = 'silver.F_EXPORTACOES_ESTADOS'
    TABLE_F_IMPORTACOES_ESTADOS = 'silver.F_IMPORTACOES_ESTADOS'