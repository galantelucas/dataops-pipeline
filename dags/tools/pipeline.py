class Pipeline(object):
    """Centraliza valores constantes de URLs e base de dados da pipeline atual.
    """

    # URL
    SOURCE_URL = 'https://www.gov.br/produtividade-e-comercio-exterior/pt-br/assuntos/comercio-exterior/estatisticas/base-de-dados-bruta'
    BASE_URL = 'https://balanca.economia.gov.br/balanca/bd'

    # Pastas de download dos arquivos
    STORAGE = '/opt/airflow/storage/'
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
