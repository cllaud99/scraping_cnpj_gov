import os

import duckdb

path = 'dados/raw/'
parquet_empresas = 'dados/parquet/empresas.parquet'
arquivos_empresas = []

if not os.path.exists('dados/parquet'):
    os.makedirs('dados/parquet')



def separa_arquivos(final_arquivo: str) -> list:
    lista_arquivos = []
    for arquivo in os.listdir(path):
        if(arquivo.endswith(final_arquivo)):
            full_path_empresas = os.path.join(path,arquivo)
            lista_arquivos.append(full_path_empresas)
    print(arquivos_empresas)
    return lista_arquivos


final_arq_empresas = '.EMPRECSV'
arquivos_empresas = separa_arquivos(final_arq_empresas)
final_arq_estabelecimentos = '.ESTABELE'
arquivos_estabelecimentos = separa_arquivos(final_arq_estabelecimentos)


tabela_estabelecimentos = f"""
    {arquivos_estabelecimentos}, AUTO_DETECT=FALSE, sep=";",columns={{
    CNPJ_BASICO: VARCHAR,
    CNPJ_ORDEM: VARCHAR,
    CNPJ_DV: VARCHAR,
    IDENTIFICADOR_MATRIZ_FILIAL: VARCHAR,
    NOME_FANTASIA: VARCHAR,
    SITUACAO_CADASTRAL: VARCHAR,
    DATA_SITUACAO_CADASTRAL: VARCHAR,
    MOTIVO_SITUACAO_CADASTRAL: VARCHAR,
    NOME_DA_CIDADE_NO_EXTERIOR: VARCHAR,
    PAIS: VARCHAR,
    DATA_DE_INICIO_ATIVIDADE: VARCHAR,
    CNAE_FISCAL_PRINCIPAL: VARCHAR,
    CNAE_FISCAL_SECUNDÁRIA: VARCHAR,
    TIPO_DE_LOGRADOURO: VARCHAR,
    LOGRADOURO: VARCHAR,
    NUMERO: VARCHAR,
    COMPLEMENTO: VARCHAR,
    BAIRRO: VARCHAR,
    CEP: VARCHAR,
    UF: VARCHAR,
    MUNICIPIO: VARCHAR,
    DDD_1: VARCHAR,
    TELEFONE_1: VARCHAR,
    DDD_2: VARCHAR,
    TELEFONE_2: VARCHAR,
    DDD_DO_FAX: VARCHAR,
    FAX: VARCHAR,
    CORREIO_ELETRONICO: VARCHAR,
    SITUACAO_ESPECIAL: VARCHAR,
    DATA_DA_SITUACAO_ESPECIAL: VARCHAR
    }}
"""



tabela_empresas = f"""
            {arquivos_empresas}, AUTO_DETECT=FALSE, sep=";", columns={{
            cnpj_basico: VARCHAR, razao_social: VARCHAR, natureza_juridica: INT, qualificacao_responsavel: VARCHAR,
            capital_social: VARCHAR, porte_empresa: INT, ente_federativo: VARCHAR}}
"""


#print(tabela_estabelecimentos)


query_test = f"""
                SELECT
                   *
                FROM
                 read_csv({tabela_estabelecimentos})
    """   






query = f"""
        SELECT
            cnpj_basico,
            razao_social,
            natureza_juridica,
            qualificacao_responsavel,
            CAST(REPLACE(capital_social, ',', '.') AS FLOAT) AS capital_social,
            porte_empresa
        FROM
            read_csv({tabela_empresas})
    """

query_to_parquet = f"""
COPY ({query})
TO '{parquet_empresas}'
(FORMAT PARQUET);
"""

#print(query_to_parquet)

#duckdb.sql(query).show()
duckdb.sql(query_test).show()
#duckdb.sql(query_to_parquet)