import duckdb
import os
import pandas as pd
import pandera as pa
import schemas


def carrega_df_duck(nome_tabela: str, pasta_origem: str, final_arquivo: str, schema: pa.SchemaModel, con: duckdb.DuckDBPyConnection):
    """
    Função que lê todos os arquivos de uma pasta que terminam com uma extensão específica e carrega para um dataframe DuckDB
    Args:
        nome_tabela (str): Nome da tabela que será criada
        pasta_origem (str): Pasta onde os arquivos estão
        final_arquivo (str): Final do arquivo desejado
        schema (pa.SchemaModel): Esquema da tabela
        con (duckdb.DuckDBPyConnection): Conexão ao banco de dados DuckDB
    """

    tipo_substituicao = {
        'str': 'varchar',
        'float64': 'double',
        'datetime64[ns]': 'DATE'
    }

    tipos_das_colunas = {col: str(schema.columns[col].dtype).replace("DataType(", "").replace(")", "") for col in schema.columns}
    tipos_das_colunas = {col: tipo_substituicao.get(tipo, tipo) for col, tipo in tipos_das_colunas.items()}

    print(tipos_das_colunas)

    acrescenta_asterisco = f"*.{final_arquivo}"
    path_empresas = os.path.join(pasta_origem, acrescenta_asterisco)

    query = f"""
        CREATE TABLE IF NOT EXISTS {nome_tabela} AS
        SELECT *
        FROM read_csv('{path_empresas}', decimal_separator=",", columns={tipos_das_colunas}, dateformat='%Y%m%d', ignore_errors=true)
    """

    print(query)

    con.execute(query)

def cria_df_duck(pasta_origem: str):
    con = duckdb.connect(database=':memory:', read_only=False)

    df_empresas = carrega_df_duck('empresas', pasta_origem, "EMPRECSV", schemas.schema_empresa, con)
    df_estabelecimentos = carrega_df_duck('estabelecimentos', pasta_origem, "ESTABELE", schemas.schema_estabelecimento, con)
    df_simples = carrega_df_duck('simples', pasta_origem, "D40210", schemas.schema_simples, con)
    df_socios = carrega_df_duck('socios', pasta_origem, "SOCIOCSV", schemas.schema_socios, con)
    df_paises = carrega_df_duck('paises', pasta_origem, "PAISCSV", schemas.schema_paises, con)
    df_municipios = carrega_df_duck('municipios', pasta_origem, "MUNICCSV", schemas.schema_municipios, con)
    df_qualificacoes_socios = carrega_df_duck('qualificacoes_socio', pasta_origem, "QUALSCSV", schemas.schema_qualificacoes_socios, con)
    df_natureza_juridica = carrega_df_duck('natureza_juridica', pasta_origem, "NATJUCSV", schemas.schema_naturezas_juridicas, con)
    df_cnaes = carrega_df_duck('cnaes', pasta_origem, "CNAECSV", schemas.schema_cnaes, con)

    dicionario_dataframes = {
        'empresa': df_empresas,
        'estabelecimentos': df_estabelecimentos,
        'simples': df_simples,
        'socios': df_socios,
        'paises': df_paises,
        'municipios': df_municipios,
        'qualificacoes_socios': df_qualificacoes_socios,
        'natureza_juridica': df_natureza_juridica,
        'cnaes': df_cnaes
    }

    return dicionario_dataframes


if __name__ == "__main__":
    cria_df_duck('dados/utf8')
