import os
import pandera as pa
import duckdb
import schemas
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

con = duckdb.connect(database='duckdb.db', read_only=False)

def carrega_df_duck (nome_tabela: str, pasta_origem: str, final_arquivo: str, schema: pa.SchemaModel):
    
    """
        Função que le todos arquivos de uma pasta que terminem com uma extensão especifica e carrega
        para um dataframe duckdb
        Args:
            nome_tabela (str) Nome da tabela que será criada
            pasta_origem (str) Pasta onde os arquivos estão
            final_arquivo(str) Final do arquivo desejado
            schema (pa.SchemaModel) Esquema da tabela
    """

    tipo_substituicao = {
    'str': 'varchar',
    'float64': 'double',
    'datetime64[ns]': 'DATE'
    }

    tipos_das_colunas = {col: str(schema.columns[col].dtype).replace("DataType(", "").replace(")", "") for col in schema.columns}
    tipos_das_colunas = {col: tipo_substituicao.get(tipo, tipo) for col, tipo in tipos_das_colunas.items()}


    print(tipos_das_colunas)

    acrescenta_asterisco = f"*.{ final_arquivo }"
    path_empresas = os.path.join(pasta_origem, acrescenta_asterisco)



    query = f"""
        SELECT
            *
        FROM
            read_csv('{ path_empresas }', decimal_separator=",", columns = { tipos_das_colunas } , dateformat='%Y%m%d', ignore_errors=true)
    """


    df_duckdb = duckdb.sql(query)

    query_insert = f" INSERT INTO { nome_tabela } { query }"
    print(query_insert)

    con.sql(query_insert)


    return df_duckdb

def cria_df_duck(pasta_origem: str):
    df_empresas = carrega_df_duck('empresas',pasta_origem,"EMPRECSV", schemas.schema_empresa)
    df_estabelecimentos = carrega_df_duck('estabelecimentos',pasta_origem,"ESTABELE", schemas.schema_estabelecimento)
    df_simples = carrega_df_duck('simples',pasta_origem,"D40210", schemas.schema_simples)
    df_socios = carrega_df_duck('socios', pasta_origem,"SOCIOCSV", schemas.schema_socios)
    df_paises = carrega_df_duck('paises', pasta_origem,"PAISCSV", schemas.schema_paises)
    df_municipios = carrega_df_duck('municipios', pasta_origem,"MUNICCSV", schemas.schema_municipios)
    df_qualificacoes_socios = carrega_df_duck('qualificacoes_socios',pasta_origem,"QUALSCSV", schemas.schema_qualificacoes_socios)
    df_natureza_juridica = carrega_df_duck('naturezas_juridicas',pasta_origem,"NATJUCSV", schemas.schema_naturezas_juridicas)
    df_cnaes = carrega_df_duck('cnaes',pasta_origem,"CNAECSV", schemas.schema_cnaes)



if __name__ == "__main__":
    cria_df_duck('dados/utf8')

  