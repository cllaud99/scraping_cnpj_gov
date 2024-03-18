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


    acrescenta_asterisco = f"*.{ final_arquivo }"
    path_empresas = os.path.join(pasta_origem, acrescenta_asterisco)



    query = f"""
        SELECT
            *
        FROM
            read_csv('{ path_empresas }', decimal_separator=",", columns = { tipos_das_colunas } , dateformat='%Y%m%d', ignore_errors=true)
    """


    #df_duckdb = duckdb.sql(query)

    #query_insert = f" COPY ( { query } ) TO '{nome_tabela}' (FORMAT PARQUET)"

    df_duckdb = duckdb.sql(query)

    #print(df_duckdb)

    #con.sql(query_insert)


    return df_duckdb

def cria_df_duck(pasta_origem: str):
    df_empresas = carrega_df_duck('dados/parquet/empresas.parquet',pasta_origem,"EMPRECSV", schemas.schema_empresa)
    df_estabelecimentos = carrega_df_duck('dados/parquet/estabelecimentos.parquet',pasta_origem,"ESTABELE", schemas.schema_estabelecimento)
    df_simples = carrega_df_duck('dados/parquet/simples.parquet',pasta_origem,"D40210", schemas.schema_simples)
    df_socios = carrega_df_duck('dados/parquet/socios.parquet', pasta_origem,"SOCIOCSV", schemas.schema_socios)
    df_paises = carrega_df_duck('dados/parquet/paises.parquet', pasta_origem,"PAISCSV", schemas.schema_paises)
    df_municipios = carrega_df_duck('dados/parquet/municipios.parquet.parquet', pasta_origem,"MUNICCSV", schemas.schema_municipios)
    df_qualificacoes_socios = carrega_df_duck('dados/parquet/qualificacoes_socios.parquet',pasta_origem,"QUALSCSV", schemas.schema_qualificacoes_socios)
    df_natureza_juridica = carrega_df_duck('dados/parquet/naturezas_juridicas.parquet',pasta_origem,"NATJUCSV", schemas.schema_naturezas_juridicas)
    df_cnaes = carrega_df_duck('dados/parquet/cnaes.parquet',pasta_origem,"CNAECSV", schemas.schema_cnaes)
    sql_novo = f"""
        SELECT
            *
        FROM
            { df_estabelecimentos } estabele
        LEFT JOIN
            {df_cnaes} cnaes ON
            cnaes.CODIGO = estabele.CNAE_FISCAL_PRINCIPAL
            """
    duckdb.sql(sql_novo).show()





if __name__ == "__main__":
    cria_df_duck('dados/utf8')
    
 
    

  