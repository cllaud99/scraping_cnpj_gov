import os
import pandera as pa
import duckdb
import schemas
import pandas as pd



def carrega_df_duck (pasta_origem: str, final_arquivo: str, schema: pa.SchemaModel):
    
    """
        Função que le todos arquivos de uma pasta que terminem com uma extensão especifica e carrega
        para um dataframe duckdb
        Args:
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

    print(query)
    
    df_duckdb = duckdb.sql(query)
    return df_duckdb




if __name__ == "__main__":

    df_empresas = carrega_df_duck("dados/utf8/","EMPRECSV", schemas.schema_empresa)
    df_estabelecimentos = carrega_df_duck("dados/utf8/","ESTABELE", schemas.schema_estabelecimento)