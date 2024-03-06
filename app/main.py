import os
import polars as pl
import schemas
import duckdb

diretorio = 'dados/raw'

print(diretorio)

def ler_arquivos_polars(final_arquivo, diretorio, headers) -> pl.DataFrame:
    """
    Lê os arquivos com o final especificado no diretório fornecido e retorna um DataFrame Polars.
    
    Args:
        final_arquivo (str): O final do nome do arquivo que se deseja ler.
        diretorio (str): O diretório onde os arquivos estão localizados.
        headers (str): os cabeçalhos que o DataFrame Polars deve ter
    Returns:
        DataFrame Polars: Um DataFrame contendo todos os arquivos com o final especificado.
    """
    dataframes = []

    # Verifica cada arquivo na pasta
    for arquivo in os.listdir(diretorio):
        if arquivo.endswith(final_arquivo):

            caminho_arquivo = os.path.join(diretorio, arquivo)

            df = pl.read_csv(caminho_arquivo, encoding='latin1', separator=';', new_columns=headers, ignore_errors=True, infer_schema_length=0)
            #with pl.Config(tbl_cols=-1):
            #    print(df)

            dataframes.append(df)
 
    df_final = pl.concat(dataframes)

    return df_final



schema_empresa = schemas.schema_empresa
final_empresas = '.EMPRECSV'
headers_empresa = list(schema_empresa.columns.keys())

#df_empresas= ler_arquivos_polars(final_empresas, diretorio, headers_empresa)
query = (
   f"""
        SELECT
            *
        FROM
            read_csv('{diretorio}/*{final_empresas}',columns={schema_empresa})
"""
)
print(query)
duckdb.sql(query).show()



#df_empresas = df_empresas.with_columns(pl.col('CAPITAL SOCIAL DA EMPRESA').str.replace(',','.'))
#df_empresas = df_empresas.with_columns(pl.col('CAPITAL SOCIAL DA EMPRESA').cast(pl.Float64))
#df_empresas = df_empresas.with_columns(pl.col('CNPJ BÁSICO','NATUREZA JURÍDICA','QUALIFICAÇÃO DO RESPONSÁVEL','PORTE DA EMPRESA').cast(pl.String))
#print(df_empresas)


#schema_estabelecimento = schemas.schema_estabelecimento
#final_estabelecimento = '.ESTABELE'
#headers_estabelecimento = list(schema_estabelecimento.columns.keys())


#df_estabelecimentos = ler_arquivos_polars(final_estabelecimento, diretorio, headers_estabelecimento)

# Exibe as informações do DataFrame final
# print("DataFrame Final:")
# print(df_estabelecimentos)
#print(df_empresas)
