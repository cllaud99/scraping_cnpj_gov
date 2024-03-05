import os
import polars as pl
import schemas

diretorio = 'dados/raw'

def ler_arquivos_polars(final_arquivo, diretorio) -> pl.DataFrame:
    """
    Lê os arquivos com o final especificado no diretório fornecido e retorna um DataFrame Polars.
    
    Args:
        final_arquivo (str): O final do nome do arquivo que se deseja ler.
        diretorio (str): O diretório onde os arquivos estão localizados.

    Returns:
        DataFrame Polars: Um DataFrame contendo todos os arquivos com o final especificado.
    """
    dataframes = []

    # Verifica cada arquivo na pasta
    for arquivo in os.listdir(diretorio):
        if arquivo.endswith(final_arquivo):

            caminho_arquivo = os.path.join(diretorio, arquivo)

            df = pl.read_csv(caminho_arquivo, encoding='latin1', has_header=False, separator=';')

            dataframes.append(df)
 
    df_final = pl.concat(dataframes)

    return df_final



schema_empresa = schemas.schema_empresa
final_empresas = '.EMPRECSV'
headers = list(schema_empresa.columns.keys())
print(headers)
df_empresas = ler_arquivos_polars(final_empresas, diretorio)



# Exibe as informações do DataFrame final
print("DataFrame Final:")
print(df_empresas)