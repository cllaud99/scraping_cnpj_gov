import os
import pandera as pa
import duckdb
import polars as pl


DATABASE_NAME = 'gov_cnpj.db'
con = duckdb.connect(DATABASE_NAME)

path = 'dados/utf8/'
dados_raw = 'dados/raw/'
parquet_empresas = 'dados/parquet/empresas.parquet'

tabela = 'inicializada'

con.query("""
SELECT
	*
FROM 
	main.empresas emp
left join main.estabelecimentos est
on emp.CNPJ_BASICO = est.CNPJ_BASICO 
""").show()



for csvs in os.listdir(dados_raw):
    if csvs.endswith('.CNAECSV'):
        tabela = 'cnaes'
    elif csvs.endswith('.EMPRECSV'):
        tabela = 'empresas'
    elif csvs.endswith('.MUNICCSV'):
        tabela = 'municipios'
    elif csvs.endswith('.ESTABELE'):
        tabela = 'estabelecimentos'
    elif csvs.endswith('.NATJUCSV'):
        tabela = 'paises'
    elif csvs.endswith('.QUALSCSV'):
        tabela = 'qualificacao_socio'
    elif csvs.endswith('.SIMPLES.CSV.D40210'):
        tabela = 'simples'
    elif csvs.endswith('.SOCIOCSV'):
        tabela = 'socios'
    print(tabela)
    print(csvs)
    df = pl.read_csv(f'{dados_raw}{csvs}', separator=';', encoding='latin-1', ignore_errors=True)
    print("Leu o DF")
    try:
        query = f"""
            INSERT INTO {tabela}
            SELECT
                    *
            FROM
            df
            """
        con.sql(query)
        print(f"Tabela {tabela} populada")
    except Exception as e:
        print(f"Erro no item {tabela}: {e}")






#query_to_parquet = f"""
#COPY ({query})
#TO '{parquet_empresas}'
#(FORMAT PARQUET);
#"""

#print(query_to_parquet)

#duckdb.query(query).show()








