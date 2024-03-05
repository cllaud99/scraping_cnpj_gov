import os
import pandera as pa
import duckdb
import polars as pl
import pandas as pd








if __name__ == "__main__":

    dados_raw = 'dados/raw/'
    DATABASE_NAME = 'gov_cnpj.db'
    con = duckdb.connect(DATABASE_NAME)

    path = 'dados/utf8/'

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

if __name__ == "__main__":

    for csvs in os.listdir(dados_raw):
        if csvs.endswith('.CNAECSV'):
            tabela = 'cnaes'
        elif csvs.endswith('.EMPRECSV'):
            tabela = 'empresas'
            csv_empresas = glob.glob
            df_empresas 
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
        df_pl = pl.read_csv(f'{dados_raw}{csvs}', separator=';', encoding='latin-1', ignore_errors=True)    

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

#       df = df_pl.to_pandas()
#
#       print("Leu o DF")
#
#       df_schema = pa.infer_schema(df)
#
#       print("Esquema inferido")
#
#       with open(f"models/{tabela}.py", "w", encoding="utf-8") as arquivo:
#           arquivo.write(df_schema.to_script())
#
#
#




