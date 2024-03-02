import os
import pandera as pa
import duckdb



DATABASE_NAME = 'gov_cnpj.db'
con = duckdb.connect(DATABASE_NAME)

path = 'dados/utf8/'
parquet_empresas = 'dados/parquet/empresas.parquet'

cnaes = ['cnaes', f"'{path}*.CNAECSV', AUTO_DETECT=TRUE, sep=';'"]
empresas = ['empresas', f"'{path}*.EMPRECSV', AUTO_DETECT=TRUE, sep=';'"]
estabelecimentos = ['estabelecimentos' , f"'{path}*.ESTABELE', AUTO_DETECT=TRUE, sep=';'"]
municipios = ['municipios' , f"'{path}*.MUNICCSV', AUTO_DETECT=TRUE, sep=';'"]
natureza_juridica = ['natureza_juridica' , f"'{path}*.NATJUCSV', AUTO_DETECT=TRUE, sep=';'"]
paises = ['paises' , f"'{path}*.PAISCSV', AUTO_DETECT=TRUE, sep=';'"]
qualificacao_socio = ['qualificacao_socio' , f"'{path}*.QUALSCSV', AUTO_DETECT=TRUE, sep=';'"]
simples = ['simples' , f"'{path}*.SIMPLES.CSV.D40210', AUTO_DETECT=TRUE, sep=';'"]
socios = ['socios' , f"'{path}*.SOCIOCSV', AUTO_DETECT=TRUE, sep=';'"]



lista_tabelas = [cnaes, empresas, estabelecimentos, municipios, natureza_juridica, paises, qualificacao_socio, simples, socios]


for item in lista_tabelas:
    try:
        query = f"""
            INSERT INTO {item[0]}
            SELECT
                    *
            FROM
            read_csv({item[1]})
            """
        print(f"Tabela {item} populada")
        con.sql(query)
    except Exception as e:
        print(f"Erro no item {item}: {e}")

#query_to_parquet = f"""
#COPY ({query})
#TO '{parquet_empresas}'
#(FORMAT PARQUET);
#"""

#print(query_to_parquet)

#duckdb.query(query).show()








