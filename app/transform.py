import os
import pandera as pa
import duckdb



DATABASE_NAME = 'gov_cnpj.db'
con = duckdb.connect(DATABASE_NAME)

path = 'dados/utf8/'
parquet_empresas = 'dados/parquet/empresas.parquet'
arquivos_empresas = []


tabela_cnaes = """
            'dados/utf8/*.CNAECSV', AUTO_DETECT=TRUE, sep=";"
"""


query_test = f"""
                SELECT
                   *
                FROM
                 read_csv({tabela_cnaes})
    """   





cnaes = ['cnaes', f"{path}*.CNAECSV', AUTO_DETECT=TRUE, sep=';'"]
empresas = ['empresas', f"{path}*.EMPRECSV', AUTO_DETECT=TRUE, sep=';'"]
estalecimentos = ['estalecimentos' , f"{path}*.ESTABELE', AUTO_DETECT=TRUE, sep=';'"]
municipios = ['municipios' , f"{path}*.MUNICCSV', AUTO_DETECT=TRUE, sep=';'"]
natureza_juridica = ['natureza_juridica' , f"{path}*.NATJUCSV', AUTO_DETECT=TRUE, sep=';'"]
paises = ['paises' , f"{path}*.PAISCSV', AUTO_DETECT=TRUE, sep=';'"]
qualificacao_socio = ['qualificacao_socio' , f"{path}*.QUALSCSV', AUTO_DETECT=TRUE, sep=';'"]
simples = ['simples' , f"{path}*.SIMPLES.CSV.D40210', AUTO_DETECT=TRUE, sep=';'"]
socios = ['socios' , f"{path}*..SOCIOCSV', AUTO_DETECT=TRUE, sep=';'"]



lista_tabelas = [cnaes, empresas, estalecimentos, municipios, natureza_juridica, paises, qualificacao_socio, simples, socios]


query = f"""
       INSERT INTO {cnaes[0]}
       SELECT
            *
        FROM
        read_csv({cnaes[1]})
    """



print(query)

#query_to_parquet = f"""
#COPY ({query})
#TO '{parquet_empresas}'
#(FORMAT PARQUET);
#"""

#print(query_to_parquet)

#duckdb.query(query).show()

con.sql(query)







