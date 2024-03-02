import os
import pandera as pa
import duckdb



DATABASE_NAME = 'gov_cnpj.db'

con = duckdb.connect(DATABASE_NAME)

path = 'dados/raw/'
parquet_empresas = 'dados/parquet/empresas.parquet'
arquivos_empresas = []

if not os.path.exists('dados/parquet'):
    os.makedirs('dados/parquet')


tabela_estabelecimentos = """
    'dados/raw/*.ESTABELE', AUTO_DETECT=FALSE, sep=";"
"""

tabela_empresas = """
            'dados/raw/*.EMPRECSV', AUTO_DETECT=FALSE, sep=";"
"""

tabela_cnaes = """
            'dados/raw/*.CNAECSV', AUTO_DETECT=TRUE, sep=";"
"""


#print(tabela_estabelecimentos)


query_test = f"""
                SELECT
                   *
                FROM
                 read_csv({tabela_estabelecimentos})
    """   





tabela_cnaes = "'dados/raw/*.CNAECSV', AUTO_DETECT=TRUE, sep=';'"

query = f"""
       INSERT INTO CNAES 
       SELECT
            *
        FROM
        read_csv({tabela_cnaes})
    """


with open('dados/raw/F.K03200$Z.D40210.CNAECSV') as f:
    print(f)

print(query)

#query_to_parquet = f"""
#COPY ({query})
#TO '{parquet_empresas}'
#(FORMAT PARQUET);
#"""

#print(query_to_parquet)


#con.sql(query)







