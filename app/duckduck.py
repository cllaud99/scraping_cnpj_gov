import duckdb


caminho = 'dados/zipados/Empresas2*.zip'


df_duckdb = duckdb.read_csv(caminho)


print(df_duckdb)