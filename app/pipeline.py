from etl import check_estabelecimentos, unzip


#Incluir os dados como lista que serão baixados, são aceitos como argumento ('Estabelecimentos', 'Empresas')
url_base = ['Estabelecimentos', 'Empresas']
check_estabelecimentos(url_base)

#Aguardar a descompactação
unzip()
