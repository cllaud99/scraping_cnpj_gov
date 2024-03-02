from app.unzip import check_estabelecimentos, downloads_gerais, unzip

#Incluir os dados como lista que serão baixados, são aceitos como argumento ('Estabelecimentos', 'Empresas', 'Socios')
url_base = ['Socios', 'Empresas', 'Estabelecimentos']
check_estabelecimentos(url_base)

#incluir os links que deseja baixar, estão disponíveis: ['Cnaes','Motivos','Municipios','Naturezas','Paises','Qualificacoes','Simples']
lista_downloads = ['Cnaes','Motivos','Municipios','Naturezas','Paises','Qualificacoes','Simples']









downloads_gerais(lista_downloads)

#Aguardar a descompactação
unzip()
