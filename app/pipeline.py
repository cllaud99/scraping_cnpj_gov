from unzip import unzip
from download import baixa_dados_cnpj
from enconding import encod
import time



start_time = time.time()

url = 'https://dadosabertos.rfb.gov.br/CNPJ/'
extensao = '.zip'
pasta_zips = "dados/zipados/"
termos = [] #['Empresas', 'Socios', 'Cnaes']
pasta_latin1 = "dados/raw/"
pasta_utf8 = "dados/utf8/"



baixa_dados_cnpj(url, extensao, pasta_zips, termos)
unzip(pasta_zips, pasta_latin1)
encod(pasta_latin1, pasta_utf8)


end_time = time.time()
duracao = end_time - start_time
print("Tempo decorido: ",duracao, "segundos")

