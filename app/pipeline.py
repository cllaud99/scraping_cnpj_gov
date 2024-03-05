from unzip import unzip
from download import baixa_dados_cnpj


url = 'https://dadosabertos.rfb.gov.br/CNPJ/'
final_arquivo = '.zip'

baixa_dados_cnpj(url, final_arquivo)

unzip()
