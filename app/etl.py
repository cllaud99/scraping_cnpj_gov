import requests
import os
import wget
from tqdm import tqdm

def check_estabelecimentos(url_base: list):
    for i in url_base:
        url_inicial =  f'https://dadosabertos.rfb.gov.br/CNPJ/'
        n_estabelecimento = 0
        n_estabelecimento_list = []
        folder_name = "dados/zipados"
        os.makedirs(folder_name, exist_ok=True)
        while True:
            url = f'{url_inicial}{i}{n_estabelecimento}.zip' 
            # Envia uma requisição HEAD para a URL
            response = requests.head(url)
            if response.status_code == 200:
                print(f"Página {url} existe!")
                n_estabelecimento_list.append(n_estabelecimento)
                file_name = f'{i}_{n_estabelecimento}.zip'
                file_path = os.path.join(folder_name, file_name)
                total_size = int(response.headers.get('Content-Length', 0))
                # Baixa o arquivo com barra de progresso
                with open(file_path, 'wb') as f:
                    response = requests.get(url, stream=True)
                    with tqdm(total=total_size, unit='iB', unit_scale=True, desc=file_name, ascii=True) as barra_de_progresso:
                        for chunk in response.iter_content(chunk_size=1024):
                            if chunk:
                                f.write(chunk)
                                barra_de_progresso.update(len(chunk))
            elif response.status_code == 404:
                print(f"Página {url} não foi encontrada!")
                break 
            else:
                print("Erro na requisição:", response.status_code)
                break 
            n_estabelecimento += 1
    return n_estabelecimento_list



# Chamada da função com a URL como argumento e atribuição do resultado a uma variável
#url_base = 'https://dadosabertos.rfb.gov.br/CNPJ/Estabelecimentos'
#estabelecimento_list = check_estabelecimentos(url_base)
#print("Lista de n_estabelecimento:", estabelecimento_list)
