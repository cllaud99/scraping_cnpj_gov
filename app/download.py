import pandas as pd
import requests
import os
from tqdm import tqdm


def baixa_dados_cnpj(url: str, final_arquivo: str):

    dfs = pd.read_html(url)

    df_urls = dfs[0]
    full_path_list = []
    pasta_destino = "dados/zipados"

    for name in df_urls["Name"]:

        if isinstance(name, str) and name.endswith(final_arquivo):

            full_path = f"{url}{name}"
            response = requests.head(full_path)

            if response.status_code == 200:
                print(f"O endereço: {full_path} foi encontrado")
                total_size = int(response.headers.get('Content-Length', 0))
                full_path_list.append(( name, full_path, total_size))  # Adicione o caminho à lista



    total_size_sum = sum(total_size for _, _, total_size in full_path_list)

    quantidade_downloads = len(full_path_list)

    size_gb = round(float(total_size_sum) / (1024 ** 3), 2)

    print(f"Será feito o download de: {len(full_path_list)} arquivos "
        f"totalizando um tamanho de: {size_gb} GB "
        "essa operação irá demorar um pouco...")
    
    resposta = input("Deseja realizar o Download agora? (s/n)")

    while resposta.lower() != 's' and resposta.lower() != 'n':
        print("Você digitou um valor incorreto")
        resposta = input("Deseja realizar o Download agora? (s/n)")
    

    if resposta.lower() == 's':
    
        etapa = 1
        total_baixado = 0

        for file_name, file_url, file_size in full_path_list:
            
            print(f"Executando etapa {etapa} de {quantidade_downloads}")
            file_path = os.path.join(pasta_destino, file_name)
            
            with requests.get(file_url, stream=True) as r:

                r.raise_for_status()
                with open(file_path, 'wb') as f:
                    pbar = tqdm(total=file_size, unit='B', unit_scale=True, desc=file_name)
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))
                    pbar.close()
            etapa += 1
            total_baixado += file_size
            total_baixado_gb = round(float(total_baixado / (1024 ** 3)),2)
            progresso_download = round(float(total_baixado / total_size_sum * 100),2)

            print(f"Baixados: {total_baixado_gb} de {size_gb} progresso total: {progresso_download}%")

        print(full_path_list)

    else: print("Processo abortado")



url_test = 'https://dadosabertos.rfb.gov.br/CNPJ/'
final_arquivo_test = '.zip'

if __name__ == "__main__":
    baixa_dados_cnpj(url_test, final_arquivo_test)