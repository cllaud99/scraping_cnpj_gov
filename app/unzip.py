import os
import zipfile
from tqdm import tqdm
import codecs

def unzip(pasta_origem: str, pasta_destino: str):
    
    """
    Função que faz a descompactação de arquivos .zip.

    Args:
        pasta_origem (str): Pasta onde estão os arquivos de origem.
        pasta_destino (str): Pasta onde serão salvos os arquivos descompactados.
    Return:
        str: Mensagem de sucesso ou erro.

    """

    if not os.path.exists(pasta_destino):
        os.makedirs(pasta_destino)
    
    zip_files = [f for f in os.listdir(pasta_origem) if f.endswith('.zip')]

    if not zip_files:
        return "Nenhum arquivo .zip encontrado na pasta de origem."

    for zip_file in tqdm(zip_files, desc="Descompactando arquivos"):
        full_path = os.path.join(pasta_origem, zip_file)
        try:
            with zipfile.ZipFile(full_path, 'r') as zip_ref:
                zip_ref.extractall(pasta_destino)
            print(f"Arquivo {zip_file} extraído com sucesso.")
            os.remove(full_path)
        except zipfile.BadZipFile:
            print(f"Erro ao extrair {zip_file}: arquivo corrompido ou inválido.")
        except Exception as e:
            print(f"Erro desconhecido ao extrair {zip_file}: {e}")
    
    return "Extração completa."



if __name__ == "__main__":

    pasta_origem = 'dados/zipados'
    pasta_destino = 'dados/raw'
    unzip(pasta_origem, pasta_destino)
 


