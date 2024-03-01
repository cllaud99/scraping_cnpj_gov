import os
import zipfile
from tqdm import tqdm

def unzip():
    dados_zipados = 'dados/zipados'
    dados_raw = 'dados/raw'
    if not os.path.exists(dados_raw):
        os.makedirs(dados_raw)
    
    zip_files = [f for f in os.listdir(dados_zipados) if f.endswith('.zip')]
    for zip_file in tqdm(zip_files, desc="Descompactando arquivos"):
        full_path = os.path.join(dados_zipados, zip_file)
        with zipfile.ZipFile(full_path, 'r') as zip_ref:
            zip_ref.extractall(dados_raw)
        print(f"Arquivo {zip_file} extraído com sucesso.")
    
    print("Extração completa.")


if __name__ == "__main__":
    unzip()


