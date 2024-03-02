import os
import zipfile
from tqdm import tqdm
import codecs

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

def encoding():
    BLOCKSIZE = 1048576
    dados_raw = 'dados/raw/'
    dados_utf8 = 'dados/utf8/'
    os.makedirs(dados_utf8, exist_ok=True)
    for csvs in os.listdir(dados_raw):
        full_path = os.path.join(dados_raw, csvs)
        full_path_out = os.path.join(dados_utf8, csvs)
        with codecs.open(full_path, 'r', encoding='latin-1', errors='ignore') as sourceFile:
            content = sourceFile.read()
        with codecs.open(full_path_out, 'w', 'utf-8') as targetFile:
            print('Convertido:', full_path_out)
            targetFile.write(content)


if __name__ == "__main__":
    encoding()


