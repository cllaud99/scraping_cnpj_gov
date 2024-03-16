import os
import codecs
import shutil
import chardet


def encoding(pasta_origem: str, pasta_destino: str):

    """
    Função que faz encoding de arquivos para utf-8

    Args:
        pasta_origem (str): Pasta onde estão os arquivos a serem verificados.
        pasta_destino (str): Pasta onde serão salvos os arquivos.

    Returns:
        str: Mensagem de sucesso ou erro.

    """

    print("Entramos na função")

    # Garante que a pasta de destino existe
    os.makedirs(pasta_destino, exist_ok=True)

    # Processa cada arquivo na pasta de origem
    for csvs in os.listdir(pasta_origem):
        full_path = os.path.join(pasta_origem, csvs)
        full_path_out = os.path.join(pasta_destino, csvs)
        with codecs.open(full_path, 'r', encoding='latin-1', errors='ignore') as sourceFile:
            content = sourceFile.read()
        with codecs.open(full_path_out, 'w', 'utf-8') as targetFile:
            print('Convertido:', full_path_out)
            targetFile.write(content)

if __name__ == "__main__":

    pasta_origem = 'dados/raw/'
    pasta_destino = 'dados/utf8/'

    encoding(pasta_origem, pasta_destino)
