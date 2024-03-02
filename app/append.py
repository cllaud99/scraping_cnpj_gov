import os

extensao_distinta = set()

for arquivo in os.listdir("dados/raw/"):
    _, extensao = os.path.splitext(arquivo)
    extensao_distinta.add(extensao)
    
print(extensao_distinta)
