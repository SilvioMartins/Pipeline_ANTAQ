###########################################################
# OBJETIVO:  Download dos Arquivos no Site ANTAC
# DATA: 02Out2023
# OWNER: Silvio Martins
###########################################################

#Importanndo Bibliotecas
import requests
import os
from datetime import datetime
import shutil
import zipfile

#Variáveis 
# Ano Atual
CY = datetime.now().year
# Determinação dos 3 últimos anos móveis - RN.1
anos = [str(CY-2),str(CY-1),str(CY)]

# Apaga arquivo ZIP  e pasra do Referido ANO
def apaga(ano):
    # Apaga o .ZIP
    os.remove(f'{ano}.zip')
    # Apaga a Pasta descompcatada
    shutil.rmtree(f'{ano}')

# Descompacat o ZIP em uma Pasta
def descompacta(ano):
    # Cria a Pasta
    os.mkdir(f'{ano}')
    # Descompacat Zip na Pasta
    zip = zipfile.ZipFile(f'{ano}.zip')
    zip.extractall(f'{ano}')
    # Fecha Arquivo
    zip.close()

# Download dos Arquivos dos anos tratados
try:
    for x in anos:
        # Pega arquivo do referido Ano no site da ANTAQ
        arq = requests.get('https://web3.antaq.gov.br/ea/txt/{x}.zip', allow_redirects=True)
        # Apagando se existir arquivos referente ao ano tratado
        apaga(f'{x}')
        # Escreve os arquivo baixados no Lake 
        open('{x}.zip', 'wb').write(arq.content)
        # Descomnpacta ZIP em Pasta
        descompacta(f'{x}')
except:
    print('Problemas com o Download!')
