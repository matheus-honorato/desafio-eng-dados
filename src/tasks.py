import pandas as pd
import requests
import datetime
import os
import json

from prefect import task

@task
def request():

    base_url = "https://dados.mobilidade.rio/gps/brt"
    url = base_url
    response = requests.get(url)
    response.raise_for_status()
    print(response.json())
    return response.json()


@task
def request():

    base_url = "https://dados.mobilidade.rio/gps/brt"
    url = base_url
    response = requests.get(url)
    response.raise_for_status()
    print(response.json())
    return response.json()

@task
def gerar_nome_arquivo() -> str:
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S") 
    nome_arquivo = f"gps_brt_{timestamp}.json"
    print(nome_arquivo)
    return nome_arquivo

@task
def salva_arquivo_json(nome_arquivo, dados):
    """
    Salva os dados extraídos da API em um arquivo JSON no diretório especificado.

    O nome do arquivo é gerado dinamicamente com a função gerar nome.
    Caso ocorra um erro durante a gravação, uma mensagem de erro será exibida.

    """

    caminho_arquivo = os.path.join("raw", nome_arquivo)

    try:
        with open(caminho_arquivo, "w", encoding="utf-8") as file:
            json.dump(dados, file, ensure_ascii=False, indent=4)
            print(f"Dados salvos no arquivo '{caminho_arquivo}' com sucesso!")
    except Exception as e:
        print(f"Erro ao salvar o arquivo JSON: {e}")
