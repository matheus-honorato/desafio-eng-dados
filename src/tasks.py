import pandas as pd
import requests
import datetime

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
