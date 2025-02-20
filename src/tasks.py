import requests
import datetime
import os
import json
from prefect import task
from utils import log
from entities.dados_api import Dados_api
from repository.dados_apiRepository import Dados_apiRepository
from sqlalchemy import Column, Integer, Table, TIMESTAMP, func, MetaData
from sqlalchemy.dialects.postgresql import JSONB
from typing import  Any

@task
def request():

    base_url = "https://dados.mobilidade.rio/gps/brt"
    url = base_url
    try:
        log("Realizando requisição dos dados...")
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data

    except requests.exceptions.HTTPError as http_err:
        log(f"Erro HTTP ao realizar a requisição: {http_err}")
        raise
    except requests.exceptions.RequestException as req_err:
        log(f"Erro durante a requisição: {req_err}")
        raise
    except ValueError as val_err:
        log(f"Erro ao processar a resposta da API: {val_err}")
        raise

@task
def gerar_nome_arquivo() -> str:
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S") 
    nome_arquivo = f"gps_brt_{timestamp}.json"
    log("Gerando nome do arquivo com o timestamp recente...")
    # print("Gerando nome do arquivo com o timestamp recente...")

    return nome_arquivo

@task
def salva_arquivo_json(nome_arquivo, dados_request):
    """
    Salva os dados extraídos da API em um arquivo JSON no diretório especificado.

    O nome do arquivo é gerado dinamicamente com a função gerar nome.
    Caso ocorra um erro durante a gravação, uma mensagem de erro será exibida.

    """

    caminho_arquivo = os.path.join("/usr/app/raw", nome_arquivo) ##Docker
    # caminho_arquivo = os.path.join("raw", nome_arquivo) # Local

    try:
        with open(caminho_arquivo, "w", encoding="utf-8") as file:
            json.dump(dados_request, file, ensure_ascii=False, indent=4)
            # print(f"Dados da API foram salvos no arquivo '{caminho_arquivo}' com sucesso!")
            log(f"Dados da API foram salvos no arquivo '{caminho_arquivo}' com sucesso!")
            return caminho_arquivo
            
            
    except Exception as e:
        print(f"Erro ao salvar o arquivo JSON: {e}")
        log("Erro ao salvar o arquivo JSON: {e}")

@task
def carregar_dados_json(nome_arquivo):
    with open(nome_arquivo, "r", encoding="utf-8") as file:
        json_data = json.load(file)
        if "veiculos" in json_data:
            log("Dados da API carregado para leitura com sucesso...")
            return json_data["veiculos"]
        else:
            log("Não encontrado results no json")

@task
def criar_schema():
    try:
        apiRepository = Dados_apiRepository()
        apiRepository.executa_query("CREATE SCHEMA IF NOT EXISTS raw_brt;")
        log("Schema criado com sucesso.")
    except Exception as e:
        log(f"Erro ao criar schema: {e}")
@task
def criar_table():
    try:
        apiRepository = Dados_apiRepository()
        apiRepository.create_table()
        log("Tabela criada com sucesso.")
    except Exception as e:
        log(f"Erro ao criar tabela: {e}")

@task
def inserir_dados_db(dados):
    apiRepository = Dados_apiRepository()
    apiRepository.insert(dados)
    log("Dados da API inserido no banco de dados com sucesso...")
