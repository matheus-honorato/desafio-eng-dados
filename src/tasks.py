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
import pandas as pd
import numpy as np

@task
def request() -> list[dict[str, Any]]:
    """
    Realiza uma requisição HTTP GET para a API de dados de mobilidade do BRT do Rio de Janeiro.

    Returns:
        List[Dict[str, Any]]: Uma lista de dicionários contendo os dados de GPS dos veículos do BRT.
                              Cada dicionário representa um veículo com suas respectivas informações.

    Raises:
        requests.exceptions.HTTPError: Se a requisição falhar com um código de status HTTP inválido.
        requests.exceptions.RequestException: Se ocorrer um erro durante a requisição, apenas uma exception genérica.
        ValueError: Se a resposta não estiver no formato JSON esperado.
    """
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
    """
    Gera um nome de arquivo único para os dados da BRT, utilizando um timestamp no formato `YYYY-MM-DD_HH-MM-SS`.

    O nome do arquivo segue o padrão: `gps_brt_<timestamp>.csv`.

    Returns:
        str: O nome do arquivo gerado, no formato `gps_brt_YYYY-MM-DD_HH-MM-SS.csv`.

    Exemplo:
        'gps_brt_2024-10-25_14-30-45.csv'
    """
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S") 
    nome_arquivo = f"gps_brt_{timestamp}.csv"
    log("Gerando nome do arquivo com o timestamp recente...")
    return nome_arquivo

@task
def salva_arquivo_csv(nome_arquivo: str, dados_request: dict) -> str :
 
    caminho_arquivo = os.path.join("/usr/app/raw", nome_arquivo)

    if not os.path.exists("/usr/app/raw"):
        raise FileNotFoundError("O diretório '/usr/app/raw' não existe.")

    try:
        if "veiculos" not in dados_request:
            raise KeyError("A chave 'veiculos' não está presente no JSON.")
        veiculos = dados_request['veiculos']
        df = pd.DataFrame(veiculos)
        df.to_csv(caminho_arquivo, index=False, encoding="utf-8")
        log(f"Dados da API foram salvos no arquivo '{caminho_arquivo}' com sucesso!")
        return caminho_arquivo
    except Exception as e:
        log(f"Erro ao salvar o arquivo JSON: {e}")

@task
def carregar_dados_json(nome_arquivo: str):
    """
    Faz a leitura dos dados de um arquivo CSV e retorna uma lista de dicionários no formato JSON.
    Args:
        nome_arquivo (str): Caminho do arquivo CSV a ser carregado.
    Returns:
        List[Dict[str, Any]]: Lista de dicionários contendo os dados dos veículos.
    
    Raises:
        FileNotFoundError: Se o arquivo especificado não existir.
        KeyError: Erro inesperado acontecer.
        Exception: Se ocorrer um erro inesperado durante a leitura do arquivo.
    Exemplo:
        dados = carregar_dados_csv("gps_brt_2023-10-25_14-30-45.csv")
        [{"codigo": "901008", "linha": "22"}, {"codigo": "901011", "linha": "50"}]
    

    """

    try:
        df = pd.read_csv(nome_arquivo, encoding="utf-8")
        df = df.replace([np.nan, 'NaN'], None)
        dados_json = df.to_dict(orient="records")
        log("Dados do CSV carregados e convertidos para JSON com sucesso...")
        return dados_json
    except FileNotFoundError:
        log(f"Erro: O arquivo '{nome_arquivo}' não foi encontrado.")
        raise
    except Exception as e:
        log(f"Erro inesperado ao carregar o arquivo JSON: {e}")
        raise


@task
def criar_schema():
    """
    Cria um schema no banco de dados chamado `raw_brt`, caso ele ainda não exista.

    Raises:
        Exception: Se ocorrer um erro durante a execução da query ou conexão com o banco de dados.

    """
    try:
        apiRepository = Dados_apiRepository()
        apiRepository.executa_query("CREATE SCHEMA IF NOT EXISTS raw_brt;")
        log("Schema criado com sucesso.")
    except Exception as e:
        log(f"Erro ao criar schema: {e}")
        raise
@task
def criar_table():
    """
    Cria uma tabela no banco de dados chamado `dados_api`, caso ela ainda não exista.

    Raises:
        Exception: Se ocorrer um erro durante a execução da query ou conexão com o banco de dados.

    """
    try:
        apiRepository = Dados_apiRepository()
        apiRepository.create_table()
        log("Tabela criada com sucesso.")
    except Exception as e:
        log(f"Erro ao criar tabela: {e}")
        raise

@task
def inserir_dados_db(dados: list[dict[str, Any]]):
    """
    Insere os dados da API no banco de dados

    Args:
        dados (List[Dict[str, Any]]): Lista de dicionários contendo os dados a serem inseridos no banco de dados.

    Raises:
        ValueError: Se `dados` estiver vazio.
        Exception: Se ocorrer um erro durante a inserção no banco de dados.


    """

    if not dados:
        raise ValueError("A lista 'dados' não pode estar vazia.")

    try:
        apiRepository = Dados_apiRepository()
        apiRepository.insert(dados)
        log("Dados da API inseridos no banco de dados com sucesso...")

    except Exception as e:
        log(f"Erro ao inserir dados no banco de dados: {e}")
        raise
