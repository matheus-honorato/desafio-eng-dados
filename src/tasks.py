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

    O nome do arquivo segue o padrão: `gps_brt_<timestamp>.json`.

    Returns:
        str: O nome do arquivo gerado, no formato `gps_brt_YYYY-MM-DD_HH-MM-SS.json`.

    Exemplo:
        'gps_brt_2024-10-25_14-30-45.json'
    """
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S") 
    nome_arquivo = f"gps_brt_{timestamp}.json"
    log("Gerando nome do arquivo com o timestamp recente...")
    return nome_arquivo

@task
def salva_arquivo_json(nome_arquivo: str, dados_request: dict) -> str :
    """
    Salva os dados extraídos da API em um arquivo JSON no diretório especificado.

    O nome do arquivo é gerado dinamicamente com a função gerar nome.

    Args:
        nome_arquivo (str): Nome do arquivo JSON a ser criado.
        dados_request List[Dict[str, Any]]: Dados da API a serem salvos no arquivo JSON.
    Returns:
        str: Caminho completo do arquivo JSON salvo.
    
    Exemplo:
        salva_arquivo_json("gps_brt_2023-10-25_14-30-45.json", dados)
        '/usr/app/raw/gps_brt_2023-10-25_14-30-45.json'

    Raises:
    FileNotFoundError: Se o diretório especificado não existir.
    Exception: Se ocorrer um erro inesperado durante a gravação do arquivo.
    """

    caminho_arquivo = os.path.join("/usr/app/raw", nome_arquivo)

    if not os.path.exists("/usr/app/raw"):
        raise FileNotFoundError("O diretório '/usr/app/raw' não existe.")

    try:
        with open(caminho_arquivo, "w", encoding="utf-8") as file:
            json.dump(dados_request, file, ensure_ascii=False, indent=4)
            log(f"Dados da API foram salvos no arquivo '{caminho_arquivo}' com sucesso!")
            return caminho_arquivo

    except Exception as e:
        log(f"Erro ao salvar o arquivo JSON: {e}")

@task
def carregar_dados_json(nome_arquivo: str):
    """
    Faz a leitura dos dados de um arquivo JSON e retorna a lista de veículos contida nele.
    Args:
        nome_arquivo (str): Caminho do arquivo JSON a ser carregado.
    Returns:
        List[Dict[str, Any]]: Lista de dicionários contendo os dados dos veículos.
    
    Raises:
        FileNotFoundError: Se o arquivo especificado não existir.
        KeyError: Se a chave "veiculos" não estiver presente no JSON.
        Exception: Se ocorrer um erro inesperado durante a leitura do arquivo.
    Exemplo:
        dados = carregar_dados_json("gps_brt_2023-10-25_14-30-45.json")
        [{"codigo": "901008", "linha": "22"}, {"codigo": "901011", "linha": "50"}]
    

    """
    try:
        with open(nome_arquivo, "r", encoding="utf-8") as file:
            json_data = json.load(file)
            if "veiculos" in json_data:
                log("Dados da API carregado para leitura com sucesso...")
                return json_data["veiculos"]
            else:
                log("Não encontrado results no json")
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
