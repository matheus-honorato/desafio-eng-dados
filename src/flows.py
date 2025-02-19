from prefect import task, Flow
import prefect
from prefect.schedules import IntervalSchedule
from tasks import request, gerar_nome_arquivo, salva_arquivo_json, inserir_dados_db, carregar_dados_json
from datetime import timedelta, datetime


schedule = IntervalSchedule(
    start_date=datetime.now(),
    interval=timedelta(minutes=1),

)


with Flow("BRT-ELT", schedule) as flow:
    dados_requisicao = request()
    nome_arquivo = gerar_nome_arquivo()
    caminho_arquivo = salva_arquivo_json(nome_arquivo, dados_requisicao)
    carga_dados = carregar_dados_json(caminho_arquivo)
    inserir_no_banco = inserir_dados_db(carga_dados)

