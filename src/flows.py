from prefect import task, Flow
import prefect
from prefect.schedules import IntervalSchedule
from tasks import request, gerar_nome_arquivo, salva_arquivo_json, inserir_dados_db, carregar_dados_json, criar_schema, criar_table
from datetime import timedelta, datetime


schedule = IntervalSchedule(
    start_date=datetime.now(),
    interval=timedelta(minutes=1),

)

with Flow("BRT-ELT", schedule) as flow:
    criando_schema = criar_schema()
    criando_tabela = criar_table()
    dados_requisicao = request()
    nome_arquivo = gerar_nome_arquivo()
    caminho_arquivo = salva_arquivo_json(nome_arquivo, dados_requisicao)
    carga_dados = carregar_dados_json(caminho_arquivo)
    inserir_no_banco = inserir_dados_db(carga_dados)


flow.register(project_name="teste", labels=["development"])
