
from configs.connection import DBConnectionHandler
from entities.dados_api import Dados_api
from sqlalchemy import Column, Integer, Table, TIMESTAMP, func, MetaData, text
from sqlalchemy.dialects.postgresql import JSONB


class Dados_apiRepository:
    """
    Classe responsável por realizar operações de banco de dados na tabela 'dados_api'

    """
    def select(self):
        """
        Recupera todos os registros da tabela dados_api.

        """
        with DBConnectionHandler() as db:
            data = db.session.query(Dados_api).all()
            return data

    def insert(self, dados):
        """
        Insere um novo registro na tabela dados_api

        """

        with DBConnectionHandler() as db:
            data_insert = Dados_api(dados=dados)
            db.session.add(data_insert)
            db.session.commit()

    def executa_query(self, query):
        """
        Executa uma query SQL personalizada no banco de dados.

        """
        engine = DBConnectionHandler().get_engine()
        with engine.begin() as conn:
            conn.execute(text(query))

    def create_table(self):
        """
        Cria a tabela dados_api no no schema raw_brt do banco de dados, caso ela não exista.

        """
        db = DBConnectionHandler()
        engine = db.get_engine()
        metadata = MetaData()
        table = Table(
            "dados_api",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data_extracao", TIMESTAMP, server_default=func.now()),
            Column("dados", JSONB, nullable=False),
            schema="raw_brt"
        )
        metadata.create_all(engine)
