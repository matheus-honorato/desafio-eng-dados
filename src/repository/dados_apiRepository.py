
from configs.connection import DBConnectionHandler
from entities.dados_api import Dados_api
from sqlalchemy import Column, Integer, Table, TIMESTAMP, func, MetaData, text
from sqlalchemy.dialects.postgresql import JSONB


class Dados_apiRepository:
    def select(self):
        with DBConnectionHandler() as db:
            data = db.session.query(Dados_api).all()
            return data

    def insert(self, dados):
        with DBConnectionHandler() as db:
            data_insert = Dados_api(dados=dados)
            db.session.add(data_insert)
            db.session.commit()

    def executa_query(self, query):
        engine = DBConnectionHandler().get_engine()
        with engine.begin() as conn:
            conn.execute(text(query))

    def create_table(self):
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
