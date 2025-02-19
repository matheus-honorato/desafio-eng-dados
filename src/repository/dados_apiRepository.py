
from configs.connection import DBConnectionHandler
from entities.dados_api import Dados_api


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
