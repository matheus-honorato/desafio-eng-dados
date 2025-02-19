import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

load_dotenv()

db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST") 
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")


class DBConnectionHandler:
    def __init__(self):
        self.__connection_string = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        self.__engine = self.__create_database_engine()
        self.session = None

    def __create_database_engine(self):
        engine = create_engine(self.__connection_string)
        return engine

    def get_engine(self):
        """
            Feito para rodar comandos SQL diretamente na engine
        """
        return self.__engine

    def __enter__(self):
        session_maker = sessionmaker(bind=self.__engine)
        self.session = session_maker()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        O uso do exit garante que a conexão seja encerrada corretamente.  É fundamental para liberar recursos, 
        como conexões abertas com o banco, evitando vazamento de recursos e possíveis travamentos ou sobrecarga no servidor de banco de dados.

        Esses parâmetros permitem que o método __exit__ saiba se o bloco with terminou normalmente ou se uma exceção foi lançada.
        exc_type: Tipo da exceção que ocorreu (se nenhuma exceção ocorreu, será None).
        exc_val: Valor ou instância da exceção.
        exc_tb: Traceback (a pilha de chamadas que levou à exceção).
        """
        self.session.close()
