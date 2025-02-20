from configs.base import Base
from sqlalchemy import Column, Integer, Text, TIMESTAMP, func
from sqlalchemy.dialects.postgresql import JSONB


class Dados_api(Base):
     """
    Classe que representa a tabela 'dados_api' no banco de dados do POSTGRESQL.

    A tabela é responsável por armazena os dados extraídos da API.

    Attributes:
        id (int): Chave primária da tabela.
        data_extracao (datetime): Timestamp da extração dos dados (gerado automaticamente).
        dados (JSONB): Dados da API armazenados em formato JSON.
    """
    __tablename__ = "dados_api"
    __table_args__ = {"schema": "raw_brt"}  

    id = Column(Integer, primary_key=True)
    data_extracao = Column(TIMESTAMP, server_default=func.now())  
    dados = Column(JSONB, nullable=False) 