from configs.base import Base
from sqlalchemy import Column, Integer, Text, TIMESTAMP, func
from sqlalchemy.dialects.postgresql import JSONB


class Dados_api(Base):
    __tablename__ = "dados_api"
    __table_args__ = {"schema": "raw_brt"}  # Definir o schema

    id = Column(Integer, primary_key=True)
    data_extracao = Column(TIMESTAMP, server_default=func.now())  # Timestamp padrão
    dados = Column(JSONB, nullable=False)  # JSON armazenado no formato otimizado do PostgreSQL