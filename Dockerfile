FROM python:3.10

RUN apt update && apt install uuid -y
RUN pip install prefect==1.2.2


WORKDIR /app

COPY ./requirements.txt /app/

RUN pip install --no-cache-dir -r requirements.txt 