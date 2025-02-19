import pandas as pd
import requests

from prefect import task

@task
def request():

    base_url = "https://dados.mobilidade.rio/gps/brt"
    url = base_url
    response = requests.get(url)
    response.raise_for_status()
    print(response.json())
    return response.json()

