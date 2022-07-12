import os
import requests

FILES: dict[str, str] = {
    "ancine_directors_brazil.csv": "https://dados.ancine.gov.br/dados-abertos/DiretoresDeObrasNaoPublicitariasBrasileiras.csv",
    "ancine_directors_foreign.csv": "https://dados.ancine.gov.br/dados-abertos/DiretoresDeObrasNaoPublicitariasEstrangeiras.csv",
    "ancine_distributors.csv": "https://www.gov.br/ancine/pt-br/oca/cinema/arquivos.csv/listagem-de-distribuidoras-2009-a-2020.csv",
    "ancine_movies.xlsx": "https://www.gov.br/ancine/pt-br/oca/cinema/arquivos/listagem-de-filmes-brasileiros-e-estrangeiros-lancados-2009-a-2020.xlsx",
    "classind.csv": "http://portal.mj.gov.br/ClassificacaoIndicativa/jsps/DadosAbertosForm.do?download=obra",
}

try:
    os.chdir("data_raw/")
except FileNotFoundError:
    os.mkdir("data_raw")
    os.chdir("data_raw/")

for file_name, file_url in FILES.items():
    response: requests.Response = requests.get(file_url, verify=False)
    with open(file_name, "wb") as file:
        file.write(response.content)
