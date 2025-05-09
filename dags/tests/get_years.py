import os
import requests
from bs4 import BeautifulSoup


BASE_URL = "https://www.gov.br/mdic/pt-br/assuntos/comercio-exterior/estatisticas/base-de-dados-bruta"

# Fazendo o request da página
response = requests.get(BASE_URL)
response.raise_for_status()

# Parseando o HTML
soup = BeautifulSoup(response.content, "html.parser")
links = soup.find_all('a', href=True)

anos = []
for link in links:
    href = link['href']
    texto = link.text.strip()
    if 'EXP_' in href and texto.isdigit():
        anos.append(int(texto))

# ordena e pega os últimos 5 anos
ultimos_5 = sorted(set(anos))[-5:]
