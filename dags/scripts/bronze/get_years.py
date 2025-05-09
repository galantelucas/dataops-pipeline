import requests
from bs4 import BeautifulSoup


class GetYears:
    """Busca os últimos 5 anos disponíveis."""

    def get_years(self, source: str) -> list:

        print("Iniciando busca de anos")
        response = requests.get(source)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, "html.parser")
        links = soup.find_all('a', href=True)

        anos = []
        for link in links:
            href = link['href']
            texto = link.text.strip()
            if 'EXP_' in href and texto.isdigit():
                anos.append(int(texto))
        print(f'anos disponiveis: {anos}')
        return sorted(set(anos))[-2:]
