import requests
from bs4 import BeautifulSoup
from datetime import datetime

from scripts.bronze.updates import CheckUpdates

class GetYears:
    """Busca os últimos 5 anos disponíveis."""

    def get_years(self, source: str) -> list:
        """Retorna a lista de anos a serem baixados"""
        print("Analisando atualização local")

        check_updates = CheckUpdates()
        db_config = check_updates.load_db_config()
        db_last_update = check_updates.get_db_last_update(db_config)

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

        anos = sorted(set(anos))[-2:]
        print(f"Anos disponíveis: {anos}")

        if db_last_update is None:
            print("Sem atualização local! Processar todos os anos disponíveis.")
            anos_filtrados = anos
        else:
            # Se for string, converter para datetime
            if isinstance(db_last_update, str):
                db_last_update = datetime.strptime(db_last_update, "%d/%m/%Y %H:%M")

            ano_limite = db_last_update.year
            anos_filtrados = [ano for ano in anos if ano >= ano_limite]
            print(f"Última atualização em {db_last_update}. Processar anos a partir de {ano_limite}: {anos_filtrados}")

        return anos_filtrados