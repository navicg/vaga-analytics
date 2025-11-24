import requests
from bs4 import BeautifulSoup

def coletar_vagas_vagascom():
    URL = "https://www.vagas.com.br/vagas-de-ti-estagio"
    response = requests.get(URL)
    soup = BeautifulSoup(response.text, "lxml")

    cards = soup.select(".vaga")
    vagas = []

    for card in cards:
        titulo = card.select_one("h2").text.strip()
        empresa = card.select_one(".emprVaga").text.strip() if card.select_one(".emprVaga") else "Não informado"
        
        local_el = card.select_one(".vagaLocal")
        local = local_el.text.strip() if local_el else "Não informado"

        # 🔥 CLASSIFICAÇÃO DE MODALIDADE
        modalidade = "Presencial"
        titulo_lower = titulo.lower()

        if "home office" in local.lower() or "remoto" in local.lower():
            modalidade = "Home Office"
        elif "híbrido" in local.lower() or "hib" in local.lower():
            modalidade = "Híbrido"

        # 🔥 FILTRAR SOMENTE ESTÁGIO
        if "estágio" not in titulo_lower:
            continue

        # 🔥 FILTRAR APENAS ÁREAS DE TI
        palavras_ti = [
            "dados", "data", "ti", "tecno", "dev", "developer",
            "software", "sistemas", "programa", "segurança",
            "cyber", "redes", "analista"
        ]

        if not any(p in titulo_lower for p in palavras_ti):
            continue

        vagas.append({
            "titulo": titulo,
            "empresa": empresa,
            "local": local,
            "modalidade": modalidade,
            "fonte": "vagas.com"
        })

    return vagas
