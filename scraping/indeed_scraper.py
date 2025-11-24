import requests
from bs4 import BeautifulSoup

def coletar_vagas_indeed():
    URL = "https://br.indeed.com/jobs?q=est%C3%A1gio+ti&l="
    response = requests.get(URL, headers={"User-Agent": "Mozilla/5.0"})
    soup = BeautifulSoup(response.text, "lxml")

    cards = soup.select("div.job_seen_beacon")
    vagas = []

    for card in cards:
        titulo_el = card.select_one("h2 span")
        if not titulo_el:
            continue

        titulo = titulo_el.text.strip().lower()  # padroniza
        empresa = card.select_one(".companyName").text.strip() if card.select_one(".companyName") else "Não informado"
        local = card.select_one(".companyLocation").text.strip() if card.select_one(".companyLocation") else "Não informado"
        local_lower = local.lower()

        # --------------------------
        # 🔥 FILTRAR SOMENTE ESTÁGIO
        # --------------------------
        if "estágio" not in titulo:
            continue

        # --------------------------
        # 🔥 FILTRAR APENAS ÁREA DE TI
        # --------------------------
        palavras_ti = [
            "dados", "data", "ti", "developer", "dev",
            "software", "sistemas", "programa", "python",
            "segurança", "infra", "analista"
        ]
        if not any(p in titulo for p in palavras_ti):
            continue

        # --------------------------
        # 🔥 CLASSIFICAR MODALIDADE
        # --------------------------
        if "remoto" in local_lower or "home" in local_lower:
            modalidade = "Home Office"
        elif "híbrido" in local_lower or "hib" in local_lower:
            modalidade = "Híbrido"
        else:
            modalidade = "Presencial"

        # -----------------------------------------------------------------
        # 🔥 RESTRIÇÃO DE LOCAL: se NÃO for remoto, só pega vagas no **DF**
        # -----------------------------------------------------------------
        if modalidade != "Home Office":
            if not any(uf in local_lower for uf in ["brasília", "df", "distrito federal"]):
                continue

        # --------------------------
        # 🔥 ADICIONAR VAGA
        # --------------------------
        vagas.append({
            "titulo": titulo_el.text.strip(),
            "empresa": empresa,
            "local": local,
            "modalidade": modalidade,
            "fonte": "indeed"
        })

    return vagas
