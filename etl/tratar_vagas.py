import pandas as pd

def tratar_vagas(df):
    # Remover duplicatas
    df = df.drop_duplicates(subset=["titulo", "empresa"], keep="first")

    # -----------------------
    # FILTRO 1: ESTÁGIO
    # -----------------------
    df = df[df["titulo"].str.contains("estágio|estagiário|intern", case=False, na=False)]

    # -----------------------
    # FILTRO 2: ÁREA DE TI
    # -----------------------
    palavras_ti = [
        "dados", "data", "ti", "tecno", "dev", "developer", "software", "sistemas",
        "programa", "python", "sql", "segurança", "cyber", "redes", "analista",
        "computação", "cloud", "suporte", "infra"
    ]

    df = df[df["titulo"].str.lower().str.contains("|".join(palavras_ti))]

    # -----------------------
    # DETECTAR MODALIDADE
    # -----------------------
    def detectar_modalidade(local):
        if pd.isna(local):
            return "Presencial"
        loc = local.lower()
        if "home" in loc or "remoto" in loc:
            return "Home Office"
        if "híbrido" in loc or "hib" in loc:
            return "Híbrido"
        return "Presencial"

    df["modalidade"] = df["local"].apply(detectar_modalidade)

    # -----------------------
    # FILTRO 3: LOCALIZAÇÃO (DF para híbrido/presencial)
    # -----------------------
    def permitido(vaga):
        modalidade = vaga["modalidade"]
        local = str(vaga["local"]).lower().strip()

        # remoto sempre permitido
        if modalidade == "Home Office":
            return True

        # SE LOCAL = NÃO INFORMADO → permitir
        if local == "não informado":
            return True

        # híbrido/presencial → só DF
        locais_df = [
            "df", "brasília", "brasilia", "distrito federal",
            "taguatinga", "ceilândia", "gama", "sobradinho"
        ]
        return any(x in local for x in locais_df)

    df = df[df.apply(permitido, axis=1)]

    return df  # ← Só retorna o DataFrame, não salva