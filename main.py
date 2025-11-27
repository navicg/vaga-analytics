from scraping.vagas_scraper import coletar_vagas_vagascom
from etl.tratar_vagas import tratar_vagas
from utils.alerta_telegram import enviar_alerta
import pandas as pd
import os
import datetime


def adicionar_timestamp():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def criar_identificador_unico(vaga):
    """Cria um identificador único para cada vaga"""
    return f"{vaga['titulo']}_{vaga['empresa']}_{vaga['local']}"


def main():
    print("=== INICIANDO BOT DE VAGAS ===")

    # --- COLETA DE VAGAS ---
    vagas = []

    print("Coletando vagas do Vagas.com...")
    try:
        vagas_vagascom = coletar_vagas_vagascom()
        vagas += vagas_vagascom
        print(f"✅ Vagas.com: {len(vagas_vagascom)} vagas")
    except Exception as e:
        print(f"❌ Erro Vagas.com: {e}")

    # Criar DataFrame bruto
    df = pd.DataFrame(vagas) if vagas else pd.DataFrame()

    if not df.empty:
        df['data_coleta'] = adicionar_timestamp()

    # Criar pasta /data caso não exista
    if not os.path.exists("data"):
        os.makedirs("data")

    # Salvar CSV bruto
    if not df.empty:
        df.to_csv("data/vagas_raw.csv", index=False)
        print(f"📊 Total de {len(df)} vagas coletadas!")
        print("📝 Primeiras 3 vagas coletadas:")
        for i, vaga in df.head(3).iterrows():
            print(f"  {i+1}. {vaga['titulo']} | {vaga['fonte']}")
    else:
        print("⚠️ Nenhuma vaga coletada!")
        return

    # --- TRATAMENTO (ETL) ---
    print("Aplicando tratamentos...")
    df_tratado_novo = tratar_vagas(df)

    if not df_tratado_novo.empty:
        df_tratado_novo['data_processamento'] = adicionar_timestamp()

    # Criar identificador único
    df_tratado_novo['id_unico'] = df_tratado_novo.apply(criar_identificador_unico, axis=1)

    print(f"🔧 Vagas após tratamento: {len(df_tratado_novo)}")
    print("📝 Primeiras 3 vagas tratadas:")
    for i, vaga in df_tratado_novo.head(3).iterrows():
        print(f"  {i+1}. {vaga['titulo']} | {vaga['fonte']}")

    path_tratado = "data/vagas_tratadas.csv"

    # --- COMPARAR COM HISTÓRICO ---
    if os.path.exists(path_tratado):
        df_tratado_antigo = pd.read_csv(path_tratado)

        # Garantir id_unico no histórico
        if 'id_unico' not in df_tratado_antigo.columns:
            df_tratado_antigo['id_unico'] = df_tratado_antigo.apply(criar_identificador_unico, axis=1)

        # Vagas novas = as que não existem no histórico
        ids_antigos = set(df_tratado_antigo['id_unico'].tolist())
        df_novas = df_tratado_novo[~df_tratado_novo['id_unico'].isin(ids_antigos)]

        print(f"📊 Vagas no histórico: {len(df_tratado_antigo)}")
        print(f"🆕 Vagas novas encontradas: {len(df_novas)}")

        # Combinar e remover duplicatas
        df_combinado = pd.concat([df_tratado_antigo, df_novas], ignore_index=True)
        df_combinado = df_combinado.drop_duplicates(subset=['id_unico'], keep='first')

        # Salvar
        df_combinado.to_csv(path_tratado, index=False)
        print(f"💾 Arquivo atualizado: {path_tratado} (total: {len(df_combinado)} vagas)")

    else:
        df_novas = df_tratado_novo
        print(f"📁 Primeira execução: {len(df_novas)} vagas")

        df_tratado_novo.to_csv(path_tratado, index=False)
        print(f"💾 Arquivo salvo: {path_tratado}")

    # --- ENVIAR PARA TELEGRAM ---
    if not df_novas.empty:
        print(f"📤 Enviando {len(df_novas)} vagas novas...")

        for i, vaga in df_novas.iterrows():

            msg = (
                "⚡ *Nova vaga de estágio!*\n\n"
                f"📌 *{vaga['titulo']}*\n"
                f"🏢 {vaga['empresa']}\n"
                f"📍 Local: {vaga['local']}\n"
                f"💼 Modalidade: {vaga['modalidade']}\n"
                f"🔗 Fonte: {vaga['fonte']}\n"
                f"🕒 Coletada em: {vaga.get('data_processamento', 'N/A')}"
            )

            print(f"📨 Enviando: {vaga['titulo'][:30]}...")
            sucesso = enviar_alerta(msg)

            if sucesso:
                print("✅ Enviada")
            else:
                print("❌ Falha no envio")

    else:
        print("🤷 Nenhuma vaga nova para enviar")


if __name__ == "__main__":
    main()
