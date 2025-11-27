from scraping.vagas_scraper import coletar_vagas_vagascom
from etl.tratar_vagas import tratar_vagas
from utils.alerta_telegram import enviar_alerta
import pandas as pd
import os
import datetime
import hashlib


def adicionar_timestamp():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def criar_hash_vaga(vaga):
    """Cria um hash único baseado em todos os dados da vaga"""
    vaga_str = f"{vaga['titulo']}{vaga['empresa']}{vaga['local']}{vaga['modalidade']}"
    return hashlib.md5(vaga_str.encode()).hexdigest()


def limpar_historico_antigo(df_historico, dias=7):
    """Remove vagas com mais de X dias do histórico"""
    if 'data_processamento' not in df_historico.columns:
        return df_historico
        
    df_historico['data_processamento'] = pd.to_datetime(df_historico['data_processamento'], errors='coerce')
    data_limite = pd.Timestamp.now() - pd.Timedelta(days=dias)
    
    df_limpo = df_historico[df_historico['data_processamento'] > data_limite]
    
    print(f"🧹 Limpeza: {len(df_historico)} -> {len(df_limpo)} vagas no histórico")
    return df_limpo


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
        # Adicionar hash único
        df['hash_vaga'] = df.apply(criar_hash_vaga, axis=1)

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
        # Manter o hash das vagas tratadas
        df_tratado_novo['hash_vaga'] = df_tratado_novo.apply(criar_hash_vaga, axis=1)

    print(f"🔧 Vagas após tratamento: {len(df_tratado_novo)}")
    if not df_tratado_novo.empty:
        print("📝 Primeiras 3 vagas tratadas:")
        for i, vaga in df_tratado_novo.head(3).iterrows():
            print(f"  {i+1}. {vaga['titulo']} | {vaga['fonte']}")
    else:
        print("⚠️ Nenhuma vaga após tratamento!")
        return

    path_tratado = "data/vagas_tratadas.csv"
    path_historico = "data/vagas_historico.csv"  # Novo arquivo para histórico

    # --- SISTEMA DE HISTÓRICO MELHORADO ---
    if os.path.exists(path_historico):
        df_historico = pd.read_csv(path_historico)
        print(f"📚 Histórico: {len(df_historico)} vagas já processadas")
        
        # Aplicar limpeza no histórico (manter só últimas 2 semanas)
        df_historico = limpar_historico_antigo(df_historico, dias=14)
        
        # Verificar vagas realmente novas
        hashes_historico = set(df_historico['hash_vaga'].tolist())
        df_novas = df_tratado_novo[~df_tratado_novo['hash_vaga'].isin(hashes_historico)]
        
        print(f"🆕 Vagas novas encontradas: {len(df_novas)}")
        
        if not df_novas.empty:
            # Adicionar novas vagas ao histórico
            df_historico_atualizado = pd.concat([df_historico, df_novas], ignore_index=True)
            df_historico_atualizado.to_csv(path_historico, index=False)
            print(f"💾 Histórico atualizado: {len(df_historico_atualizado)} vagas")
        else:
            df_historico_atualizado = df_historico
            
    else:
        # Primeira execução - criar histórico
        df_novas = df_tratado_novo
        df_tratado_novo.to_csv(path_historico, index=False)
        print(f"📁 Criado histórico com {len(df_novas)} vagas")

    # Salvar versão tratada atual (apenas para referência)
    df_tratado_novo.to_csv(path_tratado, index=False)

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