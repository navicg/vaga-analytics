from scraping.vagas_scraper import coletar_vagas_vagascom
from scraping.indeed_scraper import coletar_vagas_indeed
from etl.tratar_vagas import tratar_vagas
from utils.alerta_telegram import enviar_alerta
import pandas as pd
import os

def main():
    print("=== INICIANDO BOT DE VAGAS ===")
    
    # --- COLETA DE VAGAS ---
    vagas = []

    print("Coletando vagas do Vagas.com...")
    try:
        vagas += coletar_vagas_vagascom()
        print(f"✅ Vagas.com: {len(vagas)} vagas")
    except Exception as e:
        print(f"❌ Erro Vagas.com: {e}")

    print("Coletando vagas do Indeed...")
    try:
        vagas_indeed = coletar_vagas_indeed()
        vagas += vagas_indeed
        print(f"✅ Indeed: {len(vagas_indeed)} vagas")
    except Exception as e:
        print(f"❌ Erro Indeed: {e}")

    # Criar DataFrame
    df = pd.DataFrame(vagas) if vagas else pd.DataFrame()

    # Criar pasta data/ se não existir
    if not os.path.exists("data"):
        os.makedirs("data")

    # Salvar CSV bruto
    if not df.empty:
        df.to_csv("data/vagas_raw.csv", index=False)
        print(f"📊 Total de {len(df)} vagas coletadas!")
        print("📝 Primeiras 3 vagas coletadas:")
        for i, vaga in df.head(3).iterrows():
            print(f"  {i+1}. {vaga['titulo']}")
    else:
        print("⚠️  Nenhuma vaga coletada!")
        return

    # --- TRATAMENTO (ETL) ---
    print("Aplicando tratamentos...")
    df_tratado_novo = tratar_vagas(df)  # ← Passa o DataFrame coletado
    
    print(f"🔧 Vagas após tratamento: {len(df_tratado_novo)}")
    print("📝 Primeiras 3 vagas tratadas:")
    for i, vaga in df_tratado_novo.head(3).iterrows():
        print(f"  {i+1}. {vaga['titulo']}")

    path_tratado = "data/vagas_tratadas.csv"

    # DEBUG: Verificar se arquivo existe
    print(f"📁 Arquivo {path_tratado} existe: {os.path.exists(path_tratado)}")

    # Verificar se existem vagas novas
    if os.path.exists(path_tratado):
        print("📖 Lendo arquivo antigo...")
        df_tratado_antigo = pd.read_csv(path_tratado)
        print(f"📊 Vagas no arquivo antigo: {len(df_tratado_antigo)}")
        
        # DEBUG: Mostrar comparação
        print("🔍 Comparando vagas...")
        titulos_novos = set(df_tratado_novo['titulo'])
        titulos_antigos = set(df_tratado_antigo['titulo'])
        
        print(f"Títulos novos: {len(titulos_novos)}")
        print(f"Títulos antigos: {len(titulos_antigos)}")
        print(f"Diferença: {titulos_novos - titulos_antigos}")
        
        df_novas = df_tratado_novo[
            ~df_tratado_novo["titulo"].isin(df_tratado_antigo["titulo"])
        ]
        print(f"🆕 Vagas novas encontradas: {len(df_novas)}")
    else:
        print("📁 Primeira execução - todas as vagas são novas")
        df_novas = df_tratado_novo
        print(f"🆕 Vagas novas: {len(df_novas)}")

    # Salvar o novo arquivo tratado
    df_tratado_novo.to_csv(path_tratado, index=False)
    print(f"💾 Arquivo salvo: {path_tratado}")

    # --- ENVIAR PARA TELEGRAM ---
    if not df_novas.empty:
        print(f"📤 Enviando {len(df_novas)} vagas novas...")
        for _, vaga in df_novas.iterrows():
            msg = (
                f"⚡ Nova vaga de estágio!\n\n"
                f"📌 {vaga['titulo']}\n"
                f"🏢 {vaga['empresa']}\n"
                f"📍 Local: {vaga['local']}\n"
                f"💼 Modalidade: {vaga['modalidade']}\n"
                f"🔗 Fonte: {vaga['fonte']}"
            )
            print(f"📨 Enviando: {vaga['titulo'][:30]}...")
            sucesso = enviar_alerta(msg)
            if sucesso:
                print("✅ Enviada")
            else:
                print("❌ Falha no envio")
    else:
        print("🤷 Nenhuma vaga nova para enviar")
        
        # FORÇAR ENVIO DE TESTE
        print("\n🚨 ENVIANDO MENSAGEM DE TESTE...")
        msg_teste = "🚀 TESTE: Bot funcionando, mas sem vagas novas"
        enviar_alerta(msg_teste)

if __name__ == "__main__":
    main()