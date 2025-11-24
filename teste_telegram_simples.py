import requests

def teste_telegram():
    TOKEN = "8257668927:AAHLUjT7tGsHTDDXg7mpJ_BVtsWe5XyhTws"
    CHAT_ID = "1185406839"
    
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    
    payload = {
        "chat_id": CHAT_ID,
        "text": "🔴 TESTE SIMPLES - Bot funcionando!"
    }
    
    print("Enviando mensagem de teste...")
    try:
        response = requests.post(url, json=payload, timeout=10)
        print(f"Status: {response.status_code}")
        print(f"Resposta: {response.text}")
        
        if response.status_code == 200:
            print("✅ Mensagem enviada com sucesso!")
        else:
            print("❌ Erro no envio")
            
    except Exception as e:
        print(f"❌ Exception: {e}")

if __name__ == "__main__":
    teste_telegram()