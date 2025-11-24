import requests
import os

def enviar_alerta(texto):
    TOKEN = os.getenv("TELEGRAM_TOKEN", "8257668927:AAHLUjT7tGsHTDDXg7mpJ_BVtsWe5XyhTws")
    CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "1185406839")

    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"

    payload = {
        "chat_id": CHAT_ID, 
        "text": texto,
        "parse_mode": "HTML"
    }

    try:
        response = requests.post(url, json=payload, timeout=10)
        return response.status_code == 200
    except Exception as e:
        print(f"Erro Telegram: {e}")
        return False