import requests
import json
import logging
from typing import Dict, Any

class GigachatService:
    def __init__(self, token: str):
        self.token = token
        # URL for Gigachat API
        self.url = "https://gigachat.devices.sberbank.ru/api/v1/chat/completions"
    
    def send_invoice_parsing_request(self, invoice_text: str) -> Dict[str, Any]:
 
        system_prompt = (
            "Ты — API для разбора текста счетов. Твоя задача — проанализировать входной текст счета и вернуть заполненный объект в формате JSON, "
            "содержащий информацию, такую как: номер счета, дата платежа, сумма, контрагент, назначение платежа, статус и другие релевантные данные."
        )
        
        payload = {
            "model": "GigaChat",
            "messages": [
                {
                    "role": "system",
                    "content": system_prompt
                },
                {
                    "role": "user",
                    "content": invoice_text
                }
            ],
            "stream": False,
            "update_interval": 0
        }
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {self.token}"
        }
        try:
            response = requests.post(
                self.url, 
                headers=headers, 
                data=json.dumps(payload), 
                verify=False  # Disable SSL certificate verification (use with caution!)
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logging.error(f"Error in Gigachat API request: {e}")
            raise e