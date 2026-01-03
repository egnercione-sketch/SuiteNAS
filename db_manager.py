# db_manager.py
import streamlit as st
import requests
import time

class DatabaseManager:
    def __init__(self):
        # Tenta pegar dos Segredos do Streamlit (Nuvem)
        try:
            self.url = st.secrets["SUPABASE_URL"]
            self.key = st.secrets["SUPABASE_KEY"]
        except:
            # Se não achar, avisa (evita crash silencioso)
            print("❌ ERRO: Segredos do Supabase não configurados no Streamlit Cloud.")
            self.url = ""
            self.key = ""
            return

        self.base_url = self.url.rstrip('/')
        self.headers = {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates"
        }

    def get_data(self, key_name: str):
        if not self.url: return {}
        endpoint = f"{self.base_url}/rest/v1/app_cache"
        params = {"key": f"eq.{key_name}", "select": "value"}
        
        try:
            response = requests.get(endpoint, headers=self.headers, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data: return data[0]['value']
        except Exception as e:
            print(f"⚠️ Erro Supabase GET: {e}")
        return {}

    def save_data(self, key_name: str, data_dict: dict):
        if not self.url: return
        endpoint = f"{self.base_url}/rest/v1/app_cache"
        payload = {
            "key": key_name,
            "value": data_dict,
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        }
        try:
            requests.post(endpoint, headers=self.headers, json=payload, timeout=10)
        except Exception as e:
            print(f"⚠️ Erro Supabase SAVE: {e}")

# Instância única
db = DatabaseManager()
