# db_manager.py (Versão Upsert Debug)
import streamlit as st
import requests
import time
import json

class DatabaseManager:
    def __init__(self):
        # Tenta pegar dos Segredos
        try:
            self.url = st.secrets["SUPABASE_URL"]
            self.key = st.secrets["SUPABASE_KEY"]
            self.valid_config = True
        except:
            print("❌ ERRO: Segredos do Supabase não configurados.")
            self.url = ""
            self.key = ""
            self.valid_config = False
            return

        self.base_url = self.url.rstrip('/')
        # HEADERS CRÍTICOS:
        # 'Prefer': 'resolution=merge-duplicates' -> Força a atualização se a chave já existir (UPSERT)
        self.headers = {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates" 
        }

    def get_data(self, key_name: str):
        if not self.valid_config: return {}
        endpoint = f"{self.base_url}/rest/v1/app_cache"
        params = {"key": f"eq.{key_name}", "select": "value"}
        
        try:
            response = requests.get(endpoint, headers=self.headers, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data and len(data) > 0:
                    return data[0]['value']
            else:
                # Debug leve se falhar
                # print(f"⚠️ GET '{key_name}' falhou: {response.status_code}")
                pass
        except Exception as e:
            print(f"⚠️ Erro de Conexão (GET): {e}")
        return {}

    def save_data(self, key_name: str, data_dict: dict):
        if not self.valid_config: return False
        
        endpoint = f"{self.base_url}/rest/v1/app_cache"
        
        # Prepara o payload
        payload = {
            "key": key_name,
            "value": data_dict,
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        }
        
        try:
            # POST com header de Upsert
            response = requests.post(endpoint, headers=self.headers, json=payload, timeout=20)
            
            # CHECK DE SUCESSO REAL (200 ou 201)
            if response.status_code in [200, 201]:
                return True
            else:
                # AQUI ESTÁ O ERRO QUE ESTAVA ESCONDIDO
                print(f"❌ ERRO SUPABASE ({response.status_code}): {response.text}")
                raise Exception(f"Supabase recusou: {response.status_code}")
                
        except Exception as e:
            print(f"⚠️ Erro Crítico ao Salvar '{key_name}': {e}")
            raise e # Repassa o erro para o SuiteNAS saber que falhou

# Instância única
db = DatabaseManager()
