# db_manager.py
import os
import json
import streamlit as st
from supabase import create_client, Client

# Tenta pegar segredos do Streamlit Cloud ou variáveis de ambiente locais
try:
    URL = st.secrets["SUPABASE_URL"]
    KEY = st.secrets["SUPABASE_KEY"]
except:
    # Fallback para teste local (Preencha aqui se não usar secrets.toml)
    URL = os.getenv("https://uarbkuwiozqicaxulyyr.supabase.co")
    KEY = os.getenv("sb_publishable_lCDHC5FksGYVEyF4kvGDYg_3uYH3x0P")

class DatabaseManager:
    def __init__(self):
        self.supabase: Client = create_client(URL, KEY)

    def get_data(self, key_name: str):
        """
        Baixa o JSON do Supabase.
        Substitui: json.load(open('arquivo.json'))
        """
        try:
            response = self.supabase.table("app_cache").select("value").eq("key", key_name).execute()
            if response.data and len(response.data) > 0:
                print(f"✅ Cache '{key_name}' carregado do Supabase.")
                return response.data[0]['value']
            else:
                print(f"⚠️ Cache '{key_name}' não encontrado no banco.")
                return {}
        except Exception as e:
            print(f"❌ Erro ao baixar '{key_name}': {e}")
            return {}

    def save_data(self, key_name: str, data_dict: dict):
        """
        Sobe o JSON para o Supabase.
        Substitui: json.dump(data, open('arquivo.json'))
        """
        try:
            payload = {
                "key": key_name,
                "value": data_dict,
                "updated_at": "now()"
            }
            # Upsert = Atualiza se existir, Cria se não existir
            self.supabase.table("app_cache").upsert(payload).execute()
            print(f"✅ Cache '{key_name}' salvo no Supabase com sucesso.")
        except Exception as e:
            print(f"❌ Erro ao salvar '{key_name}': {e}")

# Instância global para ser importada
db = DatabaseManager()
