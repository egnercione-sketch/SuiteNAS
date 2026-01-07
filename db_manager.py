# ============================================================================
# DB MANAGER (VERSÃO OFICIAL SUPABASE - CORRIGIDA)
# ============================================================================
import streamlit as st
from supabase import create_client, Client
from datetime import datetime

class DatabaseHandler:
    def __init__(self):
        self.client = None
        self.connected = False
        try:
            # Tenta pegar as credenciais do secrets
            if "supabase" in st.secrets:
                # Suporta tanto st.secrets["supabase"]["url"] quanto st.secrets["SUPABASE_URL"]
                try:
                    url = st.secrets["supabase"]["url"]
                    key = st.secrets["supabase"]["key"]
                except:
                    url = st.secrets["SUPABASE_URL"]
                    key = st.secrets["SUPABASE_KEY"]
                
                self.client: Client = create_client(url, key)
                self.connected = True
                print("🔌 Supabase (Handler) Conectado com Sucesso!")
            else:
                print("⚠️ Secrets do Supabase não encontrados.")
        except Exception as e:
            print(f"❌ Erro ao conectar Supabase: {e}")

    def get_data(self, key):
        """Busca o valor JSON dentro da tabela app_cache pela chave"""
        if not self.connected: return None
        try:
            # SELECT value FROM app_cache WHERE key = 'chave'
            response = self.client.table("app_cache").select("value").eq("key", key).execute()
            
            # Verifica se retornou dados
            if response.data and len(response.data) > 0:
                return response.data[0]['value']
            else:
                return None
        except Exception as e:
            print(f"⚠️ Erro ao buscar '{key}' no DB: {e}")
            return None

    def save_data(self, key, value):
        """Salva (Upsert) o valor JSON na tabela app_cache"""
        if not self.connected: return False
        try:
            # Prepara o payload
            payload = {
                "key": key,
                "value": value,
                "last_updated": datetime.now().isoformat()
            }
            # UPSERT (Atualiza se existir, cria se não existir)
            self.client.table("app_cache").upsert(payload).execute()
            return True
        except Exception as e:
            print(f"❌ Erro Supabase Save: {e}")
            raise e

# Instância única para ser importada
try:
    db = DatabaseHandler()
    if not db.connected:
        db = None
except:
    db = None
