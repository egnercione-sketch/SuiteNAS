# ============================================================================
# DB MANAGER (CORRIGIDO PARA LER SECRETS DIRETAS)
# ============================================================================
import streamlit as st
from supabase import create_client, Client
from datetime import datetime

class DatabaseHandler:
    def __init__(self):
        self.client = None
        self.connected = False
        
        # Tenta conectar usando as chaves configuradas
        try:
            # 1. Tenta ler chaves diretas (Formato que você está usando)
            if "SUPABASE_URL" in st.secrets and "SUPABASE_KEY" in st.secrets:
                url = st.secrets["SUPABASE_URL"]
                key = st.secrets["SUPABASE_KEY"]
            
            # 2. Fallback: Tenta ler chaves aninhadas (Formato TOML [supabase])
            elif "supabase" in st.secrets:
                url = st.secrets["supabase"]["url"]
                key = st.secrets["supabase"]["key"]
            
            else:
                print("⚠️ Secrets do Supabase NÃO encontradas. Configure SUPABASE_URL e SUPABASE_KEY.")
                return

            # Cria o cliente
            self.client: Client = create_client(url, key)
            self.connected = True
            print("🔌 Supabase Conectado com Sucesso!")
            
        except Exception as e:
            print(f"❌ Erro Crítico ao conectar Supabase: {e}")
            self.connected = False

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
            # UPSERT
            self.client.table("app_cache").upsert(payload).execute()
            return True
        except Exception as e:
            print(f"❌ Erro Supabase Save: {e}")
            raise e

# Instância única para ser importada pelo SuiteNAS.py
try:
    db = DatabaseHandler()
    if not db.connected:
        print("⚠️ Aviso: DatabaseHandler falhou na inicialização.")
        db = None
except Exception as e:
    print(f"❌ Erro fatal ao instanciar DatabaseHandler: {e}")
    db = None
