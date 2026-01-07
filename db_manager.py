# ============================================================================
# DB MANAGER (VERSÃO BLINDADA CONTRA ERRO 400)
# ============================================================================
import streamlit as st
from supabase import create_client, Client
from datetime import datetime
import json
import math

class DatabaseHandler:
    def __init__(self):
        self.client = None
        self.connected = False
        
        try:
            # 1. Tenta ler chaves diretas
            if "SUPABASE_URL" in st.secrets and "SUPABASE_KEY" in st.secrets:
                url = st.secrets["SUPABASE_URL"]
                key = st.secrets["SUPABASE_KEY"]
            # 2. Fallback para chaves aninhadas
            elif "supabase" in st.secrets:
                url = st.secrets["supabase"]["url"]
                key = st.secrets["supabase"]["key"]
            else:
                print("⚠️ Secrets do Supabase NÃO encontradas.")
                return

            self.client: Client = create_client(url, key)
            self.connected = True
            print("🔌 Supabase Conectado!")
            
        except Exception as e:
            print(f"❌ Erro Crítico Conexão: {e}")
            self.connected = False

    def get_data(self, key):
        """Busca o valor JSON dentro da tabela app_cache"""
        if not self.connected: return None
        try:
            response = self.client.table("app_cache").select("value").eq("key", key).execute()
            if response.data and len(response.data) > 0:
                return response.data[0]['value']
            return None
        except Exception as e:
            print(f"⚠️ Erro GET '{key}': {e}")
            return None

    def save_data(self, key, value):
        """Salva (Upsert) com tratamento de JSON inválido (NaN/Dates)"""
        if not self.connected: return False
        try:
            # --- TRATAMENTO DE DADOS (CRÍTICO PARA EVITAR ERRO 400) ---
            # O Supabase rejeita NaN (Not a Number) e objetos datetime puros dentro do JSONB
            
            # 1. Converter para string JSON usando um encoder inteligente
            # Isso limpa NaNs, datas e tipos estranhos do Pandas/Numpy
            clean_json_str = json.dumps(value, default=str).replace("NaN", "null").replace("Infinity", "null")
            
            # 2. Carregar de volta para dict limpo
            clean_value = json.loads(clean_json_str)

            # Prepara o payload
            payload = {
                "key": key,
                "value": clean_value, # Envia o dict já limpo
                "last_updated": datetime.now().isoformat()
            }
            
            # UPSERT (on_conflict na coluna 'key')
            # O Supabase client ja entende upsert pela PK, mas garantir dados limpos é o segredo
            self.client.table("app_cache").upsert(payload).execute()
            
            print(f"✅ Salvo com sucesso: {key}")
            return True
            
        except Exception as e:
            # Mostra o erro real para debug
            print(f"❌ Erro SAVE '{key}' (Provável JSON Inválido): {e}")
            raise e

# Instância única
try:
    db = DatabaseHandler()
    if not db.connected:
        print("⚠️ Aviso: DatabaseHandler falhou na inicialização.")
        db = None
except Exception as e:
    print(f"❌ Erro fatal DB: {e}")
    db = None
