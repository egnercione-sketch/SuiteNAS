import streamlit as st
import bcrypt
from supabase import create_client, Client

# Tenta conectar ao Supabase usando os Segredos do Streamlit
try:
    SUPABASE_URL = st.secrets["supabase"]["url"]
    SUPABASE_KEY = st.secrets["supabase"]["key"]
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    DB_CONNECTED = True
except Exception as e:
    DB_CONNECTED = False
    print(f"Erro ao conectar Supabase: {e}")

class UserManager:
    def __init__(self):
        # Carrega usuários do banco ao iniciar a classe
        self.users_cache = self._load_users_from_db()
        self._ensure_admin_exists()

    def _generate_hash(self, password):
        """Gera hash seguro com bcrypt."""
        return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

    def _load_users_from_db(self):
        """
        Lê a tabela 'users' do Supabase e converte para o formato
        que o Streamlit Authenticator exige (Dicionário aninhado).
        """
        formatted_users = {"usernames": {}}
        
        if not DB_CONNECTED:
            return formatted_users

        try:
            # Select * from users
            response = supabase.table("users").select("*").execute()
            data = response.data
            
            for row in data:
                formatted_users["usernames"][row["username"]] = {
                    "name": row["name"],
                    "password": row["password"],
                    "permissions": row["permissions"], # O Supabase já devolve JSON como lista
                    "email": row["email"],
                    "logged_in": False
                }
            return formatted_users
        except Exception as e:
            st.error(f"Erro ao ler banco de dados: {e}")
            return formatted_users

    def _ensure_admin_exists(self):
        """Cria admin no Supabase se a tabela estiver vazia."""
        if not self.users_cache["usernames"] and DB_CONNECTED:
            hashed_password = self._generate_hash("admin123")
            
            new_user_data = {
                "username": "admin",
                "name": "Super Administrator",
                "password": hashed_password,
                "permissions": ["ALL"],
                "email": "admin@system.com"
            }
            
            try:
                supabase.table("users").insert(new_user_data).execute()
                # Atualiza cache local
                self.users_cache = self._load_users_from_db()
                print("⚠️ Usuário Admin criado no Supabase.")
            except Exception as e:
                print(f"Erro ao criar admin: {e}")

    def create_user(self, username, name, password, permissions=None):
        if not DB_CONNECTED:
            return False, "Erro: Banco de dados desconectado."

        if username in self.users_cache["usernames"]:
            return False, "Usuário já existe!"

        hashed_password = self._generate_hash(password)
        if permissions is None:
            permissions = ["🏠 Dashboard"]

        new_user = {
            "username": username,
            "name": name,
            "password": hashed_password,
            "permissions": permissions,
            "email": f"{username}@cliente.com"
        }

        try:
            # Envia para o Supabase
            supabase.table("users").insert(new_user).execute()
            
            # Atualiza a memória local para não precisar recarregar página
            self.users_cache["usernames"][username] = {
                "name": name,
                "password": hashed_password,
                "permissions": permissions,
                "email": new_user["email"],
                "logged_in": False
            }
            return True, f"Usuário {username} criado na nuvem!"
        except Exception as e:
            return False, f"Erro ao gravar no banco: {e}"

    def update_permissions(self, username, new_permissions):
        if not DB_CONNECTED:
            return False, "Banco desconectado."

        try:
            # Atualiza no Supabase
            supabase.table("users").update({"permissions": new_permissions}).eq("username", username).execute()
            
            # Atualiza memória local
            if username in self.users_cache["usernames"]:
                self.users_cache["usernames"][username]["permissions"] = new_permissions
                
            return True, "Permissões atualizadas e salvas na nuvem!"
        except Exception as e:
            return False, f"Erro ao atualizar: {e}"

    def get_user_permissions(self, username):
        user = self.users_cache["usernames"].get(username, {})
        if username == "admin": return ["ALL"]
        return user.get("permissions", [])

    def get_all_users(self):
        # Recarrega do banco para garantir que a lista está fresca
        self.users_cache = self._load_users_from_db()
        return list(self.users_cache["usernames"].keys())

    def get_authenticator_config(self):
        return {
            "credentials": self.users_cache,
            "cookie": {"expiry_days": 30, "key": "nba_suite_cloud_key", "name": "nba_auth_cookie"},
            "preauthorized": {"emails": []}
        }
