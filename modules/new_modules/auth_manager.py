import json
import os
import streamlit_authenticator as stauth
from config_manager import PATHS

class UserManager:
    def __init__(self):
        self.db_path = PATHS["USERS_DB"]
        self.users = self._load_users()

    def _load_users(self):
        """Carrega usuários do arquivo JSON."""
        if not os.path.exists(self.db_path):
            return {"usernames": {}}
        try:
            with open(self.db_path, 'r') as f:
                return json.load(f)
        except:
            return {"usernames": {}}

    def _save_users(self):
        """Salva alterações no arquivo JSON."""
        with open(self.db_path, 'w') as f:
            json.dump(self.users, f, indent=4)

    def create_user(self, username, name, password):
        """Cria um novo usuário e salva no banco."""
        if username in self.users["usernames"]:
            return False, "Usuário já existe!"

        # Gera o Hash da senha (Criptografia)
        hashed_password = stauth.Hasher([password]).generate()[0]

        self.users["usernames"][username] = {
            "name": name,
            "password": hashed_password,
            "email": f"{username}@exemplo.com", # Opcional
            "logged_in": False
        }
        
        self._save_users()
        return True, f"Usuário {username} criado com sucesso!"

    def get_authenticator_config(self):
        """Retorna a configuração pronta para o stauth.Authenticate"""
        return {
            "credentials": self.users,
            "cookie": {"expiry_days": 30, "key": "random_signature_key", "name": "nba_auth"},
            "preauthorized": {"emails": []}
        }
