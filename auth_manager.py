import json
import os
import bcrypt
from config_manager import PATHS

class UserManager:
    def __init__(self):
        self.db_path = PATHS["USERS_DB"]
        self.users = self._load_users()
        self._ensure_admin_exists()

    def _load_users(self):
        if not os.path.exists(self.db_path):
            return {"usernames": {}}
        try:
            with open(self.db_path, 'r') as f:
                return json.load(f)
        except:
            return {"usernames": {}}

    def _save_users(self):
        with open(self.db_path, 'w') as f:
            json.dump(self.users, f, indent=4)

    def _generate_hash(self, password):
        return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

    def _ensure_admin_exists(self):
        """Cria admin padrão se o banco estiver vazio."""
        if not self.users.get("usernames"):
            hashed_password = self._generate_hash("admin123")
            self.users["usernames"] = {
                "admin": {
                    "name": "Super Administrator",
                    "password": hashed_password,
                    "permissions": ["ALL"], # ALL = Acesso Total
                    "logged_in": False,
                    "email": "admin@system.com"
                }
            }
            self._save_users()

    def create_user(self, username, name, password, permissions=None):
        """Cria usuário salvando as permissões escolhidas."""
        if username in self.users["usernames"]:
            return False, "Usuário já existe!"

        hashed_password = self._generate_hash(password)
        
        # Se não vier permissão, dá acesso apenas ao Dashboard por segurança
        if permissions is None:
            permissions = ["🏠 Dashboard"]

        self.users["usernames"][username] = {
            "name": name,
            "password": hashed_password,
            "permissions": permissions, # Salva a lista de abas
            "logged_in": False,
            "email": f"{username}@user.com"
        }
        
        self._save_users()
        return True, f"Usuário {username} criado com sucesso!"

    def update_permissions(self, username, new_permissions):
        """Atualiza permissões de um usuário existente."""
        if username not in self.users["usernames"]:
            return False, "Usuário não encontrado."
        
        self.users["usernames"][username]["permissions"] = new_permissions
        self._save_users()
        return True, "Permissões atualizadas com sucesso!"

    def get_user_permissions(self, username):
        """Retorna a lista de abas que o usuário pode ver."""
        user = self.users["usernames"].get(username, {})
        # Admin sempre tem acesso total
        if username == "admin": 
            return ["ALL"]
        return user.get("permissions", [])

    def get_all_users(self):
        """Lista todos os logins cadastrados."""
        return list(self.users["usernames"].keys())

    def get_authenticator_config(self):
        return {
            "credentials": self.users,
            "cookie": {"expiry_days": 30, "key": "random_key_nba", "name": "auth_cookie"},
            "preauthorized": {"emails": []}
        }
