import json
import os
import bcrypt  # Usaremos a biblioteca padrão de criptografia
from config_manager import PATHS

class UserManager:
    def __init__(self):
        self.db_path = PATHS["USERS_DB"]
        # Carrega usuários. Se não existir, cria o Admin padrão.
        self.users = self._load_users()
        self._ensure_admin_exists()

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

    def _generate_hash(self, password):
        """
        Gera o hash da senha usando bcrypt diretamente.
        Isso evita erros de versão do streamlit-authenticator.
        """
        return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

    def _ensure_admin_exists(self):
        """
        Cria um usuário ADMIN padrão se não houver nenhum usuário no sistema.
        """
        if not self.users.get("usernames"):
            # Senha padrão: admin123
            # FIX: Usando bcrypt direto, impossível dar TypeError
            hashed_password = self._generate_hash("admin123")
            
            self.users["usernames"] = {
                "admin": {
                    "name": "Super Administrator",
                    "password": hashed_password,
                    "permissions": ["ALL"], # Acesso total
                    "logged_in": False,
                    "email": "admin@system.com"
                }
            }
            self._save_users()
            print("⚠️ [SISTEMA] Usuário 'admin' criado automaticamente (Senha: admin123)")

    def create_user(self, username, name, password, permissions=None):
        """Cria usuário com lista de permissões."""
        if username in self.users["usernames"]:
            return False, "Usuário já existe!"

        # Gera o Hash da senha
        hashed_password = self._generate_hash(password)
        
        # Se não passar permissões, dá acesso básico
        if permissions is None:
            permissions = ["🏠 Dashboard"]

        self.users["usernames"][username] = {
            "name": name,
            "password": hashed_password,
            "permissions": permissions,
            "logged_in": False,
            "email": f"{username}@user.com"
        }
        
        self._save_users()
        return True, f"Usuário {username} criado com sucesso!"

    def update_permissions(self, username, new_permissions):
        """Atualiza as permissões de um usuário existente."""
        if username not in self.users["usernames"]:
            return False, "Usuário não encontrado."
            
        self.users["usernames"][username]["permissions"] = new_permissions
        self._save_users()
        return True, "Permissões atualizadas!"

    def get_user_permissions(self, username):
        """Retorna a lista de abas permitidas para o usuário."""
        user = self.users["usernames"].get(username, {})
        # Admin mestre sempre vê tudo
        if username == "admin": 
            return ["ALL"]
        return user.get("permissions", [])

    def get_all_users(self):
        """Retorna lista de todos os usernames."""
        return list(self.users["usernames"].keys())

    def get_authenticator_config(self):
        return {
            "credentials": self.users,
            "cookie": {"expiry_days": 30, "key": "nba_suite_super_secret_key", "name": "nba_auth_cookie"},
            "preauthorized": {"emails": []}
        }
