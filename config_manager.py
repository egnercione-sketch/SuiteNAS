import os

# ==============================================================================
# CONFIGURAÇÃO CENTRALIZADA (PATHS & SETTINGS)
# ==============================================================================

# 1. Diretórios Base
# Pega o diretório onde este arquivo está salvo (Raiz do projeto)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(BASE_DIR, "cache")
MODULES_DIR = os.path.join(BASE_DIR, "modules")

# 2. Garante que a pasta de cache existe (Cria se não existir)
if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)

# 3. Mapeamento de Todos os Arquivos do Sistema
PATHS = {
    # --- DADOS ESSENCIAIS ---
    "L5_STATS": os.path.join(CACHE_DIR, "l5_players.pkl"),
    "SCOREBOARD": os.path.join(CACHE_DIR, "scoreboard_today.json"),
    "ODDS": os.path.join(CACHE_DIR, "odds_today.json"),
    "ODDS_PINNACLE": os.path.join(CACHE_DIR, "pinnacle_cache.json"),
    "INJURIES": os.path.join(CACHE_DIR, "injuries_cache_v44.json"),
    "REAL_LOGS": os.path.join(CACHE_DIR, "real_game_logs.json"),

    # --- DADOS DE INTELIGÊNCIA ---
    "TEAM_ADVANCED": os.path.join(CACHE_DIR, "team_advanced.json"),
    "TEAM_OPPONENT": os.path.join(CACHE_DIR, "team_opponent.json"),
    "ROTATION_DNA": os.path.join(CACHE_DIR, "rotation_dna.json"),
    "NAME_OVERRIDES": os.path.join(CACHE_DIR, "name_overrides.json"),
    "MOMENTUM_CACHE": os.path.join(CACHE_DIR, "momentum_cache.json"),

    # --- SISTEMA E ADMINISTRAÇÃO ---
    "AUDIT": os.path.join(CACHE_DIR, "audit_trixies.json"),
    "USERS_DB": os.path.join(CACHE_DIR, "users_db.json")  # <-- ONDE FICAM OS USUÁRIOS
}

# 4. Configurações Globais
CONFIG = {
    "SEASON": "2025-26",  # Temporada Simulada
    "APP_NAME": "NBA Analytics Suite",
    "VERSION": "v2.1 (RBAC Enabled)",
    # Chave Pinnacle padrão (pode ser substituída)
    "PINNACLE_API_KEY": "13e1dd2e12msh72d0553fca0e8aap16eeacjsn9d69ddb0d2bb"
}

def get_path(key):
    """Retorna o caminho absoluto de forma segura."""
    return PATHS.get(key)
