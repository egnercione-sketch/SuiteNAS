import os

# Configurações Globais
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE_DIR = os.path.join(BASE_DIR, "cache")

# Arquivos de Dados
TEAM_ADVANCED_FILE = os.path.join(CACHE_DIR, "team_advanced.json")
SCOREBOARD_FILE = os.path.join(CACHE_DIR, "scoreboard_today.json")

# Constantes
DEFAULT_SEASON = "2025-26"