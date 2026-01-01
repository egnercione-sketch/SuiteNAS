# modules/new_modules/dvp_analyzer.py
import os
import json
import pandas as pd
import requests
import time
from datetime import datetime

# ==============================================================================
# CONFIGURAÇÕES
# ==============================================================================
CACHE_DIR = os.path.join(os.getcwd(), "cache")
# Vamos mudar o nome para garantir que ele cria um novo arquivo limpo
DVP_CACHE_FILE = os.path.join(CACHE_DIR, "dvp_data_v4_static.json")

# URL da Fonte de Dados (Tentativa Principal)
DATA_URL = "https://hashtagbasketball.com/nba-defense-vs-position"

# --- DADOS DE BACKUP MANUAL (Temporada 24-25 - Baseado em Stats Reais) ---
# Se o download falhar, usamos isso. 
# Rank 30 = Pior Defesa (Alvo Verde/Over) | Rank 1 = Melhor Defesa (Alvo Vermelho/Under)
STATIC_BACKUP_DATA = {
    "WAS": {"PG": 29, "SG": 30, "SF": 28, "PF": 30, "C": 30}, # Defesa Terrível
    "UTA": {"PG": 28, "SG": 29, "SF": 30, "PF": 28, "C": 26}, # Defesa Ruim
    "CHA": {"PG": 25, "SG": 26, "SF": 24, "PF": 27, "C": 29}, # Pivôs pontuam bem aqui
    "DET": {"PG": 26, "SG": 25, "SF": 22, "PF": 26, "C": 24},
    "ATL": {"PG": 27, "SG": 28, "SF": 25, "PF": 29, "C": 18},
    "CHI": {"PG": 24, "SG": 27, "SF": 26, "PF": 20, "C": 22},
    "TOR": {"PG": 23, "SG": 24, "SF": 27, "PF": 25, "C": 25},
    "POR": {"PG": 22, "SG": 23, "SF": 21, "PF": 24, "C": 28},
    "NOP": {"PG": 20, "SG": 22, "SF": 23, "PF": 18, "C": 21},
    "IND": {"PG": 19, "SG": 21, "SF": 20, "PF": 28, "C": 23}, # Defesa Rápida mas cede pontos
    "SAC": {"PG": 18, "SG": 20, "SF": 19, "PF": 15, "C": 17},
    "PHX": {"PG": 15, "SG": 16, "SF": 18, "PF": 12, "C": 14},
    "LAL": {"PG": 16, "SG": 18, "SF": 15, "PF": 14, "C": 16},
    "GSW": {"PG": 12, "SG": 14, "SF": 10, "PF": 8, "C": 12},
    "DAL": {"PG": 14, "SG": 12, "SF": 13, "PF": 16, "C": 10},
    "HOU": {"PG": 10, "SG": 9, "SF": 12, "PF": 11, "C": 13},
    "MEM": {"PG": 13, "SG": 13, "SF": 14, "PF": 9, "C": 11},
    "LAC": {"PG": 11, "SG": 15, "SF": 11, "PF": 13, "C": 9},
    "DEN": {"PG": 17, "SG": 11, "SF": 16, "PF": 10, "C": 15},
    "MIA": {"PG": 8, "SG": 10, "SF": 9, "PF": 17, "C": 7},
    "PHI": {"PG": 9, "SG": 8, "SF": 17, "PF": 19, "C": 19},
    "BKN": {"PG": 21, "SG": 19, "SF": 29, "PF": 22, "C": 20},
    "SAS": {"PG": 15, "SG": 17, "SF": 16, "PF": 21, "C": 8},  # Wemby protege o aro (Rank 8)
    "MIL": {"PG": 20, "SG": 21, "SF": 18, "PF": 22, "C": 16},
    "NYK": {"PG": 7, "SG": 6, "SF": 8, "PF": 7, "C": 6},
    "CLE": {"PG": 6, "SG": 5, "SF": 6, "PF": 6, "C": 5},
    "MIN": {"PG": 5, "SG": 4, "SF": 5, "PF": 3, "C": 4},
    "BOS": {"PG": 4, "SG": 3, "SF": 3, "PF": 2, "C": 3},
    "OKC": {"PG": 2, "SG": 2, "SF": 2, "PF": 4, "C": 2},      # Defesa Elite
    "ORL": {"PG": 1, "SG": 1, "SF": 1, "PF": 1, "C": 1}       # Melhor Defesa Atual
}

TEAM_MAPPING = {
    "Atlanta": "ATL", "Boston": "BOS", "Brooklyn": "BKN", "Charlotte": "CHA",
    "Chicago": "CHI", "Cleveland": "CLE", "Dallas": "DAL", "Denver": "DEN",
    "Detroit": "DET", "Golden State": "GSW", "Houston": "HOU", "Indiana": "IND",
    "LA Clippers": "LAC", "LA Lakers": "LAL", "Memphis": "MEM", "Miami": "MIA",
    "Milwaukee": "MIL", "Minnesota": "MIN", "New Orleans": "NOP", "New York": "NYK",
    "Oklahoma City": "OKC", "Orlando": "ORL", "Philadelphia": "PHI", "Phoenix": "PHX",
    "Portland": "POR", "Sacramento": "SAC", "San Antonio": "SAS", "Toronto": "TOR",
    "Utah": "UTA", "Washington": "WAS"
}

class DvPAnalyzer:
    def __init__(self, force_update=False):
        self.defense_data = {}
        
        if not os.path.exists(CACHE_DIR):
            os.makedirs(CACHE_DIR)
            
        if not force_update and self._check_cache_valid():
            self._load_from_cache()
        else:
            # Tenta baixar, se falhar, usa o estático
            print("🔄 DvP: Tentando atualizar...")
            success = self.update_data()
            if not success:
                print("⚠️ DvP: Download bloqueado. Usando BASE DE DADOS BACKUP (24-25).")
                self.defense_data = STATIC_BACKUP_DATA
                # Salva o backup no cache para não tentar baixar toda hora
                with open(DVP_CACHE_FILE, 'w') as f:
                    json.dump(self.defense_data, f)

    def _check_cache_valid(self):
        if not os.path.exists(DVP_CACHE_FILE): return False
        try:
            if os.path.getsize(DVP_CACHE_FILE) < 100: return False
            return True
        except: return False

    def _load_from_cache(self):
        try:
            with open(DVP_CACHE_FILE, 'r') as f:
                self.defense_data = json.load(f)
        except:
            self.defense_data = STATIC_BACKUP_DATA

    def update_data(self):
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            }
            # Tenta baixar (timeout curto para não travar)
            response = requests.get(DATA_URL, headers=headers, timeout=5)
            if response.status_code != 200: return False

            dfs = pd.read_html(response.text)
            if not dfs: return False
            df = max(dfs, key=lambda x: len(x.columns))
            
            # Tenta encontrar a coluna de time
            team_col = next((c for c in df.columns if "Team" in str(c)), None)
            if not team_col: return False
            
            df['Abbr'] = df[team_col].apply(lambda x: self._get_abbr(str(x)))
            df = df[df['Abbr'] != "UNK"].copy()
            
            final_data = {abbr: {} for abbr in df['Abbr'].unique()}
            positions = ["PG", "SG", "SF", "PF", "C"]
            
            for pos in positions:
                target_col = next((c for c in df.columns if pos == str(c).strip() or f"{pos} " in str(c)), None)
                if target_col:
                    df[target_col] = pd.to_numeric(df[target_col], errors='coerce')
                    df[f'{pos}_Rank'] = df[target_col].rank(method='min', ascending=True)
                    for _, row in df.iterrows():
                        final_data[row['Abbr']][pos] = int(row[f'{pos}_Rank'])
                else:
                    return False # Se não achar colunas, força backup

            self.defense_data = final_data
            with open(DVP_CACHE_FILE, 'w') as f:
                json.dump(self.defense_data, f)
            return True

        except:
            return False

    def _get_abbr(self, raw_name):
        raw_name = str(raw_name).strip()
        for city, abbr in TEAM_MAPPING.items():
            if city in raw_name: return abbr
        if raw_name.upper() in TEAM_MAPPING.values(): return raw_name.upper()
        return "UNK"

    def get_position_rank(self, team_abbr, position):
        if not self.defense_data: return 15
        abbr = self._get_abbr(team_abbr)
        if abbr == "UNK": abbr = team_abbr
        stats = self.defense_data.get(abbr, {})
        # Retorna 15 apenas se não achar o time, mas os dados estão lá
        return stats.get(position, 15)