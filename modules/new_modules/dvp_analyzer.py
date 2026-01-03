# modules/new_modules/dvp_analyzer.py
# v2.0 - CLOUD NATIVE EDITION ☁️
# Integração total com Supabase e remoção de cache local.

import os
import json
import pandas as pd
import requests
import time
from datetime import datetime

# Tenta importar o banco de dados
try:
    from db_manager import db
except ImportError:
    db = None
    print("⚠️ [DvP] db_manager não encontrado. Rodando em modo Offline.")

# ==============================================================================
# CONFIGURAÇÕES & DADOS ESTÁTICOS
# ==============================================================================
# Rank 30 = Pior Defesa (Alvo Verde/Over) | Rank 1 = Melhor Defesa (Alvo Vermelho/Under)
STATIC_BACKUP_DATA = {
    "WAS": {"PG": 29, "SG": 30, "SF": 28, "PF": 30, "C": 30}, # Defesa Terrível
    "UTA": {"PG": 28, "SG": 29, "SF": 30, "PF": 28, "C": 26}, # Defesa Ruim
    "CHA": {"PG": 25, "SG": 26, "SF": 24, "PF": 27, "C": 29}, 
    "DET": {"PG": 26, "SG": 25, "SF": 22, "PF": 26, "C": 24},
    "ATL": {"PG": 27, "SG": 28, "SF": 25, "PF": 29, "C": 18},
    "CHI": {"PG": 24, "SG": 27, "SF": 26, "PF": 20, "C": 22},
    "TOR": {"PG": 23, "SG": 24, "SF": 27, "PF": 25, "C": 25},
    "POR": {"PG": 22, "SG": 23, "SF": 21, "PF": 24, "C": 28},
    "NOP": {"PG": 20, "SG": 22, "SF": 23, "PF": 18, "C": 21},
    "IND": {"PG": 19, "SG": 21, "SF": 20, "PF": 28, "C": 23}, 
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
    "SAS": {"PG": 15, "SG": 17, "SF": 16, "PF": 21, "C": 8},
    "MIL": {"PG": 20, "SG": 21, "SF": 18, "PF": 22, "C": 16},
    "NYK": {"PG": 7, "SG": 6, "SF": 8, "PF": 7, "C": 6},
    "CLE": {"PG": 6, "SG": 5, "SF": 6, "PF": 6, "C": 5},
    "MIN": {"PG": 5, "SG": 4, "SF": 5, "PF": 3, "C": 4},
    "BOS": {"PG": 4, "SG": 3, "SF": 3, "PF": 2, "C": 3},
    "OKC": {"PG": 2, "SG": 2, "SF": 2, "PF": 4, "C": 2},
    "ORL": {"PG": 1, "SG": 1, "SF": 1, "PF": 1, "C": 1}
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

DATA_URL = "https://hashtagbasketball.com/nba-defense-vs-position"

class DvPAnalyzer:
    def __init__(self, force_update=False):
        self.defense_data = {}
        self.key = "dvp_data"
        
        # 1. Tenta carregar da Nuvem (Supabase)
        loaded_from_cloud = False
        if not force_update and db:
            cloud_data = db.get_data(self.key)
            if cloud_data and isinstance(cloud_data, dict) and len(cloud_data) > 5:
                self.defense_data = cloud_data
                loaded_from_cloud = True
                # print("☁️ [DvP] Dados carregados do Supabase.")

        # 2. Se falhar ou estiver vazio, usa o Backup Estático
        if not loaded_from_cloud:
            # print("⚠️ [DvP] Usando dados estáticos (Backup).")
            self.defense_data = STATIC_BACKUP_DATA
            
            # Tenta salvar o estático na nuvem para a próxima vez ser mais rápida
            if db:
                try: db.save_data(self.key, STATIC_BACKUP_DATA)
                except: pass

    def update_data(self):
        """
        Raspa dados novos da Web e salva no Supabase.
        Retorna True se sucesso.
        """
        print("🔄 DvP: Iniciando atualização via Web...")
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            }
            response = requests.get(DATA_URL, headers=headers, timeout=10)
            
            if response.status_code != 200:
                print(f"❌ Erro HTTP {response.status_code}")
                return False

            # Pandas lê as tabelas HTML
            dfs = pd.read_html(response.text)
            if not dfs: return False
            
            # Pega a maior tabela (geralmente é a de dados)
            df = max(dfs, key=lambda x: len(x.columns))
            
            # Encontra coluna de time
            team_col = next((c for c in df.columns if "Team" in str(c)), None)
            if not team_col: return False
            
            # Normaliza siglas
            df['Abbr'] = df[team_col].apply(lambda x: self._get_abbr(str(x)))
            df = df[df['Abbr'] != "UNK"].copy()
            
            final_data = {abbr: {} for abbr in df['Abbr'].unique()}
            positions = ["PG", "SG", "SF", "PF", "C"]
            
            # Processa Ranks
            for pos in positions:
                # Busca coluna que contenha a posição (ex: "PG", "PG Stats", etc)
                target_col = next((c for c in df.columns if pos == str(c).strip() or f"{pos} " in str(c)), None)
                
                if target_col:
                    df[target_col] = pd.to_numeric(df[target_col], errors='coerce')
                    # Cria Rank (Menor número = Cede menos pontos = Rank 1 = Vermelho)
                    # No nosso sistema: Rank 30 = Cede MUITOS pontos (Verde)
                    # O rank padrão do pandas é 1 = Menor valor.
                    # Se a tabela for "Pontos Cedidos", quem cede MAIS pontos deve ser Rank 30.
                    # Então Rank Ascendente está correto (Valor baixo = Rank baixo).
                    df[f'{pos}_Rank'] = df[target_col].rank(method='min', ascending=True)
                    
                    for _, row in df.iterrows():
                        final_data[row['Abbr']][pos] = int(row[f'{pos}_Rank'])
                else:
                    print(f"⚠️ Coluna para {pos} não encontrada.")
                    return False

            self.defense_data = final_data
            
            # SALVA NO SUPABASE
            if db:
                db.save_data(self.key, self.defense_data)
                print("✅ [DvP] Dados atualizados e salvos na nuvem.")
            
            return True

        except Exception as e:
            print(f"❌ Erro ao atualizar DvP: {e}")
            return False

    def _get_abbr(self, raw_name):
        raw_name = str(raw_name).strip()
        # Tenta mapear nome completo
        for city, abbr in TEAM_MAPPING.items():
            if city in raw_name: return abbr
        # Tenta ver se já é a sigla
        if raw_name.upper() in TEAM_MAPPING.values(): return raw_name.upper()
        return "UNK"

    def get_position_rank(self, team_abbr, position):
        if not self.defense_data: return 15
        
        abbr = self._get_abbr(team_abbr)
        if abbr == "UNK": abbr = str(team_abbr).upper()
        
        # Correção para NOP/NO e UTA/UTAH
        if abbr == "NO": abbr = "NOP"
        if abbr == "UTAH": abbr = "UTA"
        if abbr == "GS": abbr = "GSW"
        if abbr == "NY": abbr = "NYK"
        if abbr == "SA": abbr = "SAS"
        
        stats = self.defense_data.get(abbr, {})
        return stats.get(position, 15) # Retorna 15 (Neutro) se falhar
