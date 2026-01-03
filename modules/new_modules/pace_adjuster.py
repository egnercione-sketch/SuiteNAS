# pace_adjuster.py
# v2.0 - CLOUD NATIVE & COMPATIBILITY FIX
# Ajusta estatísticas baseadas no ritmo do jogo (Pace) em tempo real via Supabase.

import json
import os

# Tenta importar db_manager
try:
    from db_manager import db
except ImportError:
    db = None
    print("⚠️ [Pace] db_manager não encontrado. Usando dados estáticos.")

# --- PACE MÉDIO DA LIGA (Referência 2024-25) ---
LEAGUE_AVERAGE_PACE = 99.5 

# --- FALLBACK DE SEGURANÇA (Caso o banco falhe) ---
DEFAULT_PACE_DATA = {
    "WAS": 103.5, "IND": 102.5, "ATL": 101.5, "SAS": 101.0, "GSW": 100.5,
    "DET": 100.0, "MIL": 100.0, "LAL": 100.5, "OKC": 100.5, "UTA": 99.5,
    "DAL": 99.0,  "TOR": 99.5,  "BOS": 98.5,  "MEM": 99.0,  "SAC": 100.0,
    "PHI": 98.0,  "PHX": 99.0,  "HOU": 100.0, "CHA": 98.5,  "BKN": 98.0,
    "CLE": 97.5,  "LAC": 97.5,  "NOP": 98.0,  "MIA": 96.5,  "DEN": 97.0,
    "ORL": 97.5,  "CHI": 97.0,  "MIN": 96.5,  "NYK": 96.0,  "POR": 97.5
}

class PaceAdjuster:
    def __init__(self, data_source=None):
        """
        Inicializa o PaceAdjuster com prioridade para dados da Nuvem (Supabase).
        """
        self.pace_data = {}
        loaded = False
        
        # 1. Tenta carregar do Supabase (Chave: 'team_advanced')
        if db:
            try:
                cloud_data = db.get_data("team_advanced")
                if cloud_data:
                    self.pace_data = self._parse_data(cloud_data)
                    if self.pace_data:
                        loaded = True
                        # print(f"☁️ [Pace] Dados carregados da nuvem ({len(self.pace_data)} times).")
            except Exception as e:
                print(f"⚠️ Erro ao carregar Pace da nuvem: {e}")

        # 2. Se falhar, tenta data_source local (se passado)
        if not loaded and data_source:
            if isinstance(data_source, dict):
                self.pace_data = self._parse_data(data_source)
                loaded = True
            elif isinstance(data_source, str) and os.path.exists(data_source):
                try:
                    with open(data_source, 'r') as f:
                        self.pace_data = self._parse_data(json.load(f))
                        loaded = True
                except: pass
        
        # 3. Fallback Final: Dados Estáticos
        if not loaded or not self.pace_data:
            self.pace_data = DEFAULT_PACE_DATA
            # print("⚠️ [Pace] Usando dados estáticos de fallback.")

    def _parse_data(self, data):
        """
        Extrai apenas o PACE do JSON complexo (seja lista ou dict).
        """
        pace_map = {}
        
        # Caso 1: Lista de registros (padrão NBA API / Supabase salvo como records)
        if isinstance(data, list):
            for item in data:
                # Tenta várias chaves possíveis
                team = item.get('TEAM_ABBREVIATION') or item.get('TEAM_NAME') or item.get('team') or item.get('TEAM')
                pace = item.get('PACE') or item.get('pace') or item.get('E_PACE')
                
                if team and pace:
                    # Normaliza sigla (ATL, BOS...)
                    clean_team = str(team).upper().strip()
                    try:
                        pace_map[clean_team] = float(pace)
                    except: pass
                    
        # Caso 2: Dicionário Direto { "ATL": 100.5, ... }
        elif isinstance(data, dict):
            for team, val in data.items():
                clean_team = str(team).upper().strip()
                
                if isinstance(val, (int, float)):
                    pace_map[clean_team] = float(val)
                elif isinstance(val, dict):
                    # Se for aninhado { "ATL": {"PACE": 100.5} }
                    pace = val.get('PACE') or val.get('pace')
                    if pace:
                        pace_map[clean_team] = float(pace)
                        
        return pace_map

    def get_team_pace(self, team_abbr):
        """Retorna o PACE bruto do time (ex: 102.5)"""
        if not team_abbr: return LEAGUE_AVERAGE_PACE
        return self.pace_data.get(team_abbr.upper(), LEAGUE_AVERAGE_PACE)

    def get_team_pace_factor(self, team_abbr):
        """
        Retorna o fator multiplicador de um único time vs a média da liga.
        Usado pelo Desdobrador Inteligente.
        Ex: Se time tem pace 105 e liga 100 -> retorna 1.05
        """
        pace = self.get_team_pace(team_abbr)
        return pace / LEAGUE_AVERAGE_PACE

    def calculate_game_pace(self, home_team, away_team):
        """Estima o Pace do jogo (Média dos dois times)"""
        pace_h = self.get_team_pace(home_team)
        pace_a = self.get_team_pace(away_team)
        return (pace_h + pace_a) / 2.0
    
    def get_pace_factor(self, home_team, away_team):
        """
        Retorna o fator de ajuste do JOGO.
        Ex: Jogo Pace 105 vs Média 100 -> Fator 1.05
        """
        game_pace = self.calculate_game_pace(home_team, away_team)
        return game_pace / LEAGUE_AVERAGE_PACE

    def adjust_player_stats(self, player_stats, home_team, away_team):
        """
        Ajusta stats de volume (PTS, REB, AST) baseado no Pace do jogo.
        """
        factor = self.get_pace_factor(home_team, away_team)
        
        # Travas de segurança para não distorcer demais (0.85x a 1.15x)
        factor = max(0.85, min(1.15, factor))
        
        volume_stats = ['pts_L5', 'reb_L5', 'ast_L5', 'pra_L5', 'PTS_AVG', 'REB_AVG', 'AST_AVG']
        adjusted = player_stats.copy()
        
        for stat in volume_stats:
            if stat in adjusted and isinstance(adjusted[stat], (int, float)):
                if adjusted[stat] > 0:
                    adjusted[stat] = adjusted[stat] * factor
        
        adjusted['pace_factor'] = factor
        return adjusted
