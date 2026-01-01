"""
Pace Adjuster Module (Dynamic Version)
Ajusta estatísticas baseadas no ritmo do jogo (Pace) em tempo real (2025-26).
"""

import json
import os

# --- PACE MÉDIO DA LIGA (Referência para fator de ajuste) ---
LEAGUE_AVERAGE_PACE = 99.5 

# --- FALLBACK DE SEGURANÇA ---
DEFAULT_PACE_DATA = {
    "WAS": 103.0, "IND": 102.5, "ATL": 101.5, "SAS": 101.0, "GSW": 100.5,
    "DET": 100.0, "MIL": 100.0, "LAL": 100.5, "OKC": 100.5, "UTA": 99.5,
    "DAL": 99.0,  "TOR": 99.5,  "BOS": 98.5,  "MEM": 99.0,  "SAC": 100.0,
    "PHI": 98.0,  "PHX": 99.0,  "HOU": 100.0, "CHA": 98.5,  "BKN": 98.0,
    "CLE": 97.5,  "LAC": 97.5,  "NOP": 98.0,  "MIA": 96.5,  "DEN": 97.0,
    "ORL": 97.5,  "CHI": 97.0,  "MIN": 96.5,  "NYK": 96.0,  "POR": 97.5
}

class PaceAdjuster:
    def __init__(self, data_source=None):
        """
        Args:
            data_source: Pode ser um dicionário (do session_state) ou caminho de arquivo.
        """
        self.pace_data = {}
        
        # 1. Tenta usar dados passados diretamente (Prioridade)
        if isinstance(data_source, dict) and data_source:
            self.pace_data = self._parse_data(data_source)
            
        # 2. Se for string, tenta ler como arquivo
        elif isinstance(data_source, str) and os.path.exists(data_source):
            try:
                with open(data_source, 'r', encoding='utf-8') as f:
                    self.pace_data = self._parse_data(json.load(f))
            except: pass
        
        # 3. Fallback: Tenta achar arquivo no cache padrão
        if not self.pace_data:
            try:
                # Sobe 3 níveis até achar a pasta raiz/cache (ajuste conforme sua estrutura)
                base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                default_path = os.path.join(base_dir, "cache", "team_advanced.json")
                if os.path.exists(default_path):
                    with open(default_path, 'r', encoding='utf-8') as f:
                        self.pace_data = self._parse_data(json.load(f))
            except: pass

        # 4. Último caso: usa estático
        if not self.pace_data:
            self.pace_data = DEFAULT_PACE_DATA

    def _parse_data(self, data):
        """Extrai apenas o PACE do JSON complexo."""
        pace_map = {}
        if isinstance(data, list):
            for item in data:
                team = item.get('TEAM_ABBREVIATION') or item.get('TEAM')
                pace = item.get('PACE')
                if team and pace: pace_map[team] = float(pace)
        elif isinstance(data, dict):
            for team, stats in data.items():
                if isinstance(stats, dict):
                    pace = stats.get('PACE') or stats.get('pace')
                    if pace: pace_map[team] = float(pace)
                elif isinstance(stats, (float, int)):
                    pace_map[team] = float(stats)
        return pace_map

    def get_team_pace(self, team_abbr):
        return self.pace_data.get(team_abbr, 100.0)

    def calculate_game_pace(self, home_team, away_team):
        pace_h = self.get_team_pace(home_team)
        pace_a = self.get_team_pace(away_team)
        return (pace_h + pace_a) / 2.0
    
    def get_pace_factor(self, home_team, away_team):
        game_pace = self.calculate_game_pace(home_team, away_team)
        return game_pace / LEAGUE_AVERAGE_PACE

    def adjust_player_stats(self, player_stats, home_team, away_team):
        factor = self.get_pace_factor(home_team, away_team)
        factor = max(0.85, min(1.15, factor)) # Limitadores de segurança
        
        volume_stats = ['pts_L5', 'reb_L5', 'ast_L5', 'pra_L5', 'PTS_AVG', 'REB_AVG', 'AST_AVG']
        adjusted = player_stats.copy()
        
        for stat in volume_stats:
            if stat in adjusted and isinstance(adjusted[stat], (int, float)):
                if adjusted[stat] > 0:
                    adjusted[stat] = adjusted[stat] * factor
        
        adjusted['pace_factor'] = factor
        return adjusted