import time
import json
import os
import pandas as pd
from datetime import datetime

# Tenta importar a API da NBA. Se falhar, usa modo offline.
try:
    from nba_api.stats.endpoints import playergamelog
    NBA_API_AVAILABLE = True
except ImportError:
    NBA_API_AVAILABLE = False

# ==============================================================================
# CONFIGURAÇÕES
# ==============================================================================
CACHE_DIR = os.path.join(os.getcwd(), "cache")
NARRATIVE_CACHE_FILE = os.path.join(CACHE_DIR, "narrative_cache.json")

def ensure_dir(file_path):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory): os.makedirs(directory)

def load_json(path):
    if os.path.exists(path):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except: return {}
    return {}

def save_json(path, data):
    ensure_dir(path)
    try:
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except: pass

# ==============================================================================
# ENGINE DE NARRATIVAS
# ==============================================================================
class NarrativeIntelligence:
    def __init__(self):
        self.cache = load_json(NARRATIVE_CACHE_FILE)
        self.api_delay = 0.6 # Delay para não tomar block da NBA API

    def get_player_matchup_history(self, player_id, player_name, opponent_abbr):
        """
        Busca o histórico do jogador contra um time específico (H2H).
        Retorna: Estatísticas médias e 'Badges' de narrativa.
        """
        # 1. Chave de Cache Única (Ex: 203999_BOS)
        cache_key = f"{player_id}_{opponent_abbr}"
        
        # 2. Verificar Cache (Validade de 7 dias para histórico)
        if cache_key in self.cache:
            cached_data = self.cache[cache_key]
            last_update = cached_data.get('updated_at', '2000-01-01')
            # Se for recente, retorna cache
            if (datetime.now() - datetime.strptime(last_update, "%Y-%m-%d")).days < 7:
                return cached_data['data']

        # 3. Buscar na API (Se disponível)
        if not NBA_API_AVAILABLE:
            return self._get_mock_narrative(player_name, opponent_abbr)

        try:
            # Busca logs desta temporada e da passada para ter amostra
            # (Simplificação: pegando apenas season atual por performance inicial)
            time.sleep(self.api_delay)
            gamelog = playergamelog.PlayerGameLog(player_id=player_id, season='2024-25').get_data_frames()[0]
            
            if gamelog.empty:
                return None

            # 4. Filtrar jogos contra o oponente
            # O campo MATCHUP geralmente é "LAL @ BOS" ou "LAL vs. BOS"
            # Vamos procurar a sigla do oponente na string
            
            # Normalização simples de string
            opp_games = gamelog[gamelog['MATCHUP'].str.contains(opponent_abbr, case=False, na=False)]
            
            if opp_games.empty:
                # Se não jogou esse ano, tenta verificar se o usuário quer buscar ano passado (future feature)
                return {
                    "games_played": 0,
                    "narrative": "Sem histórico recente",
                    "avg_stats": {}
                }

            # 5. Calcular Médias H2H
            avg_pts = opp_games['PTS'].mean()
            avg_reb = opp_games['REB'].mean()
            avg_ast = opp_games['AST'].mean()
            
            # 6. Analisar Narrativa (Killer vs Victim)
            # Precisamos comparar com a média geral da temporada
            season_pts = gamelog['PTS'].mean()
            
            narrative = "Neutro"
            badge = ""
            
            if avg_pts > season_pts * 1.2: # 20% acima da média
                narrative = "Killer"
                badge = "🔥 H2H KILLER"
            elif avg_pts < season_pts * 0.8: # 20% abaixo da média
                narrative = "Struggles"
                badge = "❄️ FRIA H2H"
                
            result_data = {
                "games_played": len(opp_games),
                "avg_stats": {
                    "PTS": round(avg_pts, 1),
                    "REB": round(avg_reb, 1),
                    "AST": round(avg_ast, 1)
                },
                "comparison": {
                    "season_pts": round(season_pts, 1),
                    "diff_pct": round(((avg_pts - season_pts) / season_pts) * 100, 1) if season_pts > 0 else 0
                },
                "narrative": narrative,
                "badge": badge,
                "last_games": opp_games[['GAME_DATE', 'MATCHUP', 'PTS', 'REB', 'AST']].head(3).to_dict('records')
            }
            
            # Salvar no Cache
            self.cache[cache_key] = {
                "updated_at": datetime.now().strftime("%Y-%m-%d"),
                "data": result_data
            }
            save_json(NARRATIVE_CACHE_FILE, self.cache)
            
            return result_data

        except Exception as e:
            print(f"Erro no NarrativeIntelligence: {e}")
            return None

    def _get_mock_narrative(self, player_name, opponent_abbr):
        """Fallback para quando a API falhar ou estiver offline"""
        return {
            "games_played": 2,
            "avg_stats": {"PTS": 25.0, "REB": 5.0, "AST": 5.0},
            "narrative": "Simulado",
            "badge": "⚠️ OFF-LINE",
            "last_games": []
        }