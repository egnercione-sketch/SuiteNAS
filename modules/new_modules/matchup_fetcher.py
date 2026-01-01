import os
import pickle
import time
import pandas as pd
from datetime import datetime

# Configuração de Caminhos (Ajustado para a estrutura do teu projeto)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CACHE_DIR = os.path.join(BASE_DIR, "cache")
os.makedirs(CACHE_DIR, exist_ok=True)

class MatchupHistoryFetcher:
    def __init__(self):
        # Mapeamento para garantir compatibilidade entre siglas ESPN e NBA Stats
        self.espn_to_nba = {
            "SA": "SAS", "NY": "NYK", "NO": "NOP", "UTAH": "UTA", 
            "GS": "GSW", "WSH": "WAS", "PHO": "PHX", "BRK": "BKN"
        }
        self.cache_file = os.path.join(CACHE_DIR, "h2h_cache.pkl")
        self.cache = self._load_cache()

    def _load_cache(self):
        """Carrega o cache do dia. Se for um novo dia, limpa os dados antigos."""
        today = datetime.now().strftime('%Y-%m-%d')
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'rb') as f:
                    data = pickle.load(f)
                    if data.get('date') == today:
                        return data
            except Exception as e:
                print(f"Erro ao carregar cache H2H: {e}")
        
        return {'date': today, 'data': {}}

    def _save_cache(self):
        """Guarda os dados minerados no disco para evitar chamadas repetidas à API."""
        try:
            with open(self.cache_file, 'wb') as f:
                pickle.dump(self.cache, f)
        except Exception as e:
            print(f"Erro ao guardar cache H2H: {e}")

    def get_h2h_stats(self, player_id, opponent_abbr):
        """
        Busca estatísticas históricas de um jogador contra uma equipa específica.
        Retorna status HOT, NEUTRAL ou COLD baseado no diferencial de performance.
        """
        # 1. Verificar Cache Primeiro (Instantâneo)
        cache_key = f"{player_id}_{opponent_abbr}"
        if cache_key in self.cache['data']:
            return self.cache['data'][cache_key]

        try:
            from nba_api.stats.endpoints import playergamelog
            
            # 2. Rate Limiting: Pequeno delay para não ser bloqueado pela NBA API
            time.sleep(0.4) 
            
            # 3. Chamada à API (Lenta)
            # Busca todos os jogos do jogador na época atual
            log = playergamelog.PlayerGameLog(player_id=player_id).get_data_frames()[0]
            
            # Normalizar sigla do oponente
            nba_opp = self.espn_to_nba.get(opponent_abbr, opponent_abbr)
            
            # Filtrar confrontos diretos (Head-to-Head)
            h2h_mask = log['MATCHUP'].str.contains(nba_opp)
            h2h_games = log[h2h_mask]
            
            if h2h_games.empty:
                return None

            # 4. Cálculo de Médias
            stats = {
                "PTS": round(h2h_games['PTS'].mean(), 1),
                "REB": round(h2h_games['REB'].mean(), 1),
                "AST": round(h2h_games['AST'].mean(), 1),
                "3PM": round(h2h_games['FG3M'].mean(), 1),
                "PRA": round((h2h_games['PTS'] + h2h_games['REB'] + h2h_games['AST']).mean(), 1)
            }
            
            # Baseline: Média da Época para comparação
            season_pra = (log['PTS'] + log['REB'] + log['AST']).mean()
            diff_ratio = stats["PRA"] / season_pra if season_pra > 0 else 1.0
            diff_pct = round((diff_ratio - 1) * 100, 1)

            # 5. Classificação de Matchup
            status = "NEUTRAL"
            color = "#94A3B8" # Cinza
            if diff_ratio >= 1.15: 
                status = "HOT"
                color = "#00FF9C" # Verde Neon
            elif diff_ratio <= 0.85: 
                status = "COLD"
                color = "#FF4F4F" # Vermelho Neon

            result = {
                "status": status,
                "color": color,
                "stats": stats,
                "diff_pct": diff_pct,
                "games_count": len(h2h_games)
            }
            
            # 6. Atualizar Cache
            self.cache['data'][cache_key] = result
            self._save_cache()
            
            return result
            
        except Exception as e:
            print(f"Erro ao processar H2H para ID {player_id}: {e}")
            return None