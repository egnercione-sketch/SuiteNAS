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
# Define as temporadas de análise (Atual + Passada para maior amostra H2H)
CURRENT_SEASON = "2025-26"
PREV_SEASON = "2024-25"

CACHE_DIR = os.path.join(os.getcwd(), "cache")
NARRATIVE_CACHE_FILE = os.path.join(CACHE_DIR, "narrative_cache_v2.json")

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
# ENGINE DE NARRATIVAS (CORRIGIDA)
# ==============================================================================
class NarrativeIntelligence:
    def __init__(self):
        self.cache = load_json(NARRATIVE_CACHE_FILE)
        self.api_delay = 0.6 # Delay vital para não tomar block

    def get_player_matchup_history(self, player_id, player_name, opponent_abbr):
        """
        Busca o histórico do jogador contra um time específico (H2H).
        Olha para a temporada atual e anterior para criar volume de dados.
        """
        # 1. Chave de Cache (Ex: 203999_BOS_v2)
        cache_key = f"{player_id}_{opponent_abbr}_v2"
        
        # 2. Verificar Cache (Validade de 7 dias)
        if cache_key in self.cache:
            cached_data = self.cache[cache_key]
            last_update = cached_data.get('updated_at', '2000-01-01')
            days_diff = (datetime.now() - datetime.strptime(last_update, "%Y-%m-%d")).days
            
            # Se for recente e tiver dados válidos, retorna
            if days_diff < 7 and cached_data.get('data') is not None:
                return cached_data['data']

        # 3. Buscar na API
        if not NBA_API_AVAILABLE:
            return None

        try:
            time.sleep(self.api_delay)
            
            # --- CORREÇÃO: Busca DUAS temporadas para ter amostra ---
            # Temporada Atual
            df_curr = playergamelog.PlayerGameLog(player_id=player_id, season=CURRENT_SEASON).get_data_frames()[0]
            # Temporada Passada (Opcional, mas recomendado para H2H)
            time.sleep(0.3) 
            df_prev = playergamelog.PlayerGameLog(player_id=player_id, season=PREV_SEASON).get_data_frames()[0]
            
            # Junta tudo
            df_full = pd.concat([df_curr, df_prev], ignore_index=True)
            
            if df_full.empty:
                return None

            # 4. Filtrar jogos contra o oponente
            # Normaliza para garantir que ache 'GSW' em 'GS' ou 'NOP' em 'NO'
            opp_games = df_full[df_full['MATCHUP'].str.contains(opponent_abbr, case=False, na=False)]
            
            # Se tiver menos de 2 jogos em 2 anos, não dá pra tirar conclusão
            if len(opp_games) < 2:
                result_data = None # Ignora dados insuficientes
            else:
                # 5. Calcular Médias H2H
                avg_pts = opp_games['PTS'].mean()
                avg_reb = opp_games['REB'].mean()
                avg_ast = opp_games['AST'].mean()
                
                # 6. Comparar com média da temporada ATUAL apenas (para saber se ele sobe de produção)
                if not df_curr.empty:
                    season_pts = df_curr['PTS'].mean()
                else:
                    season_pts = df_full['PTS'].mean() # Fallback
                
                narrative = "Neutro"
                badge = "H2H"
                
                # Regra do Killer: +15% sobre a média da temporada
                if season_pts > 0:
                    diff_pct = ((avg_pts - season_pts) / season_pts) * 100
                else:
                    diff_pct = 0
                
                if diff_pct >= 15:
                    narrative = "Killer"
                    badge = "🔥 CARRASCO"
                elif diff_pct <= -15:
                    narrative = "Cold"
                    badge = "❄️ TRAUMA"
                    
                result_data = {
                    "games_played": len(opp_games),
                    "avg_stats": {
                        "PTS": round(avg_pts, 1),
                        "REB": round(avg_reb, 1),
                        "AST": round(avg_ast, 1)
                    },
                    "comparison": {
                        "season_pts": round(season_pts, 1),
                        "diff_pct": round(diff_pct, 1)
                    },
                    "narrative": narrative,
                    "badge": badge,
                    # Pega apenas dados serializáveis (strings/floats) para o JSON
                    "last_games_count": len(opp_games)
                }
            
            # Salvar no Cache (Mesmo que seja None, para não tentar de novo hoje)
            self.cache[cache_key] = {
                "updated_at": datetime.now().strftime("%Y-%m-%d"),
                "data": result_data
            }
            save_json(NARRATIVE_CACHE_FILE, self.cache)
            
            return result_data

        except Exception as e:
            # Em caso de erro de API (timeout), não salva no cache pra tentar de novo depois
            print(f"Erro Intelligence: {e}")
            return None
