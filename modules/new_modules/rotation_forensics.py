import time
import pandas as pd
from nba_api.stats.endpoints import teamgamelogs, boxscoretraditionalv2
from nba_api.stats.static import teams

class RotationForensics:
    def __init__(self):
        self.teams = teams.get_teams()
        self.team_map = {t['abbreviation']: t['id'] for t in self.teams}

    def generate_dna_report(self, progress_bar=None):
        full_dna = {}
        all_teams_abbr = list(self.team_map.keys())
        total_steps = len(all_teams_abbr)
        
        for i, team_abbr in enumerate(all_teams_abbr):
            if progress_bar:
                progress_bar.progress((i / total_steps), text=f"🔍 Extraindo DNA: {team_abbr}...")
            try:
                dna = self._analyze_team_blowouts(team_abbr)
                if dna: 
                    full_dna[team_abbr] = dna
            except Exception as e:
                print(f"Erro crítico ao processar {team_abbr}: {e}")
                continue
        
        if progress_bar: 
            progress_bar.progress(1.0, text="✅ DNA Gerado com Sucesso!")
        return full_dna

    def _analyze_team_blowouts(self, team_abbr, lookback=45):
        tid = self.team_map.get(team_abbr)
        if not tid: 
            return []
        
        # --- CORREÇÃO 1: TEMPORADA ATUALIZADA PARA 2025-26 ---
        # Como estamos em Dezembro de 2025, os dados de 24-25 são obsoletos para rotação
        current_season = '2025-26'
        
        try:
            # 1. Busca jogos com diferença de placar >= 14 (Blowout claro)
            logs_call = teamgamelogs.TeamGameLogs(
                team_id_nullable=tid, 
                season_nullable=current_season, 
                last_n_games_numeric=lookback
            )
            logs = logs_call.get_data_frames()[0]
            
            # Filtra vitórias ou derrotas por 14+ pontos
            blowout_games = logs[logs['PLUS_MINUS'].abs() >= 14]
            game_ids = blowout_games['GAME_ID'].tolist()
            
            if not game_ids: 
                return []

            player_stats = {}
            # Analisa os últimos 10 blowouts para capturar a rotação mais recente
            for gid in game_ids[:10]:
                # --- CORREÇÃO 2: AUMENTO DO SLEEP PARA EVITAR RATE LIMIT (403 Forbidden) ---
                time.sleep(0.7) 
                
                try:
                    box = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=gid).get_data_frames()[0]
                    team_box = box[box['TEAM_ID'] == tid]
                    
                    for _, row in team_box.iterrows():
                        name = row['PLAYER_NAME']
                        try:
                            # Parse robusto de minutos (formato "MM:SS")
                            m_str = str(row['MIN'])
                            if ":" in m_str:
                                m, s = m_str.split(":")
                                minutes = float(m) + float(s)/60
                            else:
                                minutes = float(m_str)
                        except:
                            minutes = 0.0
                        
                        # --- CORREÇÃO 3: ALINHAMENTO DO FILTRO COM A UI (ATÉ 29m) ---
                        # Antes estava 28.0, agora cobre até 29.0 para evitar buracos na análise
                        if 3.0 <= minutes <= 29.0:
                            if name not in player_stats: 
                                player_stats[name] = {'g': 0, 'min': 0, 'pts': 0}
                            
                            player_stats[name]['g'] += 1
                            player_stats[name]['min'] += minutes
                            player_stats[name]['pts'] += row['PTS']
                except Exception:
                    continue
            
            report = []
            for name, data in player_stats.items():
                # Frequência de participação: O jogador precisa aparecer em pelo menos 20% dos casos
                freq = data['g'] / len(game_ids)
                if freq >= 0.20: 
                    report.append({
                        'name': name,
                        'frequency': f"{int(freq*100)}%",
                        'avg_min_blowout': round(data['min'] / data['g'], 1),
                        'avg_pts_blowout': round(data['pts'] / data['g'], 1)
                    })
            return report
        except Exception as e:
            print(f"Erro ao analisar {team_abbr}: {e}")
            return []