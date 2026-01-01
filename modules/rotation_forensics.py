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
                progress_bar.progress((i / total_steps), text=f"🔍 Analisando DNA: {team_abbr}...")
            try:
                dna = self._analyze_team_blowouts(team_abbr)
                if dna: full_dna[team_abbr] = dna
            except: continue
        
        if progress_bar: progress_bar.progress(1.0, text="✅ DNA Gerado!")
        return full_dna

    def _analyze_team_blowouts(self, team_abbr, lookback=40):
        tid = self.team_map.get(team_abbr)
        if not tid: return []
        try:
            logs = teamgamelogs.TeamGameLogs(team_id_nullable=tid, season_nullable='2024-25', last_n_games_nullable=lookback).get_data_frames()[0]
            blowouts = logs[logs['PLUS_MINUS'].abs() >= 15]
        except: return []

        if blowouts.empty: return []
        game_ids = blowouts['GAME_ID'].unique().tolist()
        player_stats = {}

        for gid in game_ids:
            try:
                time.sleep(0.4) 
                box = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=gid).get_data_frames()[0]
                team_box = box[box['TEAM_ID'] == tid]
                for _, row in team_box.iterrows():
                    name = row['PLAYER_NAME']
                    try:
                        m_str = str(row['MIN'])
                        if ":" in m_str: m, s = m_str.split(":")
                        else: m, s = m_str, 0
                        minutes = float(m) + float(s)/60
                    except: minutes = 0.0
                    
                    # REGRA DO BLOWOUT: Ignora titulares (>32m) e irrelevantes (<8m)
                    if 8.0 < minutes < 32.0:
                        if name not in player_stats: player_stats[name] = {'g': 0, 'min': 0, 'pts': 0}
                        player_stats[name]['g'] += 1
                        player_stats[name]['min'] += minutes
                        player_stats[name]['pts'] += row['PTS']
            except: pass

        report = []
        for name, data in player_stats.items():
            freq = data['g'] / len(game_ids)
            if freq >= 0.30: # 30% de presença mínima
                report.append({
                    'name': name,
                    'frequency': f"{int(freq*100)}%",
                    'avg_min_blowout': round(data['min'] / data['g'], 1),
                    'avg_pts_blowout': round(data['pts'] / data['g'], 1)
                })
        return report