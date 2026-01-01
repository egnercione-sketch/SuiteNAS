# modules/new_modules/insights_engine.py
from typing import List, Dict
import pandas as pd

class DailyInsightsEngine:
    def __init__(self, rotation_analyzer, dvp_analyzer):
        self.rotation_analyzer = rotation_analyzer
        self.dvp_analyzer = dvp_analyzer

    def generate_daily_insights(self, games: List[Dict], players_l5: pd.DataFrame, injuries: Dict) -> List[Dict]:
        insights = []
        
        # Lista dos times jogando hoje (para filtrar stats)
        teams_playing_today = set()
        for g in games:
            # Tenta chaves comuns de APIs (home/away ou *_team)
            teams_playing_today.add(g.get('home') or g.get('home_team') or g.get('HOME_TEAM_ABBREVIATION'))
            teams_playing_today.add(g.get('away') or g.get('away_team') or g.get('VISITOR_TEAM_ABBREVIATION'))
        
        # Remove valores nulos caso existam
        teams_playing_today = {t for t in teams_playing_today if t}

        # 1. VARREDURA DE LESÕES (VACANCY)
        # Usa o RotationAnalyzer que já temos
        if self.rotation_analyzer and hasattr(self.rotation_analyzer, 'rotation_signals'):
            for team, signal in self.rotation_analyzer.rotation_signals.items():
                # Só processa se o time estiver jogando hoje
                clean_team_name = team.replace('_rotation', '')
                if clean_team_name not in teams_playing_today and team not in teams_playing_today:
                    continue

                injured_starters = signal.get("signals", {}).get("injured_starters", [])
                if injured_starters:
                    for starter in injured_starters:
                        # Tentar achar o substituto direto nos insights de rotação
                        impact = signal.get("signals", {}).get("impact_analysis", "Rotação ajustada.")
                        insights.append({
                            "type": "VACANCY",
                            "priority": 1, # Altíssima prioridade
                            "icon": "🚨",
                            "color": "#FF4F4F", # Vermelho
                            "title": f"DESFALQUE EM {clean_team_name}",
                            "desc": f"**{starter}** está fora. {impact}",
                            "tag": "OPORTUNIDADE"
                        })

        # 2. VARREDURA DE DVP (TARGETS)
        # Cruzar quem joga hoje contra defesas Rank > 25 (Piores)
        if self.dvp_analyzer:
            for game in games:
                home = game.get('home') or game.get('home_team')
                away = game.get('away') or game.get('away_team')
                
                # Checar Home Players vs Away Defense
                # (Lógica simplificada para exemplo, o ideal é iterar players do time)
                # Aqui vamos focar em Estrelas vs Defesa Ruim
                
                # Exemplo: Se Away Defense vs PG é ruim, alertar sobre o PG do Home
                rank_pg_away = self.dvp_analyzer.get_position_rank(away, "PG")
                if rank_pg_away >= 28: # Defesa Top 3 Pior
                     insights.append({
                        "type": "MATCHUP",
                        "priority": 2,
                        "icon": "🎯",
                        "color": "#00FF9C", # Verde
                        "title": f"ALVO: PG do {home}",
                        "desc": f"Enfrenta {away} (Defesa #{rank_pg_away} vs PG).",
                        "tag": "GREEN LIGHT"
                    })

        # 3. VARREDURA DE STREAKS (HOT HAND) - ATUALIZADO
        # Jogadores muito acima da média recente APENAS DOS JOGOS DE HOJE
        if not players_l5.empty:
            
            # --- FILTRAGEM DE TIMES DO DIA ---
            # Identifica a coluna de time (pode variar entre 'team', 'TEAM_ABBREVIATION', etc)
            col_team = 'TEAM_ABBREVIATION' if 'TEAM_ABBREVIATION' in players_l5.columns else 'team'
            
            if col_team in players_l5.columns:
                # Filtra apenas jogadores cujos times estão no set teams_playing_today
                df_today = players_l5[players_l5[col_team].isin(teams_playing_today)]
            else:
                # Fallback: Se não achar a coluna, usa o DF original (mas loga erro se possível)
                df_today = players_l5
            
            # Aplica a lógica de Hot Hand no DataFrame Filtrado
            hot_players = df_today[
                (df_today["PTS_AVG"] >= 25) & 
                (df_today["MIN_AVG"] >= 30)
            ].sort_values(by="PTS_AVG", ascending=False).head(3)
            
            for _, p in hot_players.iterrows():
                insights.append({
                    "type": "STREAK",
                    "priority": 3,
                    "icon": "🔥",
                    "color": "#FFA500", # Laranja
                    "title": f"EM CHAMAS: {p.get('PLAYER', p.get('name', 'Jogador'))}",
                    "desc": f"Vem destruindo com média de {p['PTS_AVG']:.1f} PTS nos últimos 5 jogos.",
                    "tag": "HOT HAND"
                })

        return sorted(insights, key=lambda x: x['priority'])