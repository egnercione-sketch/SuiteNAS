import pandas as pd
import numpy as np

class TrendAnalyzer:
    """
    Gera visualização de tendência (Sparklines) para os últimos 10 jogos.
    """
    def __init__(self):
        pass

    def get_trend_visuals(self, player_id, market_type, line, game_logs_df=None):
        """
        Retorna uma string visual (ex: 🟩🟩🟥🟩🟥) e a Taxa de Acerto (Hit Rate).
        Se não houver logs, retorna neutro.
        """
        if game_logs_df is None or game_logs_df.empty:
            return "⬜⬜⬜⬜⬜ (Sem dados)", 0.0

        # Filtra os últimos 10 jogos do jogador
        # Assume que o DataFrame já vem filtrado pelo ID do jogador ou que passamos os logs diretos
        last_10 = game_logs_df.head(10).copy()
        
        if last_10.empty:
            return "⬜⬜⬜⬜⬜ (N/A)", 0.0

        hits = 0
        visuals = []
        
        # Mapeamento de colunas ESPN/NBA API
        col_map = {"PTS": "PTS", "REB": "REB", "AST": "AST", "3PM": "FG3M", "PRA": "PRA"}
        target_col = col_map.get(market_type, "PTS")

        # Se for PRA e não tiver coluna pronta, calcula
        if market_type == "PRA" and "PRA" not in last_10.columns:
            last_10['PRA'] = last_10['PTS'] + last_10['REB'] + last_10['AST']

        for _, game in last_10.iterrows():
            val = game.get(target_col, 0)
            if val >= line:
                visuals.append("🟩") # Green
                hits += 1
            else:
                visuals.append("🟥") # Red
        
        # Inverte para mostrar cronologia (Antigo -> Novo ou Novo -> Antigo? Normalmente mostramos recentes primeiro)
        # Vamos manter recentes na esquerda: Jogo 1 (Ontem), Jogo 2, etc.
        visual_str = "".join(visuals)
        hit_rate = (hits / len(last_10)) * 100
        
        return visual_str, hit_rate