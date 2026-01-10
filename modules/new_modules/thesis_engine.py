# modules/new_modules/thesis_engine.py
# VERSÃO V80.0 - CONTEXT AWARE (VACUUM & MATCHUP)

import logging
from typing import Dict, List, Optional

logger = logging.getLogger("ThesisEngine_V80")

class ThesisEngine:
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        
        # Taxas de acerto estimadas para cada tipo de tese
        self.WIN_RATES = {
            'VacuumOpportunity': 0.82,  # Altíssima probabilidade (substituto direto)
            'DVPExploiter': 0.78,       # Matchup muito favorável
            'HighCeiling': 0.75,
            'MinutesSafe': 0.67,
            'PlaymakerEdge': 0.60,
            'ScorerLine': 0.55
        }

    def generate_theses(self, player_ctx: Dict, context_data: Dict) -> List[Dict]:
        """
        Gera lista de teses para o jogador.
        player_ctx: Deve conter stats já projetados/boostados.
        """
        theses = []
        
        pts = player_ctx.get('pts_L5', 0)
        ast = player_ctx.get('ast_L5', 0)
        reb = player_ctx.get('reb_L5', 0)
        mins = player_ctx.get('min_L5', 0)
        
        # 1. TESE VACUUM (Topo da Pirâmide)
        if player_ctx.get('is_vacuum', False):
            theses.append({
                'type': 'VacuumOpportunity',
                'market': 'PTS' if pts > 12 else 'REB',
                'reason': f"Oportunidade por Lesão (Projeção Elevada)",
                'win_rate': self.WIN_RATES['VacuumOpportunity'],
                'confidence': 0.95,
                'category': 'ousada'
            })

        # 2. TESE MATCHUP (DvP)
        rank = player_ctx.get('matchup_rank', 15)
        if rank >= 25: # Defesa Rank 25-30 (Péssima)
            market = 'PTS' # Default, poderia refinar por pos
            if ast > 5: market = 'AST'
            elif reb > 8: market = 'REB'
            
            theses.append({
                'type': 'DVPExploiter',
                'market': market,
                'reason': f"Explorador de Matchup (Defesa #{rank})",
                'win_rate': self.WIN_RATES['DVPExploiter'],
                'confidence': 0.90,
                'category': 'balanceada'
            })

        # 3. TESES ESTATÍSTICAS (Base)
        if pts >= 15 and mins >= 28:
            theses.append({
                'type': 'HighCeiling',
                'market': 'PTS',
                'reason': f"Volume de Pontuação ({pts:.1f} proj)",
                'win_rate': self.WIN_RATES['HighCeiling'],
                'confidence': 0.80,
                'category': 'conservadora'
            })
            
        if mins >= 30:
            theses.append({
                'type': 'MinutesSafe',
                'market': 'PTS', # Genérico
                'reason': f"Minutagem Segura ({mins:.0f}m)",
                'win_rate': self.WIN_RATES['MinutesSafe'],
                'confidence': 0.75,
                'category': 'conservadora'
            })

        # Ordena por Win Rate
        theses.sort(key=lambda x: x['win_rate'], reverse=True)
        return theses

    def get_thesis_for_category(self, theses_list, target_cat):
        # Retorna a melhor tese disponível
        return theses_list[0] if theses_list else None

    def format_thesis_for_display(self, thesis):
        if not thesis: return "Análise Padrão"
        return f"{thesis['reason']} ({int(thesis['win_rate']*100)}% WR)"
    
    def enhance_thesis(self, p, mkt, original):
        # Método de compatibilidade
        return original
