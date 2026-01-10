# modules/new_modules/thesis_engine.py
# VERSÃO V80.0 - CONTEXT NARRATIVE

import logging
from typing import Dict, List, Optional

logger = logging.getLogger("ThesisEngine_V80")

class ThesisEngine:
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.WIN_RATES = {
            'VacuumOpportunity': 0.85, 
            'DVPExploiter': 0.78,
            'HighCeiling': 0.75,
            'MinutesSafe': 0.68,
            'ScorerLine': 0.55
        }

    def generate_theses(self, p_ctx: Dict, ctx_data: Dict) -> List[Dict]:
        """Gera teses baseadas em stats PROJETADOS (já com boost)"""
        theses = []
        
        # Dados (Já boostados pelo StrategyEngine)
        pts = p_ctx.get('pts', p_ctx.get('pts_L5', 0))
        mins = p_ctx.get('min', p_ctx.get('min_L5', 0))
        
        # 1. VACUUM (Oportunidade)
        if p_ctx.get('is_vacuum'):
            theses.append({
                'type': 'VacuumOpportunity',
                'reason': f"Oportunidade por Lesão (Vol+)",
                'win_rate': self.WIN_RATES['VacuumOpportunity'],
                'confidence': 0.90
            })

        # 2. MATCHUP (DvP)
        rank = p_ctx.get('matchup_rank', 15)
        if rank >= 25:
            theses.append({
                'type': 'DVPExploiter',
                'reason': f"Matchup Top (Defesa #{rank})",
                'win_rate': self.WIN_RATES['DVPExploiter'],
                'confidence': 0.85
            })

        # 3. VOLUME (Base)
        if pts >= 15 and mins >= 28:
            theses.append({
                'type': 'HighCeiling',
                'reason': f"Volume Alto ({pts:.1f} proj)",
                'win_rate': self.WIN_RATES['HighCeiling'],
                'confidence': 0.80
            })
            
        # Fallback
        if not theses:
            theses.append({
                'type': 'ScorerLine',
                'reason': f"Linha Projetada {pts:.1f}",
                'win_rate': 0.55, 'confidence': 0.55
            })

        theses.sort(key=lambda x: x['win_rate'], reverse=True)
        return theses

    def get_thesis_for_category(self, list, cat): return list[0] if list else None
    def format_thesis_for_display(self, t): return t['reason']
    def enhance_thesis(self, p, m, t): return t
