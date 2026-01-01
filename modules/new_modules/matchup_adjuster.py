"""
Matchup Adjuster v2.0
Responsável por ajustar as projeções baseadas na dificuldade defensiva do adversário (DvP).
Foca em identificar "Leaks" (Defesas Fracas) e "Walls" (Defesas de Elite).
"""

class MatchupAdjuster:
    def __init__(self):
        # =========================================================================
        # MAPA DE DEFESAS - TEMPORADA 2024-25
        # =========================================================================
        
        # 🟢 LEAKS: Times que CEDEM muitos pontos/stats para estas posições
        self.leaks = {
            "PG": ["WAS", "DET", "ATL", "IND", "TOR", "UTA"], # Defesas de perímetro fracas
            "SG": ["WAS", "CHA", "ATL", "POR", "CHI"],
            "SF": ["IND", "DET", "WAS", "CHA", "SAS"],
            "PF": ["WAS", "MEM", "IND", "UTA", "OKC"],        # OKC cede rebotes para PF/C
            "C":  ["WAS", "MEM", "CHA", "NOP", "CHI", "OKC"]  # Garrafões vulneráveis
        }
        
        # 🔴 WALLS: Times que BLOQUEIAM estatísticas (Defesas Elite)
        self.walls = {
            "PG": ["ORL", "MIN", "OKC", "HOU", "BOS"], # Perímetros de elite
            "SG": ["ORL", "MIN", "CLE", "BOS"],
            "SF": ["MIN", "ORL", "BOS", "NYK"],
            "PF": ["MIN", "ORL", "CLE", "MIA"],
            "C":  ["MIN", "ORL", "PHI", "CLE", "MEM"]  # Atenção: MEM com Edey/JJJ pode variar, mas bloqueia bem
        }

        # 🔵 REBOTE: Times específicos para targeting de Rebotes (Times baixos ou ruins no vidro)
        self.rebound_targets = ["WAS", "OKC", "CHA", "IND", "MEM"]

        # 🟡 ASSISTÊNCIA: Times que permitem muita circulação de bola
        self.assist_targets = ["WAS", "DET", "UTA", "ATL"]

    def get_adjustment_factor(self, opponent_abbr, position, market_type="PTS"):
        """
        Calcula o multiplicador de ajuste para a média do jogador.
        
        Args:
            opponent_abbr (str): Sigla do adversário (ex: 'WAS').
            position (str): Posição do jogador (PG, SG, SF, PF, C).
            market_type (str): O mercado principal (PTS, REB, AST).
        
        Returns:
            float: Fator de multiplicação (ex: 1.08 para +8%, 0.92 para -8%).
        """
        factor = 1.0
        opp = opponent_abbr.upper()
        pos = position.upper()
        
        # --- 1. AJUSTE POR POSIÇÃO (PTS/GERAL) ---
        # Se o adversário é uma "Peneira" na posição
        if opp in self.leaks.get(pos, []):
            factor += 0.08  # Boost de +8% (Cenário muito favorável)
            
        # Se o adversário é uma "Parede" na posição
        elif opp in self.walls.get(pos, []):
            factor -= 0.08  # Nerf de -8% (Cenário difícil)

        # --- 2. AJUSTE ESPECÍFICO POR MERCADO ---
        if market_type == "REB":
            if opp in self.rebound_targets:
                factor += 0.05 # +5% extra para rebotes contra times fracos no vidro
            elif opp in ["MIN", "ORL"]: # Times gigantes
                factor -= 0.05
                
        elif market_type == "AST":
            if opp in self.assist_targets:
                factor += 0.05 # +5% extra para assistências em jogos corridos

        # --- 3. TRAVA DE SEGURANÇA ---
        # Impede ajustes extremos (Máximo +/- 15%)
        if factor > 1.15: factor = 1.15
        if factor < 0.85: factor = 0.85
            
        return round(factor, 3)

    def get_matchup_grade(self, opponent_abbr, position):
        """Retorna uma nota visual para o confronto (A+ até F)."""
        factor = self.get_adjustment_factor(opponent_abbr, position)
        if factor >= 1.08: return "A+ (Excelente)"
        if factor >= 1.04: return "B (Bom)"
        if factor >= 1.00: return "C (Neutro)"
        if factor >= 0.95: return "D (Difícil)"
        return "F (Pesadelo)"