"""
Analisa impacto de lesões e efeito cascata
"""

class InjuryAnalyzer:
    def __init__(self):
        self.position_hierarchy = {
            "PG": ["SG", "SF"],
            "SG": ["PG", "SF"],
            "SF": ["SG", "PF"],
            "PF": ["SF", "C"],
            "C": ["PF", "SF"]
        }
    
    def analyze_team_injury_impact(self, team_abbr, roster):
        """Analisa impacto de lesões em um time"""
        impact_report = {
            "missing_starters": [],
            "beneficiaries": [],
            "usage_increases": {},
            "minute_increases": {}
        }
        
        # Identificar titulares lesionados
        for player in roster:
            if player.get("STARTER") and self._is_player_out(player):
                impact_report["missing_starters"].append({
                    "name": player["PLAYER"],
                    "position": player["POSITION"],
                    "avg_min": player.get("MIN_AVG", 25)
                })
        
        # Calcular redistribuição
        for missing in impact_report["missing_starters"]:
            beneficiaries = self._find_replacement_candidates(
                roster, missing["position"]
            )
            
            for beneficiary in beneficiaries:
                minute_increase = self._calculate_minute_increase(
                    beneficiary, missing
                )
                
                impact_report["beneficiaries"].append(beneficiary["PLAYER"])
                impact_report["minute_increases"][beneficiary["PLAYER"]] = minute_increase
                impact_report["usage_increases"][beneficiary["PLAYER"]] = minute_increase * 0.4
        
        return impact_report
    
    def _is_player_out(self, player):
        """Verifica se jogador está fora"""
        injury_status = player.get("INJURY_STATUS", "").upper()
        return "OUT" in injury_status or "GTD" in injury_status