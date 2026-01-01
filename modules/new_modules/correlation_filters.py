# modules/new_modules/correlation_filters.py
"""
CorrelationValidator - Valida combinações para trixies com regras leves de correlação.
"""

from typing import List, Dict, Tuple
import logging

logger = logging.getLogger(__name__)

class CorrelationValidator:
    def __init__(self):
        self.position_groups = {
            "BALL_DOMINANT": ["PG", "SG"],
            "PAINT": ["C", "PF"],
            "WING": ["SF", "SG"]
        }

    def validate_trixie(self, trixie_players: List[Dict]) -> Tuple[bool, List[str], float]:
        violations = []
        score_adjustment = 1.0

        teams = [p.get("team") for p in trixie_players]
        team_counts: Dict[str, int] = {}
        for team in teams:
            team_counts[team] = team_counts.get(team, 0) + 1
        for team, count in team_counts.items():
            if count > 2:
                violations.append(f"TRIPLO_MESMO_TIME({team})")
                score_adjustment *= 0.7

        positions = [str(p.get("position", "")).upper() for p in trixie_players]
        if positions.count("PG") > 1:
            violations.append("CANIBALISMO_PG")
            score_adjustment *= 0.8
        if positions.count("C") > 1:
            violations.append("CANIBALISMO_C")
            score_adjustment *= 0.85

        spreads = [abs(p.get("spread", 0)) for p in trixie_players]
        max_spread = max(spreads) if spreads else 0
        if max_spread > 12:
            violations.append("BLOWOUT_ALTO")
            score_adjustment *= 0.75

        all_teses = []
        for p in trixie_players:
            tags = []
            if p.get("theses"):
                tags = [t.get("name") for t in p.get("theses", [])]
            all_teses.extend(tags)
        unique_teses = set(all_teses)
        if len(unique_teses) < 3 and len(all_teses) >= 6:
            violations.append("POUCA_DIVERSIDADE_TESES")
            score_adjustment *= 0.9

        is_valid = len(violations) == 0
        return is_valid, violations, score_adjustment

    def validate_recommendation_trio(self, trio: List[Dict]) -> Dict:
        is_valid, violations, score_adjustment = self.validate_trixie(trio)
        markets = []
        for p in trio:
            if p.get('theses'):
                markets.append(p['theses'][0].get('market'))
        if markets.count('PTS') >= 2:
            score_adjustment *= 0.95
        return {"is_valid": is_valid, "violations": violations, "score_adjustment": score_adjustment}

    def validate_group(self, player_ids: List[int], category: str) -> Dict:
        # Modo lite: sem dados de co-ocorrência aqui
        return {
            "problematic_pairs": [],
            "score_adjustment": 1.0,
            "is_valid": True
        }
