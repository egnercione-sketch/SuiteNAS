# modules/new_modules/archetype_engine.py
"""
ArchetypeEngine - Classificação leve de arquétipos por estatísticas médias.
"""

from typing import Dict, List

class ArchetypeEngine:
    ARCHETYPES = {
        "PaintBeast": {"min_reb_pct": 0.12, "min_paint_pts": 8},
        "FoulMerchant": {"min_fta": 6},
        "VolumeShooter": {"min_3pa": 8},
        "Distributor": {"min_ast_to": 2.5, "min_usage": 20},
        "GlassBanger": {"min_oreb_pct": 0.15, "min_screen_assists": 2},
        "PerimeterLock": {"min_stl": 1.5, "min_def_rating": 105},
        "ClutchPerformer": {"min_clutch_pts": 3, "min_clutch_fg": 0.45},
        "TransitionDemon": {"min_fast_break_pts": 4, "min_pace": 100}
    }

    def get_archetypes(self, player_id, player_stats: Dict = None) -> List[str]:
        if player_stats is None:
            return []

        archetypes = []

        if (player_stats.get("REB_AVG", 0) >= 8 and
            player_stats.get("PTS_AVG", 0) >= 10):
            archetypes.append("PaintBeast")

        if player_stats.get("THREEPA_AVG", 0) >= 6:
            archetypes.append("VolumeShooter")

        if (player_stats.get("AST_AVG", 0) >= 5 and
            player_stats.get("AST_TO_RATIO", 0) >= 2.0):
            archetypes.append("Distributor")

        if (player_stats.get("OREB_PCT", 0) >= 0.10 and
            player_stats.get("REB_AVG", 0) >= 7):
            archetypes.append("GlassBanger")

        if player_stats.get("STL_AVG", 0) >= 1.2:
            archetypes.append("PerimeterLock")

        return archetypes
