# modules/new_modules/player_classifier.py
"""
PlayerClassifier - Classificação avançada de jogadores para estratégias NAS
Identifica archetypes e perfis de jogo para seleção de trixies
"""
import numpy as np
from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)

class PlayerClassifier:
    """Classificador de jogadores para o sistema NAS"""
    
    def __init__(self):
        self.archetype_definitions = self._get_archetype_definitions()
        self.position_weights = self._get_position_weights()
    
    def _get_archetype_definitions(self) -> Dict[str, Dict]:
        """Definições de archetypes com critérios e pesos"""
        return {
            "PaintBeast": {
                "description": "Jogador dominante no garrafão",
                "primary_stats": ["reb_per_min", "blk_avg", "fg_pct"],
                "thresholds": {
                    "reb_per_min": 0.22,
                    "blk_avg": 1.0,
                    "min_avg": 25
                },
                "weights": {"reb_per_min": 0.5, "blk_avg": 0.3, "fg_pct": 0.2},
                "positions": ["C", "PF"]
            },
            "FoulMerchant": {
                "description": "Especialista em criar faltas e jogar agressivo",
                "primary_stats": ["fta_per_min", "usage_rate", "pts_in_paint"],
                "thresholds": {
                    "fta_per_min": 0.5,
                    "usage_rate": 25,
                    "pts_in_paint": 8
                },
                "weights": {"fta_per_min": 0.4, "usage_rate": 0.4, "pts_in_paint": 0.2},
                "positions": ["C", "PF", "SF"]
            },
            "VolumeShooter": {
                "description": "Arremessador de alto volume",
                "primary_stats": ["fg3a_per_min", "pts_per_min", "shot_attempts"],
                "thresholds": {
                    "fg3a_per_min": 0.4,
                    "pts_per_min": 0.8,
                    "fg3_pct": 0.34
                },
                "weights": {"fg3a_per_min": 0.5, "pts_per_min": 0.3, "shot_attempts": 0.2},
                "positions": ["SG", "SF", "PG"]
            },
            "Distributor": {
                "description": "Jogador focado em assistências e controle",
                "primary_stats": ["ast_per_min", "ast_to_ratio", "usage_rate"],
                "thresholds": {
                    "ast_per_min": 0.18,
                    "ast_to_ratio": 2.5,
                    "min_avg": 28
                },
                "weights": {"ast_per_min": 0.6, "ast_to_ratio": 0.3, "usage_rate": 0.1},
                "positions": ["PG", "SG", "SF"]
            },
            "GlassBanger": {
                "description": "Especialista em rebotes ofensivos e defensivos",
                "primary_stats": ["oreb_per_min", "dreb_per_min", "reb_per_min"],
                "thresholds": {
                    "reb_per_min": 0.25,
                    "oreb_pct": 0.25,
                    "min_avg": 22
                },
                "weights": {"oreb_per_min": 0.4, "dreb_per_min": 0.4, "reb_per_min": 0.2},
                "positions": ["C", "PF"]
            },
            "PerimeterLock": {
                "description": "Defensor elite no perímetro",
                "primary_stats": ["stl_per_min", "def_rating", "opponent_fg_pct"],
                "thresholds": {
                    "stl_per_min": 0.15,
                    "def_rating": 105,
                    "min_avg": 25
                },
                "weights": {"stl_per_min": 0.4, "def_rating": 0.4, "opponent_fg_pct": 0.2},
                "positions": ["SG", "SF", "PG"]
            },
            "ClutchPerformer": {
                "description": "Jogador que performa em momentos decisivos",
                "primary_stats": ["clutch_pts", "clutch_fg_pct", "clutch_ast"],
                "thresholds": {
                    "clutch_pts": 3.5,
                    "clutch_fg_pct": 0.45,
                    "usage_rate": 22
                },
                "weights": {"clutch_pts": 0.5, "clutch_fg_pct": 0.3, "clutch_ast": 0.2},
                "positions": ["PG", "SG", "SF", "PF"]
            },
            "TransitionDemon": {
                "description": "Especialista em jogadas em transição",
                "primary_stats": ["fast_break_pts", "pace", "transition_poss"],
                "thresholds": {
                    "fast_break_pts": 4.0,
                    "pace": 102,
                    "min_avg": 24
                },
                "weights": {"fast_break_pts": 0.6, "pace": 0.3, "transition_poss": 0.1},
                "positions": ["PG", "SG", "SF"]
            }
        }
    
    def _get_position_weights(self) -> Dict[str, Dict[str, float]]:
        """Pesos de estatísticas por posição"""
        return {
            "PG": {"AST": 1.5, "PTS": 1.2, "REB": 0.7},
            "SG": {"PTS": 1.4, "AST": 1.0, "REB": 0.7},
            "SF": {"PTS": 1.2, "REB": 1.0, "AST": 0.9},
            "PF": {"REB": 1.4, "PTS": 1.0, "AST": 0.7},
            "C": {"REB": 1.8, "PTS": 1.0, "AST": 0.5}
        }
    
    def classify_player(self, player_ctx: Dict[str, Any]) -> List[Dict]:
        """
        Classifica um jogador em múltiplos archetypes com base em suas estatísticas
        
        Args:
            player_ctx: Contexto do jogador com estatísticas
        
        Returns:
            Lista de archetypes com scores de confiança
        """
        classifications = []
        position = player_ctx.get("position", "").upper()
        
        for archetype_name, archetype_def in self.archetype_definitions.items():
            # Verificar se a posição é compatível
            if position not in archetype_def["positions"]:
                continue
            
            # Calcular score do archetype
            score, confidence = self._calculate_archetype_score(player_ctx, archetype_def)
            
            if confidence >= 0.6:  # Threshold mínimo para considerar válido
                classifications.append({
                    "archetype": archetype_name,
                    "description": archetype_def["description"],
                    "confidence": confidence,
                    "score": score,
                    "primary_stats": archetype_def["primary_stats"]
                })
        
        # Ordenar por confiança
        classifications.sort(key=lambda x: x["confidence"], reverse=True)
        
        # Manter apenas top 3 archetypes
        return classifications[:3]
    
    def _calculate_archetype_score(self, player_ctx: Dict[str, Any], archetype_def: Dict[str, Any]) -> tuple[float, float]:
        """Calcula score e confiança para um archetype específico"""
        total_score = 0.0
        total_weight = 0.0
        thresholds_met = 0
        total_thresholds = len(archetype_def["thresholds"])
        
        for stat, weight in archetype_def["weights"].items():
            player_value = player_ctx.get(stat, 0)
            threshold = archetype_def["thresholds"].get(stat, 0)
            
            if threshold > 0:
                # Calcular quanto excede o threshold (mínimo 0, máximo 2x threshold)
                ratio = min(2.0, max(0.0, player_value / threshold))
                stat_score = ratio * weight
                
                # Verificar se atingiu o threshold
                if player_value >= threshold:
                    thresholds_met += 1
                
                total_score += stat_score
                total_weight += weight
        
        if total_weight == 0:
            return 0.0, 0.0
        
        # Calcular score final e confiança
        normalized_score = total_score / total_weight
        threshold_confidence = thresholds_met / total_thresholds
        confidence = min(1.0, normalized_score * 0.7 + threshold_confidence * 0.3)
        
        return normalized_score, confidence
    
    def get_role_classification(self, player_ctx: Dict[str, Any]) -> Dict[str, Any]:
        """Classificação de role para o sistema de trixies (substituído PRA por stats individuais)"""
        # Usar soma de PTS + REB + AST em vez de PRA
        pts_avg = player_ctx.get("pts_L5", 0)
        reb_avg = player_ctx.get("reb_L5", 0)
        ast_avg = player_ctx.get("ast_L5", 0)
        combined_avg = pts_avg + reb_avg + ast_avg  # Substituto para pra_L5
        
        min_avg = player_ctx.get("min_L5", 0)
        usage = player_ctx.get("usage", "low")
        is_starter = player_ctx.get("is_starter", False)
        
        # Determinar role principal (ajustado thresholds para abaixar restrições)
        if is_starter and combined_avg >= 25:  # Abaixado de 30
            role = "star"
            risk_level = "low"
        elif is_starter and combined_avg >= 15:  # Abaixado de 18
            role = "starter"
            risk_level = "low"
        elif not is_starter and combined_avg >= 20:  # Abaixado de 25
            role = "bench_scorer"
            risk_level = "medium"
        elif not is_starter and combined_avg >= 12:  # Abaixado de 15
            role = "rotation"
            risk_level = "medium"
        else:
            role = "deep_bench"
            risk_level = "high"
        
        # Determinar perfil de jogo
        reb_per_min = player_ctx.get("reb_per_min", 0)
        ast_per_min = player_ctx.get("ast_per_min", 0)
        
        if reb_per_min >= 0.22 and pts_avg < 16:
            style = "rebounder"
        elif ast_per_min >= 0.18:
            style = "playmaker"
        elif pts_avg >= 18:
            style = "scorer"
        elif reb_per_min >= 0.18 and pts_avg >= 12:
            style = "hustle"
        else:
            style = "role"
        
        return {
            "role": role,
            "risk_level": risk_level,
            "style": style,
            "usage_profile": usage,
            "minutes_profile": "high" if min_avg >= 30 else "medium" if min_avg >= 20 else "low"
        }
    
    def generate_player_tags(self, player_ctx: Dict[str, Any], game_ctx: Dict[str, Any]) -> List[str]:
        """Gera tags estratégicas para o jogador"""
        tags = []
        archetypes = self.classify_player(player_ctx)
        
        # Adicionar archetypes como tags
        for archetype in archetypes:
            if archetype["confidence"] >= 0.7:
                tags.append(f"archetype_{archetype['archetype'].lower()}")
        
        # Adicionar tags baseadas em contexto
        role_info = self.get_role_classification(player_ctx)
        tags.append(f"role_{role_info['role']}")
        tags.append(f"risk_{role_info['risk_level']}")
        tags.append(f"style_{role_info['style']}")
        
        # Tags de matchup
        if game_ctx.get("is_high_pace", False):
            tags.append("pace_high")
        if game_ctx.get("is_low_pace", False):
            tags.append("pace_low")
        if player_ctx.get("is_underdog", False):
            tags.append("underdog")
        if player_ctx.get("is_b2b", False):
            tags.append("b2b")
        
        # Tags de lesões
        team_injuries = player_ctx.get("team_injuries", 0)
        if team_injuries >= 2:
            tags.append("injury_opportunity")
        if team_injuries >= 3:
            tags.append("injury_boom")
        
        return list(set(tags))  # Remover duplicatas