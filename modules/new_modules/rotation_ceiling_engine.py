# modules/new_modules/rotation_ceiling_engine.py
"""
RotationCeilingEngine - Motor de análise de teto estatístico para jogadores
Calcula ceilings de performance com base em rotação, lesões e contexto do jogo
"""
import numpy as np
from typing import Dict, List, Any, Optional
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class RotationCeilingEngine:
    """Motor de análise de ceiling estatístico para rotações"""
    
    def __init__(self):
        self.ceiling_multipliers = self._get_ceiling_multipliers()
        self.context_factors = self._get_context_factors()
    
    def _get_ceiling_multipliers(self) -> Dict[str, Dict[str, float]]:
        """Multiplicadores de ceiling por archetype e contexto"""
        return {
            "PaintBeast": {
                "base": 1.35,
                "injury_boost": 1.50,
                "pace_boost": 1.25
            },
            "VolumeShooter": {
                "base": 1.40,
                "injury_boost": 1.45,
                "pace_boost": 1.35
            },
            "Distributor": {
                "base": 1.30,
                "injury_boost": 1.40,
                "pace_boost": 1.20
            },
            "PerimeterLock": {
                "base": 1.45,
                "injury_boost": 1.35,
                "pace_boost": 1.30
            },
            "ClutchPerformer": {
                "base": 1.50,
                "injury_boost": 1.40,
                "pace_boost": 1.25
            },
            "default": {
                "base": 1.25,
                "injury_boost": 1.35,
                "pace_boost": 1.20
            }
        }
    
    def _get_context_factors(self) -> Dict[str, float]:
        """Fatores de contexto que afetam o ceiling"""
        return {
            "b2b": 0.90,
            "travel": 0.95,
            "back_to_back_travel": 0.85,
            "rest_advantage": 1.05,
            "home_court": 1.03,
            "high_total": 1.08,
            "blowout_risk": 0.85
        }
    
    def calculate_player_ceiling(self, player_ctx: Dict[str, Any], game_ctx: Dict[str, Any], archetype: str = None) -> Dict[str, Any]:
        """
        Calcula ceiling estatístico para um jogador
        
        Args:
            player_ctx: Contexto do jogador
            game_ctx: Contexto do jogo
            archetype: Archetype do jogador (se conhecido)
        
        Returns:
            Dicionário com ceilings para diferentes estatísticas
        """
        base_stats = self._get_base_stats(player_ctx)
        ceilings = {}
        
        # Determinar archetype se não fornecido
        if archetype is None:
            from modules.new_modules.player_classifier import PlayerClassifier
            classifier = PlayerClassifier()
            archetypes = classifier.classify_player(player_ctx)
            archetype = archetypes[0]["archetype"] if archetypes else "default"
        
        # Obter multiplicadores
        multipliers = self.ceiling_multipliers.get(archetype, self.ceiling_multipliers["default"])
        
        # Calcular ceiling base
        base_ceiling = multipliers["base"]
        
        # Aplicar boosts de contexto
        if player_ctx.get("team_injuries", 0) >= 2:
            base_ceiling *= multipliers["injury_boost"] / multipliers["base"]
        
        if game_ctx.get("is_high_pace", False):
            base_ceiling *= multipliers["pace_boost"] / multipliers["base"]
        
        # Aplicar fatores de contexto negativos
        context_penalty = self._calculate_context_penalty(player_ctx, game_ctx)
        base_ceiling *= context_penalty
        
        # Calcular ceilings para cada estatística
        for stat, value in base_stats.items():
            if value > 0:
                # Ceiling 90% - bom jogo
                ceilings[f"{stat}_ceil_90"] = value * (1 + (base_ceiling - 1) * 0.7)
                # Ceiling 95% - jogo excelente
                ceilings[f"{stat}_ceil_95"] = value * (1 + (base_ceiling - 1) * 0.9)
                # Ceiling absoluto - melhor jogo possível
                ceilings[f"{stat}_ceil_abs"] = value * base_ceiling
        
        # Calcular ceiling PRA (soma)
        if "pts" in ceilings and "reb" in ceilings and "ast" in ceilings:
            ceilings["pra_ceil_90"] = ceilings["pts_ceil_90"] + ceilings["reb_ceil_90"] + ceilings["ast_ceil_90"]
            ceilings["pra_ceil_95"] = ceilings["pts_ceil_95"] + ceilings["reb_ceil_95"] + ceilings["ast_ceil_95"]
            ceilings["pra_ceil_abs"] = ceilings["pts_ceil_abs"] + ceilings["reb_ceil_abs"] + ceilings["ast_ceil_abs"]
        
        # Adicionar metadata
        ceilings.update({
            "base_multiplier": base_ceiling,
            "archetype_used": archetype,
            "context_penalty": context_penalty,
            "calculated_at": datetime.now().isoformat()
        })
        
        return ceilings
    
    def _get_base_stats(self, player_ctx: Dict[str, Any]) -> Dict[str, float]:
        """Extrai estatísticas base do contexto do jogador"""
        return {
            "pts": player_ctx.get("pts_L5", player_ctx.get("pts_avg", 0)),
            "reb": player_ctx.get("reb_L5", player_ctx.get("reb_avg", 0)),
            "ast": player_ctx.get("ast_L5", player_ctx.get("ast_avg", 0)),
            "stl": player_ctx.get("stl_L5", player_ctx.get("stl_avg", 0)),
            "blk": player_ctx.get("blk_L5", player_ctx.get("blk_avg", 0)),
            "min": player_ctx.get("min_L5", player_ctx.get("min_avg", 0)),
            "3pm": player_ctx.get("3pm_L5", player_ctx.get("3pm_avg", 0))
        }
    
    def _calculate_context_penalty(self, player_ctx: Dict[str, Any], game_ctx: Dict[str, Any]) -> float:
        """Calcula penalidade de contexto para o ceiling"""
        penalty = 1.0
        
        # Fatores de fadiga
        if player_ctx.get("is_b2b", False):
            penalty *= self.context_factors["b2b"]
        
        timezones = player_ctx.get("timezones_traveled", 0)
        if timezones >= 2:
            penalty *= self.context_factors["travel"]
            if player_ctx.get("is_b2b", False):
                penalty *= self.context_factors["back_to_back_travel"] / self.context_factors["travel"]
        
        # Vantagem de descanso
        games_last_6 = player_ctx.get("games_last_6", 3)
        if games_last_6 <= 2:
            penalty *= self.context_factors["rest_advantage"]
        
        # Fatores do jogo
        if game_ctx.get("home_abbr") == player_ctx.get("team"):
            penalty *= self.context_factors["home_court"]
        
        total = game_ctx.get("total", 225)
        if total >= 235:
            penalty *= self.context_factors["high_total"]
        
        spread = game_ctx.get("spread", 0)
        if abs(spread) >= 12:
            penalty *= self.context_factors["blowout_risk"]
        
        return max(0.6, min(1.2, penalty))  # Limitar entre 0.6 e 1.2
    
    def analyze_team_ceiling_potential(self, team_players: List[Dict[str, Any]], game_ctx: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analisa o potencial de ceiling para todo o time
        
        Args:
            team_players: Lista de contextos de jogadores do time
            game_ctx: Contexto do jogo
        
        Returns:
            Análise completa do potencial de ceiling do time
        """
        analysis = {
            "team": team_players[0].get("team", "unknown") if team_players else "unknown",
            "players_analyzed": 0,
            "high_ceiling_players": [],
            "team_ceiling_score": 0.0,
            "risk_factors": [],
            "opportunity_factors": [],
            "detailed_analysis": {}
        }
        
        if not team_players:
            return analysis
        
        total_ceiling_score = 0.0
        high_ceiling_count = 0
        
        for player in team_players:
            player_name = player.get("name", "unknown")
            player_archetypes = player.get("archetypes", [])
            archetype_name = player_archetypes[0]["archetype"] if player_archetypes else None
            
            try:
                ceilings = self.calculate_player_ceiling(player, game_ctx, archetype_name)
                analysis["detailed_analysis"][player_name] = ceilings
                
                # Calcular score de ceiling do jogador
                pra_ceil_95 = ceilings.get("pra_ceil_95", 0)
                pra_avg = player.get("pra_L5", player.get("pra_avg", 0))
                
                if pra_avg > 0:
                    ceiling_ratio = pra_ceil_95 / pra_avg
                    player_ceiling_score = ceiling_ratio * player.get("min_L5", 0) / 36
                
                    total_ceiling_score += player_ceiling_score
                    
                    if ceiling_ratio >= 1.4:
                        high_ceiling_count += 1
                        analysis["high_ceiling_players"].append({
                            "name": player_name,
                            "ceiling_ratio": ceiling_ratio,
                            "pra_ceil_95": pra_ceil_95,
                            "archetype": archetype_name
                        })
            except Exception as e:
                logger.warning(f"Erro ao calcular ceiling para {player_name}: {e}")
                continue
        
        analysis["players_analyzed"] = len(team_players)
        analysis["team_ceiling_score"] = total_ceiling_score / max(len(team_players), 1)
        analysis["high_ceiling_percentage"] = high_ceiling_count / max(len(team_players), 1)
        
        # Identificar fatores de risco e oportunidade
        team_injuries = max(p.get("team_injuries", 0) for p in team_players)
        if team_injuries >= 2:
            analysis["opportunity_factors"].append(f"Injúrias no time ({team_injuries} jogadores)")
        
        if any(p.get("is_b2b", False) for p in team_players):
            analysis["risk_factors"].append("Back-to-back game")
        
        if game_ctx.get("is_high_pace", False):
            analysis["opportunity_factors"].append("Jogo de ritmo alto")
        
        if game_ctx.get("is_low_pace", False):
            analysis["risk_factors"].append("Jogo de ritmo baixo")
        
        return analysis
    
    def generate_ceiling_insights(self, ceiling_analysis: Dict[str, Any]) -> str:
        """Gera insights de ceiling em formato legível"""
        insights = []
        
        team = ceiling_analysis.get("team", "unknown")
        score = ceiling_analysis.get("team_ceiling_score", 0)
        high_ceiling_pct = ceiling_analysis.get("high_ceiling_percentage", 0)
        
        insights.append(f"📈 **Análise de Ceiling - {team}**")
        insights.append(f"   • Score de Ceiling do Time: {score:.2f}")
        insights.append(f"   • {high_ceiling_pct:.1%} dos jogadores com alto potencial explosivo")
        
        # Jogadores com alto ceiling
        high_ceiling_players = ceiling_analysis.get("high_ceiling_players", [])
        if high_ceiling_players:
            insights.append(f"\n⭐ **Jogadores com Alto Potencial Explosivo:**")
            for player in high_ceiling_players[:3]:  # Top 3
                insights.append(f"   • {player['name']} ({player['archetype']}): " +
                              f"Ceiling PRA 95% = {player['pra_ceil_95']:.1f}")
        
        # Fatores de oportunidade
        opportunity_factors = ceiling_analysis.get("opportunity_factors", [])
        if opportunity_factors:
            insights.append(f"\n✅ **Fatores de Oportunidade:**")
            for factor in opportunity_factors:
                insights.append(f"   • {factor}")
        
        # Fatores de risco
        risk_factors = ceiling_analysis.get("risk_factors", [])
        if risk_factors:
            insights.append(f"\n⚠️ **Fatores de Risco:**")
            for factor in risk_factors:
                insights.append(f"   • {factor}")
        
        return "\n".join(insights)