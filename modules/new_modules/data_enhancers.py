"""
Funções auxiliares para enriquecimento de dados
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import os

class DataEnhancer:
    def __init__(self, cache_dir="cache"):
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
    
    def enhance_player_stats(self, player_stats):
        """Adiciona métricas derivadas às estatísticas do jogador"""
        enhanced = player_stats.copy()
        
        # Eficiência
        min_l5 = enhanced.get("min_L5", 1)
        if min_l5 > 0:
            enhanced["pts_per_min"] = enhanced.get("pts_L5", 0) / min_l5
            enhanced["reb_per_min"] = enhanced.get("reb_L5", 0) / min_l5
            enhanced["ast_per_min"] = enhanced.get("ast_L5", 0) / min_l5
            enhanced["pra_per_min"] = enhanced.get("pra_L5", 0) / min_l5
        
        # Consistência score
        cv_score = self._calculate_consistency_score(enhanced)
        enhanced["consistency_score"] = cv_score
        
        # Volatility classification
        enhanced["volatility_score"] = self._calculate_volatility_score(enhanced)
        enhanced["volatility"] = self._classify_volatility(enhanced["volatility_score"])
        
        # Upside potential
        enhanced["upside_potential"] = self._calculate_upside_potential(enhanced)
        
        # Risk factor
        enhanced["risk_factor"] = self._calculate_risk_factor(enhanced)
        
        # Experience adjustment
        exp = enhanced.get("exp", 1)
        enhanced["experience_factor"] = min(1.0, exp / 5.0)  # Cap at 5 years
        
        return enhanced
    
    def _calculate_consistency_score(self, stats):
        """Calcula score de consistência baseado em CVs"""
        cv_weights = {
            "min_cv": 0.35,
            "pts_cv": 0.25,
            "reb_cv": 0.20,
            "ast_cv": 0.20
        }
        
        total_score = 0
        total_weight = 0
        
        for cv_type, weight in cv_weights.items():
            cv_value = stats.get(cv_type, 1.0)
            # CV menor é melhor (mais consistente)
            cv_score = max(0, 100 * (1 - min(cv_value, 1.5) / 1.5))
            total_score += cv_score * weight
            total_weight += weight
        
        if total_weight > 0:
            return total_score / total_weight
        
        return 50.0
    
    def _calculate_volatility_score(self, stats):
        """Calcula score de volatilidade (0-1, maior = mais volátil)"""
        cv_values = [
            stats.get("min_cv", 1.0),
            stats.get("pts_cv", 1.0),
            stats.get("reb_cv", 1.0),
            stats.get("ast_cv", 1.0)
        ]
        
        avg_cv = np.mean([cv for cv in cv_values if cv > 0])
        volatility = min(1.0, avg_cv)
        
        return volatility
    
    def _classify_volatility(self, volatility_score):
        """Classifica volatilidade em categorias"""
        if volatility_score < 0.3:
            return "low"
        elif volatility_score < 0.6:
            return "medium"
        else:
            return "high"
    
    def _calculate_upside_potential(self, stats):
        """Calcula potencial de upside baseado em vários fatores"""
        upside_factors = []
        
        # Fator 1: Eficiência por minuto
        pra_per_min = stats.get("pra_per_min", 0)
        if pra_per_min > 0.9:
            upside_factors.append(1.2)
        elif pra_per_min > 0.7:
            upside_factors.append(1.1)
        elif pra_per_min > 0.5:
            upside_factors.append(1.0)
        else:
            upside_factors.append(0.9)
        
        # Fator 2: Consistência
        consistency = stats.get("consistency_score", 50) / 100
        upside_factors.append(consistency)
        
        # Fator 3: Experiência (jovens têm mais upside)
        exp = stats.get("exp", 5)
        if exp <= 2:
            upside_factors.append(1.15)  # Rookie/Sophomore
        elif exp <= 5:
            upside_factors.append(1.05)  # Desenvolvimento
        else:
            upside_factors.append(1.0)   # Estabelecido
        
        # Fator 4: Role (reservas têm mais upside em minutos extras)
        role = stats.get("role", "deep_bench")
        if role in ["bench_scorer", "rotation"]:
            upside_factors.append(1.1)
        elif role == "deep_bench":
            upside_factors.append(1.15)
        else:
            upside_factors.append(1.0)
        
        # Média dos fatores
        avg_upside = np.mean(upside_factors)
        
        return round(avg_upside, 2)
    
    def _calculate_risk_factor(self, stats):
        """Calcula fator de risco (maior = mais risco)"""
        risk_factors = []
        
        # Volatilidade
        volatility = stats.get("volatility_score", 0.5)
        risk_factors.append(volatility)
        
        # Minutos inconsistentes
        min_cv = stats.get("min_cv", 1.0)
        risk_factors.append(min_cv)
        
        # Experiência baixa
        exp = stats.get("exp", 5)
        if exp <= 2:
            risk_factors.append(0.7)  # Mais risco
        else:
            risk_factors.append(0.3)  # Menos risco
        
        # Role (deep bench = mais risco)
        role = stats.get("role", "deep_bench")
        if role == "deep_bench":
            risk_factors.append(0.8)
        elif role == "rotation":
            risk_factors.append(0.6)
        else:
            risk_factors.append(0.4)
        
        avg_risk = np.mean(risk_factors)
        return round(avg_risk, 2)
    
    def enhance_game_context(self, game_ctx):
        """Adiciona métricas derivadas ao contexto do jogo"""
        enhanced = game_ctx.copy()
        
        # Calcular spread absoluto
        spread = enhanced.get("spread")
        if spread is not None:
            try:
                enhanced["spread_abs"] = abs(float(spread))
            except:
                enhanced["spread_abs"] = 0.0
        
        # Classificar blowout risk
        spread_abs = enhanced.get("spread_abs", 0)
        if spread_abs >= 15:
            enhanced["blowout_risk"] = "EXTREME"
        elif spread_abs >= 12:
            enhanced["blowout_risk"] = "HIGH"
        elif spread_abs >= 8:
            enhanced["blowout_risk"] = "MEDIUM"
        else:
            enhanced["blowout_risk"] = "LOW"
        
        # Classificar pace
        pace = enhanced.get("pace_expected", 100)
        if pace >= 102:
            enhanced["pace_category"] = "VERY_HIGH"
        elif pace >= 100:
            enhanced["pace_category"] = "HIGH"
        elif pace >= 98:
            enhanced["pace_category"] = "AVERAGE"
        else:
            enhanced["pace_category"] = "LOW"
        
        # Classificar total
        total = enhanced.get("total", 220)
        if total >= 235:
            enhanced["total_category"] = "VERY_HIGH"
        elif total >= 225:
            enhanced["total_category"] = "HIGH"
        elif total >= 215:
            enhanced["total_category"] = "AVERAGE"
        else:
            enhanced["total_category"] = "LOW"
        
        # Score de atratividade do jogo
        enhanced["game_attractiveness"] = self._calculate_game_attractiveness(enhanced)
        
        return enhanced
    
    def _calculate_game_attractiveness(self, game_ctx):
        """Calcula score de atratividade do jogo para trixies"""
        score = 50.0  # Base
        
        # Fator 1: Close game (melhor para starters)
        spread_abs = game_ctx.get("spread_abs", 0)
        if spread_abs < 6:
            score += 20  # Jogo muito próximo
        elif spread_abs < 10:
            score += 10  # Jogo razoavelmente próximo
        elif spread_abs > 14:
            score -= 10  # Blowout potencial
        
        # Fator 2: Pace alto (mais volume)
        pace = game_ctx.get("pace_expected", 100)
        if pace > 102:
            score += 15
        elif pace > 100:
            score += 5
        
        # Fator 3: Total alto (shootout)
        total = game_ctx.get("total", 220)
        if total > 230:
            score += 10
        
        # Normalizar
        score = max(0, min(100, score))
        
        return round(score, 1)
    
    def calculate_matchup_advantage(self, player_ctx, opponent_ctx):
        """Calcula vantagem de matchup para um jogador"""
        advantage_score = 50.0
        
        # DvP advantage
        dvp_data = player_ctx.get("dvp_data", {})
        if dvp_data:
            overall = dvp_data.get("overall", 1.0)
            if overall > 1.05:
                advantage_score += (overall - 1.0) * 100
            elif overall < 0.95:
                advantage_score -= (1.0 - overall) * 100
        
        # Rebate advantage
        if (player_ctx.get("reb_per_min", 0) > 0.2 and 
            opponent_ctx.get("opponent_reb_rank", 15) >= 20):
            advantage_score += 10
        
        # Assist advantage
        if (player_ctx.get("ast_per_min", 0) > 0.15 and 
            opponent_ctx.get("opponent_ast_rank", 15) >= 20):
            advantage_score += 10
        
        # Experience advantage
        if player_ctx.get("is_veteran") and opponent_ctx.get("is_young_team", False):
            advantage_score += 5
        
        # Normalizar
        advantage_score = max(0, min(100, advantage_score))
        
        return round(advantage_score, 1)
    
    def generate_player_snapshot(self, player_ctx):
        """Gera snapshot resumido do jogador para análise rápida"""
        snapshot = {
            "name": player_ctx.get("name"),
            "team": player_ctx.get("team"),
            "position": player_ctx.get("position"),
            "role": player_ctx.get("role"),
            "key_stats": {
                "min_L5": round(player_ctx.get("min_L5", 0), 1),
                "pts_L5": round(player_ctx.get("pts_L5", 0), 1),
                "reb_L5": round(player_ctx.get("reb_L5", 0), 1),
                "ast_L5": round(player_ctx.get("ast_L5", 0), 1),
                "pra_L5": round(player_ctx.get("pra_L5", 0), 1)
            },
            "efficiency": {
                "pts_per_min": round(player_ctx.get("pts_per_min", 0), 2),
                "reb_per_min": round(player_ctx.get("reb_per_min", 0), 2),
                "ast_per_min": round(player_ctx.get("ast_per_min", 0), 2),
                "pra_per_min": round(player_ctx.get("pra_per_min", 0), 2)
            },
            "consistency": {
                "score": player_ctx.get("consistency_score", 50),
                "volatility": player_ctx.get("volatility", "medium")
            },
            "upside": {
                "potential": player_ctx.get("upside_potential", 1.0),
                "prob_90p_pts": player_ctx.get("prob_90p_pts", 50)
            },
            "risk": player_ctx.get("risk_factor", 0.5)
        }
        
        # Adicionar classes se disponível
        if player_ctx.get("player_class"):
            snapshot["classes"] = player_ctx["player_class"]
        
        # Adicionar archetypes se disponível
        if player_ctx.get("archetypes"):
            snapshot["archetypes"] = player_ctx["archetypes"]
        
        return snapshot