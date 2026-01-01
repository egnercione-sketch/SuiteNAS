"""
Funções de validação para dados e trixies
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json

class DataValidator:
    def __init__(self):
        self.validation_rules = self._load_validation_rules()
    
    def _load_validation_rules(self):
        """Carrega regras de validação"""
        return {
            "player_stats": {
                "min_avg": {"min": 0, "max": 48, "required": True},
                "pts_avg": {"min": 0, "max": 50, "required": True},
                "reb_avg": {"min": 0, "max": 25, "required": True},
                "ast_avg": {"min": 0, "max": 15, "required": True},
                "min_cv": {"min": 0, "max": 2.0, "required": False},
                "pts_cv": {"min": 0, "max": 2.0, "required": False}
            },
            "game_context": {
                "spread": {"min": -30, "max": 30, "required": False},
                "total": {"min": 180, "max": 280, "required": False},
                "pace_expected": {"min": 90, "max": 110, "required": False}
            }
        }
    
    def validate_player_stats(self, stats):
        """Valida estatísticas do jogador"""
        errors = []
        warnings = []
        
        for stat_name, rules in self.validation_rules["player_stats"].items():
            if stat_name in stats:
                value = stats[stat_name]
                
                # Verificar se é número
                if not isinstance(value, (int, float)):
                    errors.append(f"{stat_name}: valor não numérico ({value})")
                    continue
                
                # Verificar limites
                if value < rules["min"]:
                    warnings.append(f"{stat_name}: valor muito baixo ({value})")
                elif value > rules["max"]:
                    warnings.append(f"{stat_name}: valor muito alto ({value})")
                
                # Verificar outliers extremos
                if self._is_extreme_outlier(value, rules.get("min", 0), rules.get("max", 100)):
                    errors.append(f"{stat_name}: outlier extremo ({value})")
        
        # Validações cruzadas
        if "min_avg" in stats and "pts_avg" in stats:
            pts_per_min = stats["pts_avg"] / max(stats["min_avg"], 1)
            if pts_per_min > 1.5:  # Mais de 1.5 pontos por minuto é suspeito
                warnings.append(f"PTS/MIN muito alto: {pts_per_min:.2f}")
        
        return {
            "is_valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings
        }
    
    def validate_game_context(self, game_ctx):
        """Valida contexto do jogo"""
        errors = []
        warnings = []
        
        for field, rules in self.validation_rules["game_context"].items():
            if field in game_ctx and game_ctx[field] is not None:
                value = game_ctx[field]
                
                try:
                    value_float = float(value)
                    
                    if value_float < rules["min"]:
                        warnings.append(f"{field}: valor muito baixo ({value_float})")
                    elif value_float > rules["max"]:
                        warnings.append(f"{field}: valor muito alto ({value_float})")
                    
                    # Validações específicas
                    if field == "spread" and abs(value_float) > 20:
                        warnings.append("Spread muito alto, jogo provavelmente definido")
                    
                    if field == "total" and value_float > 250:
                        warnings.append("Total muito alto, verificar dados")
                    
                except (ValueError, TypeError):
                    if rules["required"]:
                        errors.append(f"{field}: valor inválido ({value})")
        
        return {
            "is_valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings
        }
    
    def validate_trixie_composition(self, trixie_players):
        """Valida composição de uma trixie"""
        errors = []
        warnings = []
        
        if len(trixie_players) < 3:
            errors.append("Trixie precisa ter pelo menos 3 jogadores")
        
        if len(trixie_players) > 5:
            warnings.append("Trixie com muitos jogadores (maior risco)")
        
        # Verificar diversidade de times
        teams = set(p.get("team") for p in trixie_players)
        if len(teams) < 2:
            warnings.append("Trixie com jogadores de apenas um time")
        
        # Verificar conflito de minutos
        total_expected_minutes = sum(p.get("expected_minutes", 0) for p in trixie_players)
        if total_expected_minutes > 144:  # 48 min * 3 jogadores
            warnings.append(f"Minutos totais muito altos: {total_expected_minutes:.1f}")
        
        # Verificar volatilidade geral
        high_volatility_count = sum(1 for p in trixie_players 
                                   if p.get("volatility") == "high")
        if high_volatility_count >= 2:
            warnings.append(f"{high_volatility_count} jogadores com alta volatilidade")
        
        return {
            "is_valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings
        }
    
    def validate_data_freshness(self, cache_timestamp, max_age_hours=24):
        """Valida se os dados não estão muito antigos"""
        if not cache_timestamp:
            return False, "Timestamp não disponível"
        
        try:
            if isinstance(cache_timestamp, str):
                cache_time = datetime.fromisoformat(cache_timestamp)
            else:
                cache_time = cache_timestamp
            
            age_hours = (datetime.now() - cache_time).total_seconds() / 3600
            
            if age_hours > max_age_hours:
                return False, f"Dados com {age_hours:.1f} horas (limite: {max_age_hours}h)"
            elif age_hours > 12:
                return True, f"Dados com {age_hours:.1f} horas - considerar atualização"
            else:
                return True, f"Dados recentes ({age_hours:.1f}h)"
                
        except Exception as e:
            return False, f"Erro ao validar timestamp: {str(e)}"
    
    def _is_extreme_outlier(self, value, min_val, max_val):
        """Verifica se um valor é outlier extremo"""
        range_size = max_val - min_val
        median = (max_val + min_val) / 2
        
        # Considerar outlier se estiver a mais de 3 "desvios" da mediana
        # (usando range_size como proxy para desvio)
        if range_size > 0:
            deviation = abs(value - median) / (range_size / 4)
            return deviation > 3
        
        return False
    
    def check_data_completeness(self, data_dict, required_fields):
        """Verifica completude dos dados"""
        missing_fields = []
        
        for field in required_fields:
            if field not in data_dict or data_dict[field] is None:
                missing_fields.append(field)
        
        completeness = 1.0 - (len(missing_fields) / len(required_fields))
        
        return {
            "completeness": completeness,
            "missing_fields": missing_fields,
            "is_complete": len(missing_fields) == 0
        }
    
    def validate_player_ctx_integrity(self, player_ctx):
        """Valida integridade do contexto do jogador"""
        issues = []
        
        required_fields = ["name", "team", "position", "min_L5", "pts_L5", "reb_L5", "ast_L5"]
        
        for field in required_fields:
            if field not in player_ctx or player_ctx[field] is None:
                issues.append(f"Campo obrigatório ausente: {field}")
        
        # Verificar valores negativos
        stat_fields = ["min_L5", "pts_L5", "reb_L5", "ast_L5", "pra_L5"]
        for field in stat_fields:
            if field in player_ctx:
                value = player_ctx[field]
                if isinstance(value, (int, float)) and value < 0:
                    issues.append(f"Valor negativo em {field}: {value}")
        
        # Verificar consistência PRA
        if all(f in player_ctx for f in ["pts_L5", "reb_L5", "ast_L5", "pra_L5"]):
            calculated_pra = player_ctx["pts_L5"] + player_ctx["reb_L5"] + player_ctx["ast_L5"]
            reported_pra = player_ctx["pra_L5"]
            
            if abs(calculated_pra - reported_pra) > 2.0:
                issues.append(f"PRA inconsistente: calculado={calculated_pra:.1f}, reportado={reported_pra:.1f}")
        
        return {
            "is_valid": len(issues) == 0,
            "issues": issues,
            "has_issues": len(issues) > 0
        }


class TrixieValidator:
    def __init__(self):
        self.min_score_threshold = 60.0
        self.max_volatility_ratio = 0.67  # Máximo 2/3 jogadores high volatility
    
    def validate_trixie_quality(self, trixie):
        """Valida qualidade geral de uma trixie"""
        validation_result = {
            "passed": True,
            "score_validation": {"passed": True, "message": ""},
            "diversity_validation": {"passed": True, "message": ""},
            "volatility_validation": {"passed": True, "message": ""},
            "minutes_validation": {"passed": True, "message": ""},
            "recommendations": []
        }
        
        players = trixie.get("players", [])
        score = trixie.get("score", 0)
        
        # 1. Validar score
        if score < self.min_score_threshold:
            validation_result["score_validation"] = {
                "passed": False,
                "message": f"Score muito baixo: {score:.1f} (mínimo: {self.min_score_threshold})"
            }
            validation_result["passed"] = False
        
        # 2. Validar diversidade
        teams = set(p.get("team") for p in players)
        if len(teams) < 2:
            validation_result["diversity_validation"] = {
                "passed": False,
                "message": "Baixa diversidade de times"
            }
            validation_result["recommendations"].append("Adicionar jogador de outro time")
        
        # 3. Validar volatilidade
        high_vol_count = sum(1 for p in players if p.get("volatility") == "high")
        if high_vol_count / len(players) > self.max_volatility_ratio:
            validation_result["volatility_validation"] = {
                "passed": False,
                "message": f"Muitos jogadores voláteis: {high_vol_count}/{len(players)}"
            }
            validation_result["recommendations"].append("Substituir jogadores muito voláteis")
        
        # 4. Validar minutos
        low_minute_players = [p for p in players if p.get("expected_minutes", 0) < 20]
        if len(low_minute_players) >= 2:
            validation_result["minutes_validation"] = {
                "passed": False,
                "message": f"Muitos jogadores com minutos limitados: {len(low_minute_players)}"
            }
            validation_result["recommendations"].append("Evitar múltiplos jogadores com menos de 20 minutos")
        
        # 5. Validar estratégia
        if not trixie.get("strategy"):
            validation_result["recommendations"].append("Trixie sem estratégia clara identificada")
        
        return validation_result
    
    def calculate_trixie_confidence(self, trixie):
        """Calcula score de confiança para uma trixie"""
        confidence_factors = []
        
        players = trixie.get("players", [])
        
        # Fator 1: Score da trixie
        base_score = trixie.get("score", 0)
        score_factor = min(1.0, base_score / 100.0)
        confidence_factors.append(score_factor)
        
        # Fator 2: Consistência dos jogadores
        consistency_scores = [p.get("consistency_score", 50) for p in players]
        avg_consistency = np.mean(consistency_scores) / 100.0
        confidence_factors.append(avg_consistency)
        
        # Fator 3: Minutos projetados
        minute_scores = []
        for p in players:
            exp_min = p.get("expected_minutes", 0)
            if exp_min >= 30:
                minute_scores.append(1.0)
            elif exp_min >= 25:
                minute_scores.append(0.8)
            elif exp_min >= 20:
                minute_scores.append(0.6)
            else:
                minute_scores.append(0.4)
        
        avg_minute_score = np.mean(minute_scores)
        confidence_factors.append(avg_minute_score)
        
        # Fator 4: DvP advantage
        dvp_scores = []
        for p in players:
            dvp_data = p.get("dvp_data", {})
            if dvp_data:
                overall = dvp_data.get("overall", 1.0)
                if overall > 1.05:
                    dvp_scores.append(1.0)
                elif overall > 1.0:
                    dvp_scores.append(0.8)
                else:
                    dvp_scores.append(0.6)
            else:
                dvp_scores.append(0.5)
        
        avg_dvp_score = np.mean(dvp_scores)
        confidence_factors.append(avg_dvp_score)
        
        # Fator 5: Estratégia identificada
        strategy = trixie.get("strategy")
        if strategy and strategy != "GENERIC":
            confidence_factors.append(0.9)
        else:
            confidence_factors.append(0.7)
        
        # Calcular confiança média
        if confidence_factors:
            confidence = np.mean(confidence_factors) * 100
        else:
            confidence = 50.0
        
        return round(confidence, 1)