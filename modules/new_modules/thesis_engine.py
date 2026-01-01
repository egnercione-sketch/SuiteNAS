# modules/new_modules/thesis_engine.py
# VERSÃO 5.1 - HYBRID OPTIMIZED CORRIGIDO
# Atualizações principais:
# - Removido dia 29/12 da lista de feriados (não é feriado oficial e estava ativando holiday_adjust desnecessariamente em 29 de dezembro)
# - Thresholds mais flexíveis para gerar mais teses em dias normais
# - Aumentado max_theses_per_player para 5
# - Pequenos ajustes de confiança e multiplicadores para maior geração de resultados
# - Mantido foco em win rates comprovados (HighCeiling, MinutesSafe, PlaymakerEdge)

import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime

logger = logging.getLogger("ThesisEngine_V5_1")

class ThesisEngine:
    """
    Engine de Teses Híbrido Otimizado - Foco em win rates comprovados do CSV, com integração de teses nichadas do antigo evaluator.
    Parametrização completa para thresholds e multiplicadores. Ajuste automático para feriados/sazonalidade.
    """
    
    def __init__(self, config: Optional[Dict] = None):
        """Inicializa com configurações parametrizadas"""
        self.config = config or {}
        
        # WIN RATES BASEADOS NO CSV + ESTIMADOS PARA NOVAS (de trends NBA históricas)
        self.WIN_RATES = self.config.get('win_rates', {
            'HighCeiling': 0.75,        # CSV: 3/4
            'MinutesSafe': 0.667,       # CSV: 2/3
            'PlaymakerEdge': 0.50,      # CSV: 1/2
            'ScorerLine': 0.333,        # CSV: 3/9
            'BigRebound': 0.20,         # CSV: 1/5
            'AssistMatchup': 0.111,     # CSV: 1/9
            # Resgatadas do Antigo com Estimativas (baseado em ~50-60% win médio em props matchup/consistência)
            'LowVariance': 0.60,        # Estimado: Baixa volatilidade geralmente ~60% win
            'ReboundMatchup': 0.55,     # Estimado: Matchups fracos ~55% win em rebounds
            'RookieBlindado': 0.50,     # Estimado: Rookies high-min ~50% win
        })
        
        # Multiplicadores baseados no win rate (parametrizável)
        self.confidence_multipliers = self.config.get('multipliers', {
            'HighCeiling': 1.45,       # Leve aumento para compensar thresholds mais baixos
            'MinutesSafe': 1.35,
            'PlaymakerEdge': 1.15,
            'ScorerLine': 1.05,
            'AssistMatchup': 0.75,
            'BigRebound': 0.85,
            'LowVariance': 1.10,       
            'ReboundMatchup': 1.10,    
            'RookieBlindado': 1.00,    
            'VacuumOpportunity': 1.0,
            'Sniper3PM': 1.0,
            'DefensiveAnchor': 1.0,
        })
        
        # Pesos ajustados (parametrizável) - Ênfase em Role/Consistência
        self.weights = self.config.get('weights', {
            'DvP': 0.20,
            'Pace': 0.15,
            'Role': 0.25,     
            'PlayerClass': 0.10,
            'Usage': 0.15,
            'MonteCarlo': 0.15  
        })
        
        # THRESHOLDS MAIS FLEXÍVEIS (ajustados para gerar mais resultados sem perder qualidade)
        self.thresholds = self.config.get('thresholds', {
            # HighCeiling - mais acessível
            'high_ceiling_pts': 13.0,   # era 15.0
            'high_ceiling_min': 28.0,   # era 30.0
            'high_ceiling_cv': 0.30,    # um pouco mais tolerante à variância
            
            # MinutesSafe - mais inclusivo
            'minutes_safe_min': 26.0,   # era 28.0
            'minutes_safe_cv': 0.20,    # tolera um pouco mais de variação
            
            # PlaymakerEdge
            'playmaker_edge_ast': 6.0,  # era 7.0
            'playmaker_edge_min': 25.0, # era 26.0
            
            # ScorerLine
            'scorer_line_pts': 11.0,    # era 12.0
            'scorer_line_min': 23.0,    # era 24.0
            
            # AssistMatchup / BigRebound
            'assist_matchup_ast': 5.5,
            'big_rebound_reb': 7.5,
            
            # Novos/Resgatados
            'low_variance_max_cv': 0.35,  
            'rebound_matchup_opp_rank': 23,  
            'rookie_blindado_exp': 2,     
            'rookie_blindado_min': 26,
            
            # Contextuais
            'max_spread': 12.0,           # menos penalidade em jogos desequilibrados
            'min_pace': 0.93,
            'holiday_adjust_factor': 0.85,  # menos agressivo (caso ainda ative)
        })
        
        # Mapeamento de fallback SEGURO
        self.safe_fallback_map = self.config.get('fallback_map', {
            "ScorerLine": "HighCeiling",
            "AssistMatchup": "PlaymakerEdge", 
            "BigRebound": "MinutesSafe",
            "GlassCleaner": "MinutesSafe",
            "FloorGeneral": "PlaymakerEdge",
            "VolumeShooter": "HighCeiling",
            "ReboundMatchup": "BigRebound",
            "LowVariance": "MinutesSafe",
        })
        
        # CORREÇÃO PRINCIPAL: Removido 29/12 da lista de feriados
        # Apenas Natal (25) e Boxing Day (26) - dias tradicionalmente mais conservadores na NBA
        self.holiday_dates = self.config.get('holiday_dates', [(12, 25), (12, 26)])
        
        logger.info(f"ThesisEngine v5.1 inicializado. Prioridade: {self.get_thesis_priority_list()}")

    # =========================================================================
    # HELPERS
    # =========================================================================
    
    def is_holiday(self, date=None):
        """Verifica se é feriado/sazonal (apenas Natal e Boxing Day)"""
        date = date or datetime.now().date()
        month_day = (date.month, date.day)
        return month_day in self.holiday_dates
    
    def apply_holiday_adjust(self, conf_factors):
        """Ajusta confiança para feriados (reduz volatilidade)"""
        if self.is_holiday():
            holiday_factor = self.thresholds['holiday_adjust_factor']
            return [('HolidayAdjust', holiday_factor)] + conf_factors
        return conf_factors
    
    def calculate_dvp_factor(self, dvp_value: float, is_favorable: bool = True) -> float:
        """Calcula fator DvP (mantido do antigo)"""
        if pd.isna(dvp_value) or dvp_value == 0: return 1.0
        if is_favorable:
            return 1.0 + (dvp_value - 1.0) * 0.4 if dvp_value > 1.0 else max(0.6, 1.0 - (1.0 - dvp_value) * 0.5)
        return 1.0

    def _get_mc_factor(self, context_data: Dict) -> Tuple[float, str]:
        """Extrai fator Monte Carlo (mantido)"""
        mc_prob = context_data.get('mc_prob', 0)
        if mc_prob <= 0: return 1.0, ""
        if mc_prob >= 65: return 1.25, f"MC Alta ({mc_prob:.1f}%)"
        if mc_prob >= 55: return 1.15, f"MC Boa ({mc_prob:.1f}%)"
        return 1.0, ""

    # =========================================================================
    # GERADORES DE TESES (mantidos com thresholds atualizados)
    # =========================================================================

    # Exemplo de um gerador (os outros seguem o mesmo padrão - não alterados aqui por brevidade)
    def generate_high_ceiling_thesis(self, player_ctx: Dict, context_data: Dict) -> Optional[Dict]:
        pts = player_ctx.get('pts_L5', 0)
        minutes = player_ctx.get('min_L5', 0)
        cv = player_ctx.get('pts_cv', 0.3)
        
        if (pts >= self.thresholds['high_ceiling_pts'] and 
            minutes >= self.thresholds['high_ceiling_min'] and 
            cv <= self.thresholds['high_ceiling_cv']):
            
            factors = [('Role', 1.3), ('Usage', 1.2)]
            evidences = [f"{pts:.1f} pts L5", f"{minutes:.1f} min garantidos"]
            return self._build_thesis(player_ctx, "HighCeiling", "PTS", factors, evidences, pts)
        return None

    # ... (demais geradores permanecem iguais ao original, apenas usando os novos thresholds)

    def _build_thesis(self, p_ctx, t_type, market, factors, evidences, line_val):
        """Constrói tese com confiança ponderada"""
        if not factors: return None
        
        base_score = 0.50
        total_weight = 0
        
        for f_type, f_val in factors:
            weight = self.weights.get(f_type, 0.1)
            delta = (f_val - 1.0)
            base_score += delta * (weight * 5)
            total_weight += weight
        
        final_conf = base_score * self.confidence_multipliers.get(t_type, 1.0)
        final_conf = min(0.98, max(0.40, final_conf))
        
        category = "ousada"
        if final_conf > 0.75: category = "conservadora"
        elif final_conf >= 0.60: category = "balanceada"
        elif final_conf < 0.55: category = "explosao"
        elif final_conf < 0.45: category = "perigosa"
        
        return {
            'type': t_type,
            'market': market,
            'confidence': round(final_conf, 2),
            'category': category,
            'reason': f"{t_type}: {', '.join(evidences[:3])}.",
            'evidences': evidences,
            'line': line_val,
            'player_name': p_ctx.get('name'),
            'win_rate': self.WIN_RATES.get(t_type, 0.0)
        }

    def generate_theses(self, player_ctx: Dict, context_data: Dict, target_market: Optional[str] = None) -> List[Dict]:
        """Gera teses priorizadas por win rate"""
        theses = []
        
        generators = [
            (self.generate_high_ceiling_thesis, "HighCeiling"),    
            (self.generate_minutes_safe_thesis, "MinutesSafe"),     
            (self.generate_low_variance_thesis, "LowVariance"),     
            (self.generate_rebound_matchup_thesis, "ReboundMatchup"), 
            (self.generate_playmaker_edge_thesis, "PlaymakerEdge"), 
            (self.generate_rookie_blindado_thesis, "RookieBlindado"), 
            (self.generate_scorer_line_thesis, "ScorerLine"),       
            (self.generate_big_rebound_thesis, "BigRebound"),       
            (self.generate_assist_matchup_thesis, "AssistMatchup"), 
            (self.generate_vacuum_thesis, "VacuumOpportunity"),     
        ]
        
        for gen_func, gen_name in generators:
            try:
                t = gen_func(player_ctx, context_data)
                if t and t['win_rate'] >= self.config.get('min_win_rate', 0.15):  # ligeiramente mais permissivo
                    if t['category'] != "perigosa":
                        if not target_market or t['market'] == target_market:
                            theses.append(t)
                if len(theses) >= self.config.get('max_theses_per_player', 5):
                    break
            except Exception as e:
                logger.debug(f"Erro em {gen_name}: {e}")
        
        # Fallback se vazio e target_market
        if target_market and not theses:
            return self.generate_theses(player_ctx, context_data)  
        
        theses.sort(key=lambda x: (x['win_rate'], x['confidence']), reverse=True)
        return theses
    
    # =========================================================================
    # COMPATIBILIDADE E UTILS (Mantidos)
    # =========================================================================
    
    def get_thesis_for_category(self, theses_list, target_category):
        matches = [t for t in theses_list if t['category'] == target_category]
        return max(matches, key=lambda x: x['win_rate']) if matches else max(theses_list, key=lambda x: x['win_rate']) if theses_list else None
    
    def format_thesis_for_display(self, thesis):
        if not thesis: return "Análise Técnica (Padrão)"
        return f"{thesis['reason']} | Conf: {thesis['confidence']:.0%} | WR: {thesis['win_rate']:.0%}"
    
    def get_safe_fallback(self, original_tag):
        return self.safe_fallback_map.get(original_tag, "MinutesSafe")
    
    def get_thesis_priority_list(self):
        return sorted([(name, wr) for name, wr in self.WIN_RATES.items() if wr >= 0.2], key=lambda x: x[1], reverse=True)
    
    def enhance_thesis(self, player_data, market, original_thesis):
        return original_thesis

# =========================================================================
# CLASSE DE FALLBACK SIMPLIFICADA (Mantida)
# =========================================================================

class SimpleThesisFallback:
    """Fallback mínimo para falhas"""
    
    WIN_RATES = {  
        'HighCeiling': 0.75,
        'MinutesSafe': 0.667,
        'PlaymakerEdge': 0.50,
    }
    
    def generate_theses(self, player_ctx, context_data):
        theses = []
        pts = player_ctx.get('pts_L5', 0)
        min_val = player_ctx.get('min_L5', 0)
        ast = player_ctx.get('ast_L5', 0)
        reb = player_ctx.get('reb_L5', 0)
        
        if pts >= 13 and min_val >= 28:
            theses.append({'type': 'HighCeiling', 'market': 'PTS', 'reason': f'HighCeiling: {pts:.1f} pts', 'win_rate': 0.75, 'confidence': 0.75, 'category': 'conservadora'})
        
        if min_val >= 26:
            market = 'PTS' if pts > 10 else 'REB' if reb > 7 else 'AST'
            theses.append({'type': 'MinutesSafe', 'market': market, 'reason': f'MinutesSafe: {min_val:.1f} min', 'win_rate': 0.667, 'confidence': 0.70, 'category': 'conservadora'})
        
        if ast >= 6 and min_val >= 25:
            theses.append({'type': 'PlaymakerEdge', 'market': 'AST', 'reason': f'PlaymakerEdge: {ast:.1f} ast', 'win_rate': 0.50, 'confidence': 0.65, 'category': 'balanceada'})
        
        return theses
    
    def get_safe_fallback(self, original_tag):
        safe_map = {
            "ScorerLine": "HighCeiling",
            "AssistMatchup": "PlaymakerEdge", 
            "BigRebound": "MinutesSafe",
        }
        return safe_map.get(original_tag, "MinutesSafe")
    
    def enhance_thesis(self, player_data, market, original_thesis):
        return original_thesis

# =========================================================================
# FACTORY PARA CRIAÇÃO
# =========================================================================

def create_thesis_engine(custom_config: Optional[Dict] = None):
    """Cria engine com config padrão ou custom"""
    default_config = {
        'use_win_rates': True,
        'min_win_rate': 0.15,           # Mais permissivo
        'max_theses_per_player': 5,     # Aumentado de 4 para 5
        'holiday_dates': [(12, 25), (12, 26)],  # Apenas feriados reais
    }
    config = {**default_config, **(custom_config or {})}
    
    engine = ThesisEngine(config)
    logger.info(f"ThesisEngine v5.1 criado com sucesso. Prioridade: {engine.get_thesis_priority_list()}")
    return engine