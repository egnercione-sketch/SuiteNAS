# modules/new_modules/strategy_engine.py
import logging
import random
import uuid
import sys
import os
import hashlib
import json
import math
import unicodedata
import re
from datetime import datetime

logger = logging.getLogger("StrategyEngine_V71_1")

# =========================================================================
# IMPORTAÇÃO DE TODOS OS MÓDULOS DISPONÍVEIS (COM FALLBACKS SEGUROS)
# =========================================================================

MONTE_CARLO_AVAILABLE = False
PACE_ADJUSTER_AVAILABLE = False
VACUUM_MATRIX_AVAILABLE = False
THESIS_ENGINE_AVAILABLE = False
DVP_ANALYZER_AVAILABLE = False
ROTATION_CEILING_AVAILABLE = False
PLAYER_CLASSIFIER_AVAILABLE = False
NARRATIVE_INTELLIGENCE_AVAILABLE = False
CORRELATION_VALIDATOR_AVAILABLE = False
TREND_ANALYZER_AVAILABLE = False

try:
    from modules.new_modules.monte_carlo import MonteCarloEngine
    MONTE_CARLO_AVAILABLE = True
except ImportError as e:
    logger.warning(f"⚠️ MonteCarloEngine não disponível: {e}")

try:
    from modules.new_modules.pace_adjuster import PaceAdjuster
    PACE_ADJUSTER_AVAILABLE = True
except ImportError as e:
    logger.warning(f"⚠️ PaceAdjuster não disponível: {e}")

try:
    from modules.new_modules.vacuum_matrix import VacuumMatrixAnalyzer
    VACUUM_MATRIX_AVAILABLE = True
except ImportError as e:
    logger.warning(f"⚠️ VacuumMatrixAnalyzer não disponível: {e}")

try:
    from modules.new_modules.thesis_engine import ThesisEngine, create_thesis_engine, SimpleThesisFallback
    THESIS_ENGINE_AVAILABLE = True
except ImportError as e:
    logger.warning(f"⚠️ ThesisEngine não disponível: {e}")

try:
    from modules.new_modules.dvp_analyzer import DvPAnalyzer
    DVP_ANALYZER_AVAILABLE = True
except ImportError as e:
    logger.warning(f"⚠️ DvPAnalyzer não disponível: {e}")

try:
    from modules.new_modules.rotation_ceiling_engine import RotationCeilingEngine
    ROTATION_CEILING_AVAILABLE = True
except ImportError as e:
    logger.warning(f"⚠️ RotationCeilingEngine não disponível: {e}")

try:
    from modules.new_modules.player_classifier import PlayerClassifier
    PLAYER_CLASSIFIER_AVAILABLE = True
except ImportError as e:
    logger.warning(f"⚠️ PlayerClassifier não disponível: {e}")

try:
    from modules.new_modules.narrative_intelligence import NarrativeIntelligence
    NARRATIVE_INTELLIGENCE_AVAILABLE = True
except ImportError as e:
    logger.warning(f"⚠️ NarrativeIntelligence não disponível: {e}")

try:
    from modules.new_modules.correlation_filters import CorrelationValidator
    CORRELATION_VALIDATOR_AVAILABLE = True
except ImportError as e:
    logger.warning(f"⚠️ CorrelationValidator não disponível: {e}")

try:
    from modules.new_modules.trend_analyzer import TrendAnalyzer
    TREND_ANALYZER_AVAILABLE = True
except ImportError as e:
    logger.warning(f"⚠️ TrendAnalyzer não disponível: {e}")

# Configuração de Cache
ROTATION_DNA_FILE = os.path.join("cache", "rotation_dna.json")
INJURIES_CACHE_FILE = os.path.join("cache", "injuries_cache_v44.json")

# Import Streamlit
try:
    import streamlit as st
    ST_AVAILABLE = True
except ImportError:
    ST_AVAILABLE = False
    st = None 

class StrategyEngine:
    def __init__(self, df_l5=None, pace_data=None, dvp_data=None, props_map=None):
        self.df_l5 = df_l5
        self.pace_data = pace_data or {}
        self.dvp_data = dvp_data or {}
        self.version = "71.1_NO_ODDS_FLEX"
        self.today_seed = int(datetime.now().strftime("%Y%m%d"))
        
        # --- CARREGAMENTOS ---
        self.dna_cache = self._load_dna()
        self.injuries_banned = self._load_injuries()
               
        # --- NOVOS MÓDULOS (INICIALIZAÇÃO CONDICIONAL) ---
        if DVP_ANALYZER_AVAILABLE:
            try: self.dvp_analyzer = DvPAnalyzer()
            except: self.dvp_analyzer = None
        else: self.dvp_analyzer = None
        
        if ROTATION_CEILING_AVAILABLE:
            try: self.ceiling_engine = RotationCeilingEngine()
            except: self.ceiling_engine = None
        else: self.ceiling_engine = None
        
        if PLAYER_CLASSIFIER_AVAILABLE:
            try: self.player_classifier = PlayerClassifier()
            except: self.player_classifier = None
        else: self.player_classifier = None
        
        if MONTE_CARLO_AVAILABLE:
            try: self.monte_carlo = MonteCarloEngine()
            except: self.monte_carlo = None
        else: self.monte_carlo = None
        
        if PACE_ADJUSTER_AVAILABLE:
            try: self.pace_adjuster = PaceAdjuster()
            except: self.pace_adjuster = None
        else: self.pace_adjuster = None
        
        if VACUUM_MATRIX_AVAILABLE:
            try: self.vacuum_matrix = VacuumMatrixAnalyzer()
            except: self.vacuum_matrix = None
        else: self.vacuum_matrix = None
        
        # FORÇAR inicialização do Thesis Engine (com fallback)
        self._initialize_thesis_engine()
        
        if NARRATIVE_INTELLIGENCE_AVAILABLE:
            try: self.narrative_intel = NarrativeIntelligence()
            except: self.narrative_intel = None
        else: self.narrative_intel = None
        
        if CORRELATION_VALIDATOR_AVAILABLE:
            try: self.correlation_validator = CorrelationValidator()
            except: self.correlation_validator = None
        else: self.correlation_validator = None
        
        if TREND_ANALYZER_AVAILABLE:
            try: self.trend_analyzer = TrendAnalyzer()
            except: self.trend_analyzer = None
        else: self.trend_analyzer = None
        
        # Props map (não mais usado para odds)
        self.props_map = props_map or {}
        if not self.props_map and ST_AVAILABLE and 'pinnacle_props_map' in st.session_state:
            self.props_map = st.session_state.get('pinnacle_props_map', {})

        logger.info(f"StrategyEngine v{self.version} inicializado com sucesso")

    def _load_dna(self):
        if os.path.exists(ROTATION_DNA_FILE):
            try:
                with open(ROTATION_DNA_FILE, 'r') as f:
                    return json.load(f)
            except:
                return {}
        return {}

    def _load_injuries(self):
        if os.path.exists(INJURIES_CACHE_FILE):
            try:
                with open(INJURIES_CACHE_FILE, 'r') as f:
                    data = json.load(f)
                    return set(data.get('banned', []))
            except:
                return set()
        return set()

    def _initialize_thesis_engine(self):
        """Inicializa thesis_engine com fallback seguro"""
        if THESIS_ENGINE_AVAILABLE:
            try:
                self.thesis_engine = create_thesis_engine()
                logger.info("ThesisEngine v5.1 carregado")
                return
            except Exception as e:
                logger.warning(f"ThesisEngine falhou ({e}), usando fallback simples")
        
        self.thesis_engine = SimpleThesisFallback()
        logger.info("Usando SimpleThesisFallback")

    def _normalize_name(self, name: str) -> str:
        if not name:
            return ""
        name = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode('ascii')
        name = re.sub(r'[^a-zA-Z\s]', '', name).strip().lower()
        return name

    def _is_player_injured(self, player_name: str) -> bool:
        if not player_name:
            return False
        norm = self._normalize_name(player_name)
        return norm in self.injuries_banned

    # =========================================================================
    # MÉTODOS DE CRIAÇÃO DE LEG E PACK (SEM ODDS)
    # =========================================================================

    def _create_leg(self, player: Dict, market: str, risk_profile: str = "balanceada", thesis_override: str = None) -> Optional[Dict]:
        """Cria uma leg sem odds"""
        try:
            name = player.get('name') or player.get('player_name') or "Unknown"
            team = player.get('team', '')
            
            stat_map = {
                'PTS': 'pts_L5', 'REB': 'reb_L5', 'AST': 'ast_L5',
                '3PM': '3pm_L5', 'STL': 'stl_L5', 'BLK': 'blk_L5'
            }
            stat_key = stat_map.get(market, 'pts_L5')
            avg = float(player.get(stat_key, 0) or 0)
            
            if avg < 1.0:
                return None
                
            # Multiplicador por perfil
            mult_map = {"conservadora": 0.85, "balanceada": 0.95, "ousada": 1.10}
            mult = mult_map.get(risk_profile, 0.95)
            line = max(1, int(avg * mult))
            
            thesis = thesis_override or "Análise Técnica"
            if self.thesis_engine and not thesis_override:
                try:
                    ctx = {"pace_factor": player.get('pace_factor', 1.0)}
                    theses = self.thesis_engine.generate_theses(player, ctx)
                    if theses:
                        best = max(theses, key=lambda t: t['win_rate'])
                        thesis = self.thesis_engine.format_thesis_for_display(best)
                except Exception as e:
                    logger.debug(f"Falha ao enriquecer tese: {e}")
            
            return {
                "player_name": name,
                "team": team,
                "market_type": market,
                "market_display": f"{line}+ {market}",
                "thesis": thesis,
                "player_data": player
            }
        except Exception as e:
            logger.warning(f"Erro ao criar leg: {e}")
            return None

    def _pack_trixie(self, legs: List[Dict], category: str, sub_category: str, ctx: Dict) -> Optional[Dict]:
        """Empacota trixie sem odds"""
        if len(legs) < 3:
            return None
        
        # Score simples baseado na quantidade de legs e random
        base_score = 70 + len(legs) * 5
        score = min(98, base_score + random.randint(0, 15))
        
        away = ctx.get('away', '?')
        home = ctx.get('home', '?')
        
        return {
            "id": uuid.uuid4().hex[:8],
            "category": category.upper(),
            "sub_category": sub_category,
            "game": f"{away} @ {home}",
            "game_info": {"away": away, "home": home},
            "legs": legs,
            "score": score
        }

    # =========================================================================
    # BUILDERS DE TRIXIES ESPECÍFICAS (TODAS FUNCIONANDO SEM ODDS)
    # =========================================================================

    def _build_safe_trixie(self, players: List[Dict], ctx: Dict) -> Optional[Dict]:
        """Safe Conservadora - alta minutagem e consistência"""
        candidates = [p for p in players if p.get('min_L5', 0) >= 26.0]
        if len(candidates) < 3:
            return None
        
        candidates.sort(key=lambda x: x.get('min_L5', 0), reverse=True)
        legs = []
        
        for p in candidates[:8]:
            if len(legs) >= 3:
                break
            if p.get('pts_L5', 0) >= 11:
                market = "PTS"
            elif p.get('reb_L5', 0) >= 7:
                market = "REB"
            elif p.get('ast_L5', 0) >= 5:
                market = "AST"
            else:
                continue
                
            leg = self._create_leg(p, market, "conservadora")
            if leg:
                legs.append(leg)
        
        return self._pack_trixie(legs, "SAFE", "CONSERVADORA", ctx)

    def _build_bold_trixie(self, players: List[Dict], ctx: Dict) -> Optional[Dict]:
        """Ousada - alto upside"""
        candidates = [p for p in players if p.get('pts_L5', 0) >= 14 or p.get('ast_L5', 0) >= 7 or p.get('reb_L5', 0) >= 8]
        if len(candidates) < 3:
            return None
        
        legs = []
        for p in candidates[:8]:
            if len(legs) >= 3:
                break
            market = max(['PTS', 'AST', 'REB'], key=lambda m: p.get(f'{m.lower()}_L5', 0))
            leg = self._create_leg(p, market, "ousada")
            if leg:
                legs.append(leg)
        
        return self._pack_trixie(legs, "BOLD", "OUSADA", ctx)

    def _build_explosion_trixie(self, players: List[Dict], ctx: Dict) -> Optional[Dict]:
        """Explosão - alto ceiling em pontos"""
        candidates = [p for p in players if p.get('pts_L5', 0) >= 16.0]
        if len(candidates) < 3:
            return None
        
        candidates.sort(key=lambda x: x.get('pts_L5', 0), reverse=True)
        legs = []
        for p in candidates[:6]:
            if len(legs) >= 3:
                break
            leg = self._create_leg(p, "PTS", "ousada", "High Ceiling Explosion")
            if leg:
                legs.append(leg)
        
        return self._pack_trixie(legs, "EXPLOSAO", "CEILING", ctx)

    def _build_bench_trixie(self, players: List[Dict], ctx: Dict) -> Optional[Dict]:
        """Bench - jogadores de banco com boa produção"""
        candidates = [p for p in players if 18 <= p.get('min_L5', 0) <= 27 and p.get('pts_L5', 0) >= 9]
        if len(candidates) < 3:
            return None
        
        candidates.sort(key=lambda x: x.get('pts_L5', 0) / max(1, x.get('min_L5', 1)), reverse=True)
        legs = []
        for p in candidates[:7]:
            if len(legs) >= 3:
                break
            market = "PTS" if p.get('pts_L5', 0) >= p.get('reb_L5', 0) else "REB"
            leg = self._create_leg(p, market, "balanceada", "Bench Impact")
            if leg:
                legs.append(leg)
        
        return self._pack_trixie(legs, "BENCH", "ROTAÇÃO", ctx)

    def _build_versatile_trixie(self, players: List[Dict], ctx: Dict) -> Optional[Dict]:
        """Versatile - foco em STL, BLK, 3PM"""
        candidates = [p for p in players 
                     if p.get('3pm_L5', 0) >= 1.8 or 
                        p.get('stl_L5', 0) >= 1.2 or 
                        p.get('blk_L5', 0) >= 1.0]
        if len(candidates) < 3:
            return None
        
        legs = []
        for p in candidates[:8]:
            if len(legs) >= 3:
                break
            if p.get('3pm_L5', 0) >= 2.0:
                market = "3PM"
            elif p.get('stl_L5', 0) >= 1.3:
                market = "STL"
            elif p.get('blk_L5', 0) >= 1.1:
                market = "BLK"
            else:
                continue
            leg = self._create_leg(p, market, "balanceada", "Versatile Contributor")
            if leg:
                legs.append(leg)
        
        return self._pack_trixie(legs, "VERSATILE", "DEF/3PT", ctx)

    def _build_vulture_trixie(self, players: List[Dict], ctx: Dict) -> Optional[Dict]:
        """Vulture - steals + blocks"""
        candidates = [p for p in players if p.get('stl_L5', 0) + p.get('blk_L5', 0) >= 2.2]
        if len(candidates) < 3:
            return None
        
        candidates.sort(key=lambda x: x.get('stl_L5', 0) + x.get('blk_L5', 0), reverse=True)
        legs = []
        for p in candidates[:6]:
            if len(legs) >= 3:
                break
            market = "STL" if p.get('stl_L5', 0) >= p.get('blk_L5', 0) else "BLK"
            leg = self._create_leg(p, market, "balanceada", "Defensive Vulture")
            if leg:
                legs.append(leg)
        
        return self._pack_trixie(legs, "VULTURE", "DEFENSE", ctx)

    # =========================================================================
    # MÉTODOS AUXILIARES (mantidos)
    # =========================================================================

    def process_team_context(self, team_abbr, roster):
        if not self.vacuum_matrix or not roster: return {}
        try: return self.vacuum_matrix.analyze_team_vacuum(roster, team_abbr)
        except: return {}
    
    def enhance_player_data(self, player_data, team_abbr):
        return player_data.copy()
    
    def get_narrative_badge(self, player_data, opponent_team):
        if not self.narrative_intel: return None
        try:
            player_id = player_data.get('player_id')
            player_name = player_data.get('name')
            if not player_id or not player_name: return None
            return self.narrative_intel.get_player_matchup_history(player_id, player_name, opponent_team)
        except: return None
    
    def validate_correlation(self, legs):
        if not self.correlation_validator: return {'is_valid': True, 'violations': []}
        try:
            players_data = []
            for leg in legs:
                players_data.append({
                    'name': leg.get('player_name'),
                    'team': leg.get('team'),
                    'position': '',
                    'market': leg.get('market_type'),
                    'thesis': leg.get('thesis', '')
                })
            return self.correlation_validator.validate_trixie(players_data)
        except: return {'is_valid': True, 'violations': []}

    def get_specific_leg(self, player, market, profile="balanceada", thesis_override=None):
        """Método compatível com multipla_do_dia"""
        return self._create_leg(player, market, profile, thesis_override)

    def get_available_modules_report(self):
        return {
            'version': self.version,
            'modules': {
                'dvp_analyzer': bool(self.dvp_analyzer),
                'ceiling_engine': bool(self.ceiling_engine),
                'monte_carlo': bool(self.monte_carlo),
                'pace_adjuster': bool(self.pace_adjuster),
                'vacuum_matrix': bool(self.vacuum_matrix),
                'thesis_engine': bool(self.thesis_engine),
                'player_classifier': bool(self.player_classifier),
                'narrative_intelligence': bool(self.narrative_intel),
                'correlation_validator': bool(self.correlation_validator),
                'trend_analyzer': bool(self.trend_analyzer),
            },
            'injuries_loaded': len(self.injuries_banned),
            'dna_loaded': len(self.dna_cache)
        }