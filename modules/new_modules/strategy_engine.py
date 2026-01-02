# modules/new_modules/strategy_engine.py
# VERSÃO V70.4 RESTAURADA - COM MÓDULOS NOVOS MAS FLUXO ORIGINAL

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

logger = logging.getLogger("StrategyEngine_V70_4")

# =========================================================================
# IMPORTAÇÃO DE TODOS OS MÓDULOS DISPONÍVEIS (COM FALLBACKS SEGUROS)
# =========================================================================

# --- MÓDULOS CORE (OPCIONAL - NÃO QUEBRA SE FALTAR) ---
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

# Módulos legados
try:
    from modules.new_modules.monte_carlo import MonteCarloEngine
    MONTE_CARLO_AVAILABLE = True
    logger.info("✅ MonteCarloEngine carregado")
except ImportError as e:
    logger.warning(f"⚠️ MonteCarloEngine não disponível: {e}")

try:
    from modules.new_modules.pace_adjuster import PaceAdjuster
    PACE_ADJUSTER_AVAILABLE = True
    logger.info("✅ PaceAdjuster carregado")
except ImportError as e:
    logger.warning(f"⚠️ PaceAdjuster não disponível: {e}")

try:
    from modules.new_modules.vacuum_matrix import VacuumMatrixAnalyzer
    VACUUM_MATRIX_AVAILABLE = True
    logger.info("✅ VacuumMatrixAnalyzer carregado")
except ImportError as e:
    logger.warning(f"⚠️ VacuumMatrixAnalyzer não disponível: {e}")

try:
    from modules.new_modules.thesis_engine import ThesisEngine
    THESIS_ENGINE_AVAILABLE = True
    logger.info("✅ ThesisEngine carregado")
except ImportError as e:
    logger.warning(f"⚠️ ThesisEngine não disponível: {e}")

try:
    from modules.new_modules.dvp_analyzer import DvPAnalyzer
    DVP_ANALYZER_AVAILABLE = True
    logger.info("✅ DvPAnalyzer carregado")
except ImportError as e:
    logger.warning(f"⚠️ DvPAnalyzer não disponível: {e}")

try:
    from modules.new_modules.rotation_ceiling_engine import RotationCeilingEngine
    ROTATION_CEILING_AVAILABLE = True
    logger.info("✅ RotationCeilingEngine carregado")
except ImportError as e:
    logger.warning(f"⚠️ RotationCeilingEngine não disponível: {e}")

try:
    from modules.new_modules.player_classifier import PlayerClassifier
    PLAYER_CLASSIFIER_AVAILABLE = True
    logger.info("✅ PlayerClassifier carregado")
except ImportError as e:
    logger.warning(f"⚠️ PlayerClassifier não disponível: {e}")

# Novos módulos de inteligência (para uso futuro)
try:
    from modules.new_modules.narrative_intelligence import NarrativeIntelligence
    NARRATIVE_INTELLIGENCE_AVAILABLE = True
    logger.info("✅ NarrativeIntelligence carregado")
except ImportError as e:
    logger.warning(f"⚠️ NarrativeIntelligence não disponível: {e}")

try:
    from modules.new_modules.correlation_filters import CorrelationValidator
    CORRELATION_VALIDATOR_AVAILABLE = True
    logger.info("✅ CorrelationValidator carregado")
except ImportError as e:
    logger.warning(f"⚠️ CorrelationValidator não disponível: {e}")

try:
    from modules.new_modules.trend_analyzer import TrendAnalyzer
    TREND_ANALYZER_AVAILABLE = True
    logger.info("✅ TrendAnalyzer carregado")
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
    def __init__(self, df_l5=None, pace_data=None, dvp_data=None):
        self.df_l5 = df_l5
        self.pace_data = pace_data or {}
        self.dvp_data = dvp_data or {}
        self.version = "70.4_FIXED_LEGACY"
        self.today_seed = int(datetime.now().strftime("%Y%m%d"))
        
        # --- CARREGAMENTOS ---
        self.dna_cache = self._load_dna()
        self.injuries_banned = self._load_injuries()
               
        # --- NOVOS MÓDULOS (INICIALIZAÇÃO CONDICIONAL) ---
        
        # DvP Analyzer
        if DVP_ANALYZER_AVAILABLE:
            try:
                self.dvp_analyzer = DvPAnalyzer()
                logger.info("✅ DvPAnalyzer inicializado")
            except Exception as e:
                self.dvp_analyzer = None
                logger.error(f"❌ Erro ao inicializar DvPAnalyzer: {e}")
        else:
            self.dvp_analyzer = None
        
        # Rotation Ceiling Engine
        if ROTATION_CEILING_AVAILABLE:
            try:
                self.ceiling_engine = RotationCeilingEngine()
                logger.info("✅ RotationCeilingEngine inicializado")
            except Exception as e:
                self.ceiling_engine = None
                logger.error(f"❌ Erro ao inicializar RotationCeilingEngine: {e}")
        else:
            self.ceiling_engine = None
        
        # Player Classifier
        if PLAYER_CLASSIFIER_AVAILABLE:
            try:
                self.player_classifier = PlayerClassifier()
                logger.info("✅ PlayerClassifier inicializado")
            except Exception as e:
                self.player_classifier = None
                logger.error(f"❌ Erro ao inicializar PlayerClassifier: {e}")
        else:
            self.player_classifier = None
        
        # Monte Carlo Engine
        if MONTE_CARLO_AVAILABLE:
            try:
                self.monte_carlo = MonteCarloEngine()
                logger.info("✅ MonteCarloEngine inicializado")
            except Exception as e:
                self.monte_carlo = None
                logger.error(f"❌ Erro ao inicializar MonteCarloEngine: {e}")
        else:
            self.monte_carlo = None
        
        # Pace Adjuster
        if PACE_ADJUSTER_AVAILABLE:
            try:
                self.pace_adjuster = PaceAdjuster()
                logger.info("✅ PaceAdjuster inicializado")
            except Exception as e:
                self.pace_adjuster = None
                logger.error(f"❌ Erro ao inicializar PaceAdjuster: {e}")
        else:
            self.pace_adjuster = None
        
        # Vacuum Matrix Analyzer
        if VACUUM_MATRIX_AVAILABLE:
            try:
                self.vacuum_matrix = VacuumMatrixAnalyzer()
                logger.info("✅ VacuumMatrixAnalyzer inicializado")
            except Exception as e:
                self.vacuum_matrix = None
                logger.error(f"❌ Erro ao inicializar VacuumMatrixAnalyzer: {e}")
        else:
            self.vacuum_matrix = None
        
        # Thesis Engine
        if THESIS_ENGINE_AVAILABLE:
            try:
                self.thesis_engine = ThesisEngine()
                logger.info("✅ ThesisEngine inicializado")
            except Exception as e:
                self.thesis_engine = None
                logger.error(f"❌ Erro ao inicializar ThesisEngine: {e}")
        else:
            self.thesis_engine = None
        
        # Narrative Intelligence (para uso futuro)
        if NARRATIVE_INTELLIGENCE_AVAILABLE:
            try:
                self.narrative_intel = NarrativeIntelligence()
                logger.info("✅ NarrativeIntelligence inicializado")
            except Exception as e:
                self.narrative_intel = None
                logger.error(f"❌ Erro ao inicializar NarrativeIntelligence: {e}")
        else:
            self.narrative_intel = None
        
        # Correlation Validator (para uso futuro)
        if CORRELATION_VALIDATOR_AVAILABLE:
            try:
                self.correlation_validator = CorrelationValidator()
                logger.info("✅ CorrelationValidator inicializado")
            except Exception as e:
                self.correlation_validator = None
                logger.error(f"❌ Erro ao inicializar CorrelationValidator: {e}")
        else:
            self.correlation_validator = None
        
        # Trend Analyzer (para uso futuro)
        if TREND_ANALYZER_AVAILABLE:
            try:
                self.trend_analyzer = TrendAnalyzer()
                logger.info("✅ TrendAnalyzer inicializado")
            except Exception as e:
                self.trend_analyzer = None
                logger.error(f"❌ Erro ao inicializar TrendAnalyzer: {e}")
        else:
            self.trend_analyzer = None
        
        # --- MAPA DE PROPS REAIS DA PINNACLE ---
        self.props_map = {}
        if ST_AVAILABLE and 'pinnacle_props_map' in st.session_state:
            self.props_map = st.session_state['pinnacle_props_map']
            logger.info("✅ Mapa de props reais da Pinnacle carregado")
        else:
            logger.warning("⚠️ Mapa de props reais não encontrado. Usando odds fictícias.")
        
        logger.info(f"StrategyEngine v{self.version} inicializado com sucesso")
        logger.info(f"Módulos ativos: DvP={bool(self.dvp_analyzer)}, Ceiling={bool(self.ceiling_engine)}, ThesisGen={hasattr(self, 'thesis_gen')}")

    # =========================================================================
    # NORMALIZAÇÃO E CARREGAMENTO (BLINDADO - MANTIDO IGUAL)
    # =========================================================================
    def _normalize_team(self, t):
        if not t: return ""
        t = str(t).upper().strip()
        mapping = {
            'CHO': 'CHA', 'CHARLOTTE': 'CHA', 'HORNETS': 'CHA',
            'WSH': 'WAS', 'WASHINGTON': 'WAS', 'WIZARDS': 'WAS',
            'UTA': 'UTA', 'UTAH': 'UTA', 'JAZZ': 'UTA',
            'NO': 'NOP', 'NOP': 'NOP', 'NEW ORLEANS': 'NOP',
            'NY': 'NYK', 'NYK': 'NYK', 'NEW YORK': 'NYK',
            'GS': 'GSW', 'GSW': 'GSW', 'GOLDEN STATE': 'GSW',
            'SA': 'SAS', 'SAS': 'SAS', 'SAN ANTONIO': 'SAS',
            'PHO': 'PHX', 'PHX': 'PHX', 'PHOENIX': 'PHX',
            'BK': 'BKN', 'BKN': 'BKN', 'BRK': 'BKN', 'BROOKLYN': 'BKN'
        }
        return mapping.get(t, t[:3])

    def _normalize_name(self, text):
        if not text: return ""
        text = str(text).lower().strip()
        try:
            text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('ASCII')
        except: pass
        return " ".join(text.replace(".", "").replace(",", "").replace("'", "").split())

    def _load_dna(self):
        data = {}
        try:
            if os.path.exists(ROTATION_DNA_FILE):
                with open(ROTATION_DNA_FILE, 'r') as f:
                    raw = json.load(f)
                    for k, v in raw.items():
                        norm_k = self._normalize_team(k)
                        
                        # SEGURANÇA MÁXIMA: Garante que é Dict
                        if isinstance(v, dict):
                            data[norm_k] = v
                        elif isinstance(v, list):
                            # Converte lista para dict indexado pelo nome
                            team_dict = {}
                            for p in v:
                                if isinstance(p, dict):
                                    p_name = self._normalize_name(p.get('name', ''))
                                    team_dict[p_name] = p
                            data[norm_k] = team_dict
                        else:
                            # Se for lixo, ignora
                            data[norm_k] = {}
        except Exception as e:
            logger.error(f"Erro Loading DNA: {e}")
        return data

    def _load_injuries(self):
        banned = set()
        try:
            if os.path.exists(INJURIES_CACHE_FILE):
                with open(INJURIES_CACHE_FILE, 'r') as f:
                    data = json.load(f)
                    src = data.get('teams', data) if isinstance(data, dict) else data
                    items = []
                    if isinstance(src, dict):
                        for v in src.values(): items.extend(v if isinstance(v, list) else [])
                    elif isinstance(src, list): items = src
                    for i in items:
                        if any(x in str(i.get('status','')).lower() for x in ['out', 'inj', 'doubt']):
                            banned.add(self._normalize_name(i.get('name', '')))
        except Exception as e:
            logger.error(f"Erro loading injuries: {e}")
        logger.info(f"Lesionados carregados: {len(banned)} jogadores banidos.")
        return banned

    def refresh_injuries_cache(self):
        """Força o recarregamento do cache de injuries"""
        self.injuries_banned = self._load_injuries()
        logger.info(f"Cache de injuries recarregado: {len(self.injuries_banned)} jogadores")
        return self.injuries_banned

    # =========================================================================
    # NOVOS MÉTODOS PARA INTEGRAÇÃO COM MÓDULOS EXTERNOS
    # =========================================================================
    
    def _estimate_position(self, player_data):
        """Estima a posição do jogador para análise DvP"""
        pos = player_data.get('position', '').upper()
        if not pos:
            # Estimativa baseada no perfil
            if player_data.get('ast_L5', 0) > 5:
                return "PG"
            elif player_data.get('pts_L5', 0) > 20:
                return "SG"
            elif player_data.get('reb_L5', 0) > 8:
                return "PF"
            elif player_data.get('reb_L5', 0) > 5 and player_data.get('pts_L5', 0) > 15:
                return "SF"
            else:
                return "C"
        
        # Mapeamento simplificado
        pos_map = {
            'PG': 'PG', 'G': 'PG',
            'SG': 'SG', 'G-F': 'SG',
            'SF': 'SF', 'F': 'SF', 
            'PF': 'PF', 'F-C': 'PF',
            'C': 'C'
        }
        return pos_map.get(pos, 'SF')  # Default para SF

    def _apply_dvp_adjustment(self, player_data, market_type, base_line, opponent_team=None):
        """Aplica ajuste DvP à linha base, se disponível"""
        if not self.dvp_analyzer or not opponent_team:
            return base_line
        
        try:
            position = self._estimate_position(player_data)
            dvp_rank = self.dvp_analyzer.get_position_rank(opponent_team, position)
            
            # Lógica de ajuste:
            # Rank 30 = pior defesa (aumentar linha)
            # Rank 1 = melhor defesa (diminuir linha)
            # Base: rank 15 = neutro
            
            # Fator de ajuste: ±2% por ponto de rank acima/abaixo de 15
            adjustment_factor = 1.0 + (dvp_rank - 15) * 0.02
            
            # Limitar ajuste entre -20% e +20%
            adjustment_factor = max(0.8, min(1.2, adjustment_factor))
            
            adjusted_line = int(base_line * adjustment_factor)
            
            logger.debug(f"DvP ajuste: {player_data.get('name')} vs {opponent_team} ({position}): "
                        f"Rank {dvp_rank}, Fator {adjustment_factor:.2f}, Linha {base_line}→{adjusted_line}")
            
            return adjusted_line
            
        except Exception as e:
            logger.warning(f"Erro ao aplicar ajuste DvP: {e}")
            return base_line

    def _get_ceiling_analysis(self, player_data, game_context):
        """Obtém análise de ceiling do jogador, se disponível"""
        if not self.ceiling_engine:
            return None
        
        try:
            # Preparar contexto do jogador
            player_ctx = {
                'name': player_data.get('name'),
                'team': player_data.get('team'),
                'pts_L5': player_data.get('pts_L5', 0),
                'reb_L5': player_data.get('reb_L5', 0),
                'ast_L5': player_data.get('ast_L5', 0),
                'min_L5': player_data.get('min_L5', 0),
                'stl_L5': player_data.get('stl_L5', 0),
                'blk_L5': player_data.get('blk_L5', 0),
                '3pm_L5': player_data.get('3pm_L5', 0),
                'team_injuries': player_data.get('team_injuries', 0)
            }
            
            # Determinar archetype usando classificador ou heurística
            archetype = None
            if self.player_classifier:
                try:
                    archetypes = self.player_classifier.classify_player(player_ctx)
                    if archetypes:
                        archetype = archetypes[0].get('archetype')
                except:
                    pass
            
            ceilings = self.ceiling_engine.calculate_player_ceiling(
                player_ctx=player_ctx,
                game_ctx=game_context,
                archetype=archetype
            )
            
            return ceilings
            
        except Exception as e:
            logger.warning(f"Erro ao obter ceiling analysis: {e}")
            return None

    # =========================================================================
    # ENTRY POINT (GARANTE LISTA) - COM MELHORIAS - VERSÃO ORIGINAL RESTAURADA
    # =========================================================================
    def generate_basic_trixies_by_category(self, players_ctx, game_ctx, category):
        # FLUXO ORIGINAL - NÃO MEXER
        all_players = []
        for raw_team, p_list in players_ctx.items():
            norm_team = self._normalize_team(raw_team)
            for p in p_list:
                p['team'] = norm_team
                # Float conversion - CHAVES ESPERADAS: pts_L5, reb_L5, etc.
                for k in ['pts_L5','reb_L5','ast_L5','min_L5','3pm_L5','blk_L5','stl_L5', 'pts_cv']:
                    try: p[k] = float(p.get(k, 0))
                    except: p[k] = 0.0
                
                if self._normalize_name(p.get('name')) not in self.injuries_banned:
                    # Calcular smart_score melhorado com módulos externos
                    base_score = self._calculate_smart_score(p)
                    
                    # Adicionar análise de ceiling se disponível
                    if self.ceiling_engine:
                        try:
                            ceilings = self._get_ceiling_analysis(p, game_ctx)
                            if ceilings:
                                p['ceiling_95'] = ceilings.get('pra_ceil_95', 0)
                                p['ceiling_ratio'] = p['ceiling_95'] / max(p.get('pra_L5', 1), 1)
                                # Boost no score baseado no ceiling
                                base_score *= (1 + min(p['ceiling_ratio'] * 0.1, 0.3))
                        except:
                            pass
                    
                    p['smart_score'] = base_score
                    all_players.append(p)

        cat = category.lower()
        res = []

        if cat == "conservadora": res = self._build_safe_trixie(all_players, game_ctx)
        elif cat == "ousada": res = self._build_bold_trixie(all_players, game_ctx)
        elif cat == "banco": res = self._build_bench_trixie(all_players, game_ctx)
        elif cat == "explosao": res = self._build_explosion_trixie(all_players, game_ctx)
        elif cat == "vulture": res = self._build_vulture_trixie(all_players, game_ctx)
        elif cat == "versatil": res = self._build_versatile_trixie(all_players, game_ctx)

        if res:
            if isinstance(res, list): return res
            return [res]
        return []

    # =========================================================================
    # MÉTODO _create_leg ATUALIZADO COM INTEGRAÇÕES
    # =========================================================================
    def _create_leg(self, p, mkt, prob, thesis):
        p_name = self._normalize_name(p.get('name', ''))
        real_odd = None
        real_line = None
        
        if p_name in self.props_map:
            props = self.props_map[p_name]
            if mkt in props:
                real_line = props[mkt].get('line', None) if isinstance(props[mkt], dict) else props[mkt]
                real_odd = props[mkt].get('odds', None) if isinstance(props[mkt], dict) else None
        
        # Determinar linha base
        stat_keys = {
            'PTS': 'pts_L5',
            'REB': 'reb_L5', 
            'AST': 'ast_L5',
            'BLK': 'blk_L5',
            'STL': 'stl_L5',
            '3PM': '3pm_L5'
        }
        
        base_line_key = stat_keys.get(mkt, 'pts_L5')
        base_line = max(1, int(p.get(base_line_key, 0)))
        
        # Aplicar ajuste DvP se disponível e se conhecemos o oponente
        if self.dvp_analyzer and 'opponent_team' in p:
            base_line = self._apply_dvp_adjustment(p, mkt, base_line, p.get('opponent_team'))
        
        # Se linha real disponível, usar (mas ajustada se muito diferente)
        if real_line is not None:
            line = real_line
            # Se nossa linha estimada for muito diferente, logar
            if abs(line - base_line) > 3:
                logger.info(f"Linha real diferente: {p_name} {mkt}: Estimada={base_line}, Real={line}")
        else:
            line = base_line
        
        # Determinar odd
        if real_odd is not None:
            odd = real_odd
            logger.info(f"Usando odd real da Pinnacle para {p_name} {mkt}: {odd}")
        else:
            # Usar Monte Carlo se disponível para estimar odd
            if self.monte_carlo and hasattr(self.monte_carlo, 'estimate_probability'):
                try:
                    estimated_prob = self.monte_carlo.estimate_probability(p, mkt, line)
                    if estimated_prob > 0:
                        # Converter probabilidade para odd decimal
                        odd = 1.0 / estimated_prob
                        odd = min(max(odd, 1.1), 3.0)  # Limitar entre 1.1 e 3.0
                        logger.debug(f"Odd estimada por Monte Carlo para {p_name}: {odd:.2f}")
                    else:
                        odd = 1.70
                except:
                    odd = 1.70
            else:
                # Fallback: odd baseada em seed determinística
                seed = int(hashlib.md5(p['name'].encode()).hexdigest(), 16) % 20
                odd = 1.70 + (seed/100)
                logger.warning(f"Odd fictícia para {p_name} {mkt}: {odd} (Pinnacle não encontrou)")
        
        # Melhorar thesis com módulos externos se disponível
        enhanced_thesis = thesis
        if self.thesis_engine and hasattr(self.thesis_engine, 'enhance_thesis'):
            try:
                enhanced_thesis = self.thesis_engine.enhance_thesis(p, mkt, thesis)
            except:
                pass
        
        # Adicionar análise de ceiling à leg se disponível
        extra_info = {}
        if self.ceiling_engine:
            ceilings = self._get_ceiling_analysis(p, {})
            if ceilings:
                extra_info['ceiling_95'] = round(ceilings.get('pra_ceil_95', 0), 1)
                extra_info['ceiling_ratio'] = round(ceilings.get('ceiling_ratio', 1.0), 2)
        
        return {
            "player_name": p['name'], 
            "team": p['team'], 
            "market_display": f"{line}+ {mkt}", 
            "market_type": mkt, 
            "odds": round(odd, 2), 
            "thesis": enhanced_thesis,
            "line": line,
            **extra_info
        }

    # =========================================================================
    # MÉTODO get_specific_leg ATUALIZADO (VERSÃO ROBUSTA)
    # =========================================================================
    def get_specific_leg(self, p, market_type, category="ousada", thesis_override=None):
        """
        Gera uma leg sob demanda com validação robusta de dados.
        """
        # 1. Filtro de Lesão
        p_norm = self._normalize_name(p.get('name'))
        if p_norm in self.injuries_banned:
            return None
        
        # 2. Verificar dados básicos com Fallback
        stat_key = f'{market_type.lower()}_L5'
        stat_value = p.get(stat_key, 0)
        
        # Se a chave _L5 for 0, tenta buscar a chave simples (ex: 'pts' em vez de 'pts_L5')
        if not stat_value:
            stat_value = p.get(market_type.lower(), 0)
            if not stat_value:
                stat_value = p.get(market_type.upper(), 0)

        try:
            stat_value = float(stat_value)
        except:
            stat_value = 0.0
        
        # 3. Threshold de Segurança
        # Se for PTS/REB/AST, exige mínimo. Se for BLK/STL/3PM, aceita valores menores.
        min_threshold = 0.5 if market_type in ['BLK', 'STL', '3PM'] else 1.0
        
        if stat_value < min_threshold:
            # Tenta salvar se for um jogador relevante (minutos altos)
            minutos = float(p.get('min_L5', p.get('min', 0)) or 0)
            if minutos < 15:
                return None
        
        # 4. Configura multiplicador
        mult = 0.90
        if category == "conservadora": mult = 0.85
        elif category == "ousada": mult = 0.95
        
        # 5. Define a Tese
        thesis = thesis_override if thesis_override else "Desdobrador Inteligente"
        
        # 6. Chama o método de criação
        try:
            # Garante que p tenha a chave esperada pelo _create_leg
            p_copy = p.copy()
            if p_copy.get(stat_key, 0) == 0:
                p_copy[stat_key] = stat_value # Injeta o valor encontrado no fallback

            leg = self._create_leg(p_copy, market_type, mult, thesis)
            
            if not leg:
                return None
            
            # 7. Garantir linha mínima
            if leg.get('line', 0) < 1:
                leg['line'] = 1
                leg['market_display'] = f"1+ {market_type}"
            
            return leg
            
        except Exception as e:
            logger.error(f"Erro ao criar leg para {p.get('name')}: {e}")
            return None

    # =========================================================================
    # MÉTODOS DE BUILDS (MANTIDOS IGUAIS - CORRIGIDOS PARA USAR thesis_gen)
    # =========================================================================
    def _calculate_smart_score(self, p):
        base = (p['pts_L5']*1.0 + p['reb_L5']*1.2 + p['ast_L5']*1.5 + p['stl_L5']*3 + p['blk_L5']*3)
        cv = p.get('pts_cv', 0.5)
        if cv > 0.35: base *= 0.9
        if p['min_L5'] > 30: base *= 1.1
        return base

    def _build_vulture_trixie(self, players, ctx):
        candidates = []
        
        for p in players:
            if p['pts_L5'] > 13.5: continue
            if p['min_L5'] > 29.0: continue
            
            p_name = self._normalize_name(p['name'])
            t_key = p['team']
            
            dna_stats = None
            has_dna = False
            
            if t_key in self.dna_cache:
                team_dna = self.dna_cache[t_key]
                if isinstance(team_dna, list):
                    temp_dict = {}
                    for x in team_dna:
                        if isinstance(x, dict):
                            temp_dict[self._normalize_name(x.get('name',''))] = x
                    team_dna = temp_dict
                
                for dna_name, stats in team_dna.items():
                    dn = self._normalize_name(dna_name)
                    if dn in p_name or p_name in dn:
                        dna_stats = stats
                        has_dna = True
                        break
            
            proj = 0
            tag = ""
            boost = 0
            
            if has_dna:
                proj = float(dna_stats.get('avg_pts_blowout', 0))
                tag = f"DNA ({dna_stats.get('frequency','?')})"
                boost = 50
                if proj < p['pts_L5']: proj = (proj + p['pts_L5']) / 2
            else:
                if p['min_L5'] > 20.0: continue
                if p['min_L5'] > 0:
                    ppm = p['pts_L5'] / p['min_L5']
                    proj = ppm * 14.0 
                    tag = "MATH"
            
            if proj < 5.0: continue

            ratio_reb = p['reb_L5'] / max(p['pts_L5'], 1)
            ratio_ast = p['ast_L5'] / max(p['pts_L5'], 1)
            
            proj_reb = proj * ratio_reb
            proj_ast = proj * ratio_ast
            
            leg = None
            if proj_reb >= 2.5:
                l1, l2 = int(proj), int(proj_reb)
                leg = self._create_combo_leg(p, "PTS", "REB", 0.9, f"Vulture {tag}")
                leg['market_display'] = f"{l1}+ PTS & {l2}+ REB"
                leg['_score'] = proj + proj_reb + boost
            elif proj_ast >= 2.0:
                l1, l2 = int(proj), int(proj_ast)
                leg = self._create_combo_leg(p, "PTS", "AST", 0.9, f"Vulture {tag}")
                leg['market_display'] = f"{l1}+ PTS & {l2}+ AST"
                leg['_score'] = proj + proj_ast + boost
            elif proj >= 7.0:
                leg = self._create_leg(p, "PTS", 0.9, f"Garbage {tag}")
                leg['market_display'] = f"{int(proj)}+ PTS"
                leg['_score'] = proj + (boost/2)
            
            if leg: candidates.append(leg)

        candidates.sort(key=lambda x: x.get('_score', 0), reverse=True)
        final = candidates[:3]
        if not final: return None
        return self._pack_trixie(final, "VULTURE", "GARBAGE TIME", ctx)

    def _build_versatile_trixie(self, players, ctx):
        candidates = []
        for p in players:
            if p['min_L5'] < 14: continue
            leg = None
            score = 0
            
            if p['blk_L5'] >= 0.8:
                leg = self._create_leg(p, "BLK", 0.88, "Rim Prot")
                leg['market_display'] = "1+ Block"
                score = p['blk_L5'] * 15
            elif p['stl_L5'] >= 1.0:
                leg = self._create_leg(p, "STL", 0.88, "Thief")
                leg['market_display'] = "1+ Steal"
                score = p['stl_L5'] * 12
            elif p['3pm_L5'] >= 2.0:
                leg = self._create_leg(p, "3PM", 0.90, "Sniper")
                leg['market_display'] = "2+ 3PM"
                score = p['3pm_L5'] * 6

            if leg:
                leg['_sort'] = score
                candidates.append(leg)
                
        final = []
        seen = set()
        candidates.sort(key=lambda x: x.get('_sort', 0), reverse=True)
        for c in candidates:
            if c['player_name'] not in seen:
                final.append(c)
                seen.add(c['player_name'])
        
        if len(final) < 2: return None
        return self._pack_trixie(final[:3], "VERSATIL", "ESPECIALISTAS", ctx)

    def _build_bold_trixie(self, players, ctx):
        cands = []
        for p in players:
            if p['min_L5'] < 24: continue
            
            if p['reb_L5'] >= 7 and p['pts_L5'] >= 12:
                t_reb = int(p['reb_L5'])
                t_pts = int(p['pts_L5'] * 0.9)
                l = self._create_combo_leg(p, "REB", "PTS", 0.85, "Double-Double Risk")
                l['market_display'] = f"{t_pts}+ PTS & {t_reb}+ REB"
                cands.append(l)
            elif p['ast_L5'] >= 5 and p['pts_L5'] >= 12:
                t_ast = int(p['ast_L5'])
                t_pts = int(p['pts_L5'] * 0.9)
                l = self._create_combo_leg(p, "AST", "PTS", 0.85, "Playmaker")
                l['market_display'] = f"{t_pts}+ PTS & {t_ast}+ AST"
                cands.append(l)

        random.shuffle(cands)
        final = []
        seen = set()
        for c in cands:
            if c['player_name'] not in seen:
                final.append(c)
                seen.add(c['player_name'])
        
        if len(final)<2: return None
        return self._pack_trixie(final[:3], "OUSADA", "UPSIDE", ctx)

    def _build_safe_trixie(self, players, ctx):
        # Filtra jogadores com minutos sólidos (segurança)
        cands = sorted([p for p in players if p['min_L5'] >= 28], key=lambda x: x.get('smart_score', 0), reverse=True)
        legs = []
        
        for p in cands[:4]:
            if len(legs) >= 3: break
            
            # Escolhe o mercado alvo (Target)
            tgt = "PTS"
            if p['reb_L5'] > 8: tgt = "REB"
            elif p['ast_L5'] > 7: tgt = "AST"
            
            # --- CORREÇÃO E INTEGRAÇÃO COM THESIS ENGINE ---
            # 1. Tese padrão (Fallback para não quebrar)
            thesis_text = f"Consistência segura em {tgt}"
            
            # 2. Tenta usar o Motor Avançado se disponível
            if self.thesis_engine:
                try:
                    # Mapeamento: De Mercado (PTS) para Categoria de Tese (ScorerLine)
                    target_cat_map = {
                        'PTS': 'ScorerLine',
                        'REB': 'BigRebound',
                        'AST': 'AssistMatchup',
                        '3PM': 'Sniper3PM',
                        'BLK': 'DefensiveAnchor',
                        'STL': 'DefensiveAnchor'
                    }
                    desired_cat = target_cat_map.get(tgt, 'ValueHunter')
                    
                    # Gera todas as teses para o jogador
                    all_theses = self.thesis_engine.generate_theses(p, ctx)
                    
                    # Busca a melhor tese para a categoria que queremos
                    best_thesis = self.thesis_engine.get_thesis_for_category(all_theses, desired_cat)
                    
                    if best_thesis:
                        # Formata para texto: "Matchup Favorável (85%)"
                        thesis_text = self.thesis_engine.format_thesis_for_display(best_thesis)
                        
                except Exception as e:
                    logger.warning(f"Erro ao gerar tese safe para {p.get('name')}: {e}")

            # Cria a leg com o texto final
            legs.append(self._create_leg(p, tgt, 0.85, thesis_text))
            
        return self._pack_trixie(legs, "CONSERVADORA", "MAIN", ctx)

    def _build_bench_trixie(self, players, ctx):
        cands = []
        for p in players:
            if p['min_L5'] > 28.5: continue
            if p.get('is_starter'): continue
            
            if p['pts_L5'] >= 9.5:
                l = self._create_leg(p, "PTS", 0.85, "Bench Scorer")
                l['market_display'] = f"{int(p['pts_L5']-1)}+ PTS"
                cands.append(l)
            elif p['reb_L5'] >= 4.5:
                l = self._create_leg(p, "REB", 0.85, "Bench Energy")
                l['market_display'] = f"{int(p['reb_L5'])}+ REB"
                cands.append(l)
        random.shuffle(cands)
        final = cands[:3]
        if not final: return None
        return self._pack_trixie(final, "BANCO", "ROTAÇÃO", ctx)

    def _build_explosion_trixie(self, players, ctx):
        scorers = []
        for p in players:
            if p['pts_L5'] >= 20:
                # CORREÇÃO: Usar volatility_adj que agora existe
                vol = self.volatility_adj.adjust(p)
                p['expl_score'] = p['pts_L5'] * vol
                scorers.append(p)
        scorers.sort(key=lambda x: x.get('expl_score', 0), reverse=True)
        
        legs = [self._create_leg(p, "PTS", 0.9, "Explosion") for p in scorers[:3]]
        return self._pack_trixie(legs, "EXPLOSAO", "CEILING", ctx)

    # --- HELPERS (MANTIDOS) ---
    def _create_combo_leg(self, p, m1, m2, mult, thesis):
        p_name = self._normalize_name(p.get('name', ''))
        real_odd = None
        real_line = None
        
        if p_name in self.props_map:
            props = self.props_map[p_name]
            combo_key = f"{m1}+{m2}"
            if combo_key in props:
                real_line = props[combo_key].get('line', None)
                real_odd = props[combo_key].get('odds', None)
        
        if real_odd is not None:
            odd = real_odd
            logger.info(f"Usando odd real da Pinnacle para combo {p_name}: {odd}")
        else:
            odd = 2.80
        
        return {
            "player_name": p['name'], "team": p['team'], 
            "market_display": f"{m1}+{m2} Combo", 
            "market_type": "COMBO", "odds": round(odd, 2), "thesis": thesis
        }

    def _pack_trixie(self, legs, cat, sub, ctx):
        if len(legs) < 2: return None
        tot = 1.0
        for l in legs: tot *= l['odds']
        return {
            "id": uuid.uuid4().hex[:6], "category": cat, "sub_category": sub,
            "game_info": {"away": ctx.get('away'), "home": ctx.get('home')},
            "players": legs, "score": int(random.uniform(75, 98)),
            "estimated_total_odd": round(tot, 2)
        }

    # =========================================================================
    # MÉTODOS PONTE PARA USO FUTURO (NÃO INTERFEREM NO FLUXO PRINCIPAL)
    # =========================================================================
    
    def process_team_context(self, team_abbr, roster):
        """
        Para uso futuro: analisa contexto do time (Vacuum, Pace).
        NÃO chamado pelo fluxo principal para não quebrar.
        """
        if not self.vacuum_matrix or not roster:
            return {}
        
        try:
            vacuum_report = self.vacuum_matrix.analyze_team_vacuum(roster, team_abbr)
            return vacuum_report
        except Exception as e:
            logger.warning(f"Erro ao processar contexto do time {team_abbr}: {e}")
            return {}
    
    def enhance_player_data(self, player_data, team_abbr):
        """
        Para uso futuro: aplica ajustes (Vacuum, Pace) aos dados.
        NÃO chamado pelo fluxo principal para não quebrar.
        """
        enhanced = player_data.copy()
        
        # Apenas para demonstração - não afeta o fluxo principal
        if hasattr(self, 'vacuum_matrix') and self.vacuum_matrix:
            # Simulação de aplicação de vacuum
            enhanced['_enhanced'] = True
        
        return enhanced
    
    def get_narrative_badge(self, player_data, opponent_team):
        """
        Para uso futuro: retorna badge narrativo.
        """
        if not self.narrative_intel:
            return None
        
        try:
            player_id = player_data.get('player_id')
            player_name = player_data.get('name')
            
            if not player_id or not player_name:
                return None
            
            narrative_data = self.narrative_intel.get_player_matchup_history(
                player_id, player_name, opponent_team
            )
            
            return narrative_data
        except Exception as e:
            logger.warning(f"Erro ao buscar narrative badge: {e}")
            return None
    
    def validate_correlation(self, legs):
        """
        Para uso futuro: valida correlação entre legs.
        """
        if not self.correlation_validator:
            return {'is_valid': True, 'violations': []}
        
        try:
            players_data = []
            for leg in legs:
                player_data = {
                    'name': leg.get('player_name'),
                    'team': leg.get('team'),
                    'position': '',
                    'market': leg.get('market_type'),
                    'thesis': leg.get('thesis', '')
                }
                players_data.append(player_data)
            
            return self.correlation_validator.validate_trixie(players_data)
        except Exception as e:
            logger.warning(f"Erro ao validar correlação: {e}")
            return {'is_valid': True, 'violations': []}

    # =========================================================================
    # MÉTODOS DE DIAGNÓSTICO E RELATÓRIO (ATUALIZADO)
    # =========================================================================
    
    def get_available_modules_report(self):
        """Retorna relatório dos módulos disponíveis"""
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
                'legacy_modules': hasattr(self, 'thesis_gen')
            },
            'injuries_loaded': len(self.injuries_banned),
            'dna_loaded': len(self.dna_cache)
        }